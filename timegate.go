package timegate

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/client/v3/concurrency"
)

type AdjacencyMode int

const (
    AdjacentAllowed AdjacencyMode = iota
    AdjacentNotAllowed
)

type ProcessingWindow struct {
    Start time.Time `json:"start"`
    End   time.Time `json:"end"`
}

func (w ProcessingWindow) Overlaps(start, end time.Time, mode AdjacencyMode) bool {
    if mode == AdjacentAllowed {
        return start.Before(w.End) && end.After(w.Start)
    }
    // AdjacentNotAllowed
    return !start.After(w.End) && !end.Before(w.Start)
}

type RejectReason int

const (
    RejectNone RejectReason = iota
    RejectTooOld
    RejectInvalidTTL
    RejectOverlap
)

type Result struct {
    Accepted bool
    Reason   RejectReason
    LeaseID  clientv3.LeaseID
    Key      string
}

type TimeGate struct {
    cli *clientv3.Client

    Adjacency AdjacencyMode

    // Timeouts
    LockTimeout       time.Duration
    SessionTimeout    time.Duration
    LockSessionTTL int
    LeaseGrantTimeout time.Duration
    PutTimeout        time.Duration
    GetTimeout        time.Duration
    UnlockTimeout     time.Duration
    DeleteTimeout     time.Duration
}

func NewTimeGate(
    cli *clientv3.Client,
    adjacency AdjacencyMode,

    // timeouts
    lockTimeout time.Duration,
    sessionTimeout time.Duration,
    leaseGrantTimeout time.Duration,
    putTimeout time.Duration,
    getTimeout time.Duration,
    unlockTimeout time.Duration,
    deleteTimeout time.Duration,

    // TTLs
    lockSessionTTL int,
) *TimeGate {
    return &TimeGate{
        cli:              cli,
        Adjacency:        adjacency,
        LockTimeout:      lockTimeout,
        SessionTimeout:   sessionTimeout,
        LeaseGrantTimeout: leaseGrantTimeout,
        PutTimeout:       putTimeout,
        GetTimeout:       getTimeout,
        UnlockTimeout:    unlockTimeout,
        DeleteTimeout:    deleteTimeout,
        LockSessionTTL:   lockSessionTTL,
    }
}

func (g *TimeGate) Check(
    id string,
    timestamp time.Time,
    window time.Duration,
    maxValidity time.Duration,
) (*Result, error) {

    age := time.Since(timestamp)
    if age > maxValidity {
        return &Result{Accepted: false, Reason: RejectTooOld}, nil
    }

    ttl := int((maxValidity - age).Seconds())
    if ttl <= 0 {
        return &Result{Accepted: false, Reason: RejectInvalidTTL}, nil
    }

    // Session for locking only — now uses LockSessionTTL
    sessCtx, cancelSess := context.WithTimeout(context.Background(), g.SessionTimeout)
    defer cancelSess()

    session, err := concurrency.NewSession(
        g.cli,
        concurrency.WithTTL(g.LockSessionTTL),
        concurrency.WithContext(sessCtx),
    )
    if err != nil {
        return nil, err
    }
    defer session.Close()

    lockKey := fmt.Sprintf("/lock/%s", id)
    mutex := concurrency.NewMutex(session, lockKey)

    lockCtx, cancelLock := context.WithTimeout(context.Background(), g.LockTimeout)
    defer cancelLock()

    if err := mutex.Lock(lockCtx); err != nil {
        return nil, err
    }
    defer func() {
        unlockCtx, cancel := context.WithTimeout(context.Background(), g.UnlockTimeout)
        defer cancel()
        _ = mutex.Unlock(unlockCtx)
    }()

    // Check existing windows
    prefix := fmt.Sprintf("/processing/%s/", id)

    getCtx, cancelGet := context.WithTimeout(context.Background(), g.GetTimeout)
    defer cancelGet()

    resp, err := g.cli.Get(getCtx, prefix, clientv3.WithPrefix())
    if err != nil {
        return nil, err
    }

    newStart := timestamp
    newEnd := timestamp.Add(window)

    for _, kv := range resp.Kvs {
        var existing ProcessingWindow
        if err := json.Unmarshal(kv.Value, &existing); err != nil {
            return nil, err
        }
        if existing.Overlaps(newStart, newEnd, g.Adjacency) {
            return &Result{Accepted: false, Reason: RejectOverlap}, nil
        }
    }

    // Create a dedicated lease for the processing window — still uses ttl
    leaseCtx, cancelLease := context.WithTimeout(context.Background(), g.LeaseGrantTimeout)
    defer cancelLease()

    leaseResp, err := g.cli.Grant(leaseCtx, int64(ttl))
    if err != nil {
        return nil, err
    }
    leaseID := leaseResp.ID

    dataKey := fmt.Sprintf("/processing/%s/%d", id, newStart.UnixNano())
    pw := ProcessingWindow{Start: newStart, End: newEnd}
    data, err := json.Marshal(pw)
    if err != nil {
        return nil, err
    }

    putCtx, cancelPut := context.WithTimeout(context.Background(), g.PutTimeout)
    defer cancelPut()

    _, err = g.cli.Put(putCtx, dataKey, string(data), clientv3.WithLease(leaseID))
    if err != nil {
        return nil, err
    }

    return &Result{
        Accepted: true,
        Reason:   RejectNone,
        LeaseID:  leaseID,
        Key:      dataKey,
    }, nil
}

func (g *TimeGate) RollbackLease(leaseID clientv3.LeaseID, key string) error {
    if leaseID == 0 || key == "" {
        return errors.New("invalid leaseID or key")
    }

    delCtx, cancelDel := context.WithTimeout(context.Background(), g.DeleteTimeout)
    defer cancelDel()

    if _, err := g.cli.Lease.Revoke(delCtx, leaseID); err != nil {
        return err
    }

    _, err := g.cli.Delete(delCtx, key)
    return err
}
