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
    RejectMeaninglessAdjacency
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

type Option func(*TimeGate)

func WithLockTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.LockTimeout = d }
}

func WithSessionTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.SessionTimeout = d }
}

func WithLeaseGrantTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.LeaseGrantTimeout = d }
}

func WithPutTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.PutTimeout = d }
}

func WithGetTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.GetTimeout = d }
}

func WithUnlockTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.UnlockTimeout = d }
}

func WithDeleteTimeout(d time.Duration) Option {
    return func(g *TimeGate) { g.DeleteTimeout = d }
}

func WithLockSessionTTL(ttl int) Option {
    return func(g *TimeGate) { g.LockSessionTTL = ttl }
}

func WithAdjacency(mode AdjacencyMode) Option {
    return func(g *TimeGate) { g.Adjacency = mode }
}

func NewTimeGate(cli *clientv3.Client, opts ...Option) *TimeGate {
	timegate := &TimeGate{
        	cli:              cli,
        	Adjacency:        AdjacentAllowed,
        	LockTimeout:      500 * time.Millisecond, // Lock acquisition via etcd concurrency is fast; half a second tolerates minor jitter.
        	SessionTimeout:   2 * time.Second, // Creating a session involves a lease; 2s avoids false timeouts during etcd leader elections.
        	LeaseGrantTimeout: 1 * time.Second, // Lease grants are cheap; 1s is enough even under load.
        	PutTimeout:       500 * time.Millisecond, // Writes are fast; 500ms is safe for moderate cluster load.
        	GetTimeout:       300 * time.Millisecond, // Reads are extremely fast; 300ms is generous.
        	UnlockTimeout:    300 * time.Millisecond, // Unlock is just a delete; 300ms is safe.
        	DeleteTimeout:    300 * time.Millisecond, // Same reasoning as unlock.
        	LockSessionTTL:   10, // Long enough to survive transient pauses, short enough to avoid stale locks.
	}

    	for _, opt := range opts {
		opt(timegate)
	}

	return timegate
}

func (g *TimeGate) Check(
    id string,
    timestamp time.Time,
    before time.Duration,
    after time.Duration,
    maxValidity time.Duration,
) (*Result, error) {

    if before == 0 && after == 0 && g.Adjacency == AdjacentAllowed {
	return &Result{Accepted: false, Reason: RejectMeaninglessAdjacency}, nil
    }

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

    newStart := timestamp.Add(-before)
    newEnd := timestamp.Add(after)

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
