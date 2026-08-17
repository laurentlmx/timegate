package timegate

import (
    "context"
    "fmt"
    "net/url"
    "os"
    "path/filepath"
    "testing"
    "time"

    clientv3 "go.etcd.io/etcd/client/v3"
    "go.etcd.io/etcd/server/v3/embed"
)

var (
    sharedEtcd  *embed.Etcd
    sharedCli   *clientv3.Client
)

// ----------------------------
// GLOBAL SETUP
// ----------------------------

func TestMain(m *testing.M) {
    // Start etcd once
    e, cli := mustStartEtcd()

    sharedEtcd = e
    sharedCli = cli

    // Run all tests
    code := m.Run()

    // Cleanup
    cli.Close()
    e.Close()

    os.Exit(code)
}

func mustStartEtcd() (*embed.Etcd, *clientv3.Client) {
    dir, err := os.MkdirTemp("", "etcd-test-*")
    if err != nil {
        panic(err)
    }

    cfg := embed.NewConfig()
    cfg.Dir = filepath.Join(dir, "default.etcd")
    cfg.LogLevel = "panic"

    lp, _ := url.Parse("http://127.0.0.1:0")
    lc, _ := url.Parse("http://127.0.0.1:0")

    cfg.ListenPeerUrls = []url.URL{*lp}
    cfg.ListenClientUrls = []url.URL{*lc}
    cfg.AdvertisePeerUrls = []url.URL{*lp}
    cfg.AdvertiseClientUrls = []url.URL{*lc}
    cfg.InitialCluster = fmt.Sprintf("default=%s", lp.String())

    e, err := embed.StartEtcd(cfg)
    if err != nil {
        panic(fmt.Errorf("failed to start embedded etcd: %w", err))
    }

    select {
    case <-e.Server.ReadyNotify():
    case <-time.After(10 * time.Second):
        panic("etcd server did not start")
    }

    cli, err := clientv3.New(clientv3.Config{
        Endpoints:   []string{e.Clients[0].Addr().String()},
        DialTimeout: 5 * time.Second,
    })
    if err != nil {
        panic(fmt.Errorf("failed to create etcd client: %w", err))
    }

    return e, cli
}

func newGate(mode AdjacencyMode) *TimeGate {
    return NewTimeGate(sharedCli, WithAdjacency(mode), WithGetTimeout(2*time.Second), WithPutTimeout(2*time.Second)) // Timeouts modified for local unit tests to pass without warning
}

// ----------------------------
// TESTS
// ----------------------------

func TestRejectTooOld(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now().Add(-10 * time.Minute)
    maxValidity := 1 * time.Minute

    res, err := gate.Check("id1", ts, 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if res.Accepted {
        t.Fatalf("expected rejection for too old timestamp")
    }
    if res.Reason != RejectTooOld {
        t.Fatalf("expected RejectTooOld, got %v", res.Reason)
    }
}

func TestRejectInvalidTTL(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now()
    maxValidity := 1 * time.Second

    res, err := gate.Check("id2", ts, 0, 0, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if res.Accepted {
        t.Fatalf("expected rejection for invalid TTL")
    }
    if res.Reason != RejectInvalidTTL {
        t.Fatalf("expected RejectInvalidTTL, got %v", res.Reason)
    }
}

func TestAcceptFirstWindow(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now()
    maxValidity := 1 * time.Minute

    res, err := gate.Check("id3", ts, 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if !res.Accepted {
        t.Fatalf("expected acceptance, got rejection: %v", res.Reason)
    }
}

func TestRejectOverlap(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now()
    maxValidity := 1 * time.Minute

    _, err := gate.Check("id4", ts, 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    res2, err := gate.Check("id4", ts.Add(5*time.Second), 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if res2.Accepted {
        t.Fatalf("expected rejection due to overlap")
    }
    if res2.Reason != RejectOverlap {
        t.Fatalf("expected RejectOverlap, got %v", res2.Reason)
    }
}

func TestAcceptNonOverlappingWindows(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now()
    maxValidity := 1 * time.Minute

    _, err := gate.Check("id5", ts, 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    res2, err := gate.Check("id5", ts.Add(20*time.Second), 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if !res2.Accepted {
        t.Fatalf("expected acceptance, got rejection: %v", res2.Reason)
    }
}

func TestRollback(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now()
    maxValidity := 1 * time.Minute

    res, err := gate.Check("id6", ts, 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if !res.Accepted {
        t.Fatalf("expected acceptance")
    }

    err = gate.RollbackLease(res.LeaseID, res.Key)
    if err != nil {
        t.Fatalf("rollback failed: %v", err)
    }

    getCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
    defer cancel()

    resp, err := sharedCli.Get(getCtx, res.Key)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if len(resp.Kvs) != 0 {
        t.Fatalf("expected key to be deleted after rollback")
    }
}

func TestConcurrentAccess(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    ts := time.Now()
    maxValidity := 1 * time.Minute

    results := make(chan bool, 5)

    for i := 0; i < 5; i++ {
        go func() {
            res, err := gate.Check("id7", ts, 0, 10*time.Second, maxValidity)
            if err != nil {
                results <- false
                return
            }
            results <- res.Accepted
        }()
    }

    acceptedCount := 0
    for i := 0; i < 5; i++ {
        if <-results {
            acceptedCount++
        }
    }

    if acceptedCount != 1 {
        t.Fatalf("expected exactly 1 acceptance, got %d", acceptedCount)
    }
}

func TestTwoNonOverlappingThenOverlappingThird(t *testing.T) {
    gate := newGate(AdjacentNotAllowed)

    base := time.Now()
    maxValidity := 1 * time.Minute

    res1, err := gate.Check("id8", base, 0, 10*time.Second, maxValidity)
    if err != nil || !res1.Accepted {
        t.Fatalf("expected first window accepted, got err=%v reason=%v", err, res1.Reason)
    }

    res2, err := gate.Check("id8", base.Add(20*time.Second), 0, 10*time.Second, maxValidity)
    if err != nil || !res2.Accepted {
        t.Fatalf("expected second window accepted, got err=%v reason=%v", err, res2.Reason)
    }

    res3, err := gate.Check("id8", base.Add(5*time.Second), 0, 10*time.Second, maxValidity)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if res3.Accepted {
        t.Fatalf("expected third window to be rejected due to overlap")
    }
    if res3.Reason != RejectOverlap {
        t.Fatalf("expected RejectOverlap, got %v", res3.Reason)
    }
}

func TestAdjacentWindowsBehavior(t *testing.T) {
    base := time.Now()
    maxValidity := 1 * time.Minute

    // AdjacentNotAllowed → touching windows rejected
    {
        gate := newGate(AdjacentNotAllowed)

        _, err := gate.Check("id9", base, 0, 10*time.Second, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        res, err := gate.Check("id9", base.Add(10*time.Second), 0, 10*time.Second, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        if res.Accepted {
            t.Fatalf("expected adjacent windows to overlap in AdjacentNotAllowed mode")
        }
    }

    // AdjacentAllowed → touching windows accepted
    {
        gate := newGate(AdjacentAllowed)

        _, err := gate.Check("id10", base, 0, 10*time.Second, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        res, err := gate.Check("id10", base.Add(10*time.Second), 0, 10*time.Second, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        if !res.Accepted {
            t.Fatalf("expected adjacent windows to be allowed in AdjacentAllowed mode")
        }
    }
}

func TestSameTimestampsWithoutTimeframes(t *testing.T) {
    ts := time.Now()
    maxValidity := 1 * time.Minute

    // AdjacentNotAllowed → same timestamp rejected
    {
        gate := newGate(AdjacentNotAllowed)

        _, err := gate.Check("id11", ts, 0, 0, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        res, err := gate.Check("id11", ts, 0, 0, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        if res.Accepted {
            t.Fatalf("expected same timestamps to be rejected in AdjacentNotAllowed mode")
        }
    }

    // AdjacentAllowed → same timestamp accepted
    {
        gate := newGate(AdjacentAllowed)

        _, err := gate.Check("id12", ts, 0, 0, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

        res, err := gate.Check("id12", ts, 0, 0, maxValidity)
        if err != nil {
            t.Fatalf("unexpected error: %v", err)
        }

	if res.Accepted {
            t.Fatalf("expected timestamp to be rejected due to overlap")
        }

        if res.Reason != RejectMeaninglessAdjacency {
            t.Fatalf("expected RejectMeaninglessAdjacency, got %v", res.Reason)
        }
    }
}
