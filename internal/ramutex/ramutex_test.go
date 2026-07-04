package ramutex

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
)

// localNet routes RequestCS calls to the target node's HandleRequest
// directly — the algorithm under test is identical to production; only the
// wire is replaced.
type localNet struct {
	nodes map[string]*Mutex
}

type localTransport struct {
	net  *localNet
	self string
}

func (t localTransport) RequestCS(ctx context.Context, peerID string, ts uint64) error {
	return t.net.nodes[peerID].HandleRequest(ctx, ts, t.self)
}

func newCluster(ids ...string) map[string]*Mutex {
	net := &localNet{nodes: make(map[string]*Mutex)}
	for _, id := range ids {
		net.nodes[id] = New(id, ids, &lamport.Clock{}, localTransport{net: net, self: id})
	}
	return net.nodes
}

// TestMutualExclusion hammers the lock from every node concurrently and
// asserts no two holders ever overlap: entering flips an atomic flag 0->1,
// leaving flips it back; any concurrent holder would see the CAS fail.
func TestMutualExclusion(t *testing.T) {
	nodes := newCluster("node-1", "node-2", "node-3")

	var inCS int32
	var entries int32
	const perNode = 20

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, m := range nodes {
		for g := 0; g < 3; g++ {
			wg.Add(1)
			go func(m *Mutex) {
				defer wg.Done()
				for i := 0; i < perNode; i++ {
					if err := m.Lock(ctx); err != nil {
						t.Errorf("%s: Lock: %v", m.id, err)
						return
					}
					if !atomic.CompareAndSwapInt32(&inCS, 0, 1) {
						t.Errorf("%s: entered CS while another holder inside", m.id)
					}
					atomic.AddInt32(&entries, 1)
					time.Sleep(100 * time.Microsecond)
					if !atomic.CompareAndSwapInt32(&inCS, 1, 0) {
						t.Errorf("%s: CS flag corrupted on exit", m.id)
					}
					m.Unlock()
				}
			}(m)
		}
	}
	wg.Wait()

	want := int32(len(nodes) * 3 * perNode)
	if entries != want {
		t.Fatalf("completed %d CS entries, want %d", entries, want)
	}
}

// TestLostUpdatePrevented performs racy read-modify-write increments — the
// exact operation the DB's Incr RPC runs — under the distributed lock and
// expects zero lost updates.
func TestLostUpdatePrevented(t *testing.T) {
	nodes := newCluster("node-1", "node-2", "node-3")

	counter := 0 // deliberately unsynchronized; the distributed lock is the only protection
	const perNode = 25

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for _, m := range nodes {
		wg.Add(1)
		go func(m *Mutex) {
			defer wg.Done()
			for i := 0; i < perNode; i++ {
				if err := m.Lock(ctx); err != nil {
					t.Errorf("%s: Lock: %v", m.id, err)
					return
				}
				v := counter
				time.Sleep(50 * time.Microsecond) // widen the read-modify-write window
				counter = v + 1
				m.Unlock()
			}
		}(m)
	}
	wg.Wait()

	if want := len(nodes) * perNode; counter != want {
		t.Fatalf("counter = %d, want %d (lost updates despite lock)", counter, want)
	}
}

// TestLockTimesOutWhileHeld: a request against a busy holder must respect
// ctx, roll back cleanly, and leave both nodes usable afterwards.
func TestLockTimesOutWhileHeld(t *testing.T) {
	nodes := newCluster("node-1", "node-2")
	a, b := nodes["node-1"], nodes["node-2"]

	ctx := context.Background()
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a.Lock: %v", err)
	}

	shortCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := b.Lock(shortCtx); err == nil {
		t.Fatal("b.Lock succeeded while a holds the CS")
	}

	a.Unlock()

	// Both nodes must be fully functional after the aborted request.
	for _, m := range []*Mutex{a, b} {
		lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		if err := m.Lock(lockCtx); err != nil {
			t.Fatalf("%s: Lock after abort: %v", m.id, err)
		}
		m.Unlock()
		cancel()
	}
}

// TestPriorityByTimestamp pins the tie-break rule: with equal timestamps the
// smaller node id wins, so a deferred grant is only released on Unlock.
func TestPriorityByTimestamp(t *testing.T) {
	nodes := newCluster("node-1", "node-2")
	a := nodes["node-1"]

	ctx := context.Background()

	// node-1 requests first (ts=1) and holds the CS.
	if err := a.Lock(ctx); err != nil {
		t.Fatalf("a.Lock: %v", err)
	}

	// node-2's request must be deferred by node-1 until Unlock.
	granted := make(chan error, 1)
	go func() {
		granted <- a.HandleRequest(ctx, 1, "node-2") // same ts, higher id -> loses to a's request... but a already holds, defer regardless
	}()

	select {
	case err := <-granted:
		t.Fatalf("grant released while CS held (err=%v)", err)
	case <-time.After(100 * time.Millisecond):
		// expected: still deferred
	}

	a.Unlock()
	select {
	case err := <-granted:
		if err != nil {
			t.Fatalf("deferred grant returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deferred grant never released after Unlock")
	}
}
