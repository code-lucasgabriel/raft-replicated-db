// Package ramutex implements Ricart-Agrawala distributed mutual exclusion
// (Ricart & Agrawala, "An Optimal Algorithm for Mutual Exclusion in Computer
// Networks", CACM 1981).
//
// The algorithm in one paragraph: to enter the critical section a node
// stamps a REQUEST with its Lamport clock and sends it to every other node,
// then waits for a grant from all of them. A node receiving a REQUEST grants
// immediately unless it is inside the CS, or it is requesting with an
// earlier (timestamp, id) pair — in those cases it defers the grant until it
// exits the CS. The total order on (timestamp, id) pairs guarantees exactly
// one node collects all grants first: safety from pairwise agreement,
// liveness because the globally smallest request is never deferred by
// everyone.
//
// Mapping to RPC: the paper's REQUEST/REPLY message pair maps to a single
// blocking call — Transport.RequestCS sends the REQUEST and returns when the
// peer grants; a deferred REPLY is simply a response the peer has not sent
// yet. 2*(N-1) messages per CS entry, the paper's optimal count.
//
// This package is transport-agnostic. The algorithm state machine lives
// here; the gRPC plumbing lives in internal/server.
package ramutex

import (
	"context"
	"fmt"
	"log"
	"sync"

	"golang.org/x/sync/errgroup"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
)

// Transport delivers a critical-section request to one peer and blocks until
// that peer grants it (the peer may defer the grant arbitrarily long).
// Implementations must respect ctx cancellation while waiting.
type Transport interface {
	RequestCS(ctx context.Context, peerID string, ts uint64) error
}

type csState int

const (
	released csState = iota // not interested in the CS
	wanted                  // requested, collecting grants
	held                    // inside the CS
)

func (s csState) String() string {
	switch s {
	case released:
		return "released"
	case wanted:
		return "wanted"
	case held:
		return "held"
	}
	return "unknown"
}

// Mutex is one node's view of the distributed lock. All nodes must agree on
// the peer set; ids must be unique cluster-wide (they break timestamp ties).
type Mutex struct {
	id        string
	peers     []string // every other node's id — self excluded
	clock     *lamport.Clock
	transport Transport

	// lockMu serializes local Lock callers: the algorithm allows one
	// outstanding request per node, so concurrent local goroutines queue
	// here and the distributed protocol only ever sees one request from us.
	lockMu sync.Mutex

	// mu guards the algorithm state below. Every decision — adopting the
	// wanted state, comparing priorities, deferring — happens atomically
	// under it, which is what makes the pairwise-agreement argument sound.
	mu       sync.Mutex
	state    csState
	reqTime  uint64          // our request's Lamport timestamp, fixed for the whole request
	deferred []chan struct{} // pending grants, released on Unlock
}

// New builds the Mutex for node id. peers is the full cluster membership;
// self is filtered out if present.
func New(id string, peers []string, clock *lamport.Clock, t Transport) *Mutex {
	others := make([]string, 0, len(peers))
	for _, p := range peers {
		if p != id {
			others = append(others, p)
		}
	}
	return &Mutex{id: id, peers: others, clock: clock, transport: t}
}

// Lock acquires the distributed critical section, blocking until every peer
// has granted or ctx is done. On error the request is fully rolled back and
// the Mutex is reusable.
func (m *Mutex) Lock(ctx context.Context) error {
	m.lockMu.Lock() // released in Unlock or on the error paths below

	m.mu.Lock()
	m.state = wanted
	// One Tick stamps the request event; the same timestamp goes to every
	// peer. It is fixed BEFORE any request leaves, and Observe of incoming
	// requests never changes it — both sides must compare identical pairs.
	m.reqTime = m.clock.Tick()
	ts := m.reqTime
	m.mu.Unlock()

	log.Printf("ramutex: %s requesting CS (ts=%d), asking %d peers", m.id, ts, len(m.peers))

	g, gctx := errgroup.WithContext(ctx)
	for _, peer := range m.peers {
		g.Go(func() error {
			if err := m.transport.RequestCS(gctx, peer, ts); err != nil {
				return fmt.Errorf("peer %s: %w", peer, err)
			}
			log.Printf("ramutex: %s got grant from %s (req ts=%d)", m.id, peer, ts)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		// Abort: return to released and flush any grants we deferred while
		// wanted — otherwise a lower-priority peer would wait forever on a
		// request we abandoned.
		m.release()
		m.lockMu.Unlock()
		return fmt.Errorf("ramutex: %s failed to acquire CS: %w", m.id, err)
	}

	m.mu.Lock()
	m.state = held
	m.mu.Unlock()
	log.Printf("ramutex: %s ENTERED CS (ts=%d)", m.id, ts)
	return nil
}

// Unlock leaves the critical section and releases every deferred grant,
// unblocking the peers' pending RequestCS calls.
func (m *Mutex) Unlock() {
	n := m.release()
	log.Printf("ramutex: %s EXITED CS, released %d deferred grant(s)", m.id, n)
	m.lockMu.Unlock()
}

func (m *Mutex) release() int {
	m.mu.Lock()
	m.state = released
	m.reqTime = 0
	n := len(m.deferred)
	for _, ch := range m.deferred {
		close(ch)
	}
	m.deferred = nil
	m.mu.Unlock()
	return n
}

// HandleRequest is the receiving side of the protocol, invoked by the
// transport when a peer asks to enter the CS. It blocks until this node
// grants — immediately when we don't hold priority, or after our next
// Unlock when we do. Returning nil IS the grant.
func (m *Mutex) HandleRequest(ctx context.Context, theirTime uint64, theirID string) error {
	// Message receive: merge the sender's clock. This must happen before the
	// decision so that, if we request later, our timestamp is strictly
	// greater — the happens-before edge that makes both sides agree on
	// priority.
	m.clock.Observe(theirTime)

	m.mu.Lock()
	// We defer iff we're in the CS, or we're requesting with priority.
	// Priority = lexicographic (timestamp, id): unique ids make the order
	// total, so ties are impossible.
	ourPair := fmt.Sprintf("(%d,%s)", m.reqTime, m.id)
	deferGrant := m.state == held ||
		(m.state == wanted && less(m.reqTime, m.id, theirTime, theirID))
	if !deferGrant {
		state := m.state
		m.mu.Unlock()
		log.Printf("ramutex: %s grants %s immediately (their ts=%d, our state=%s)",
			m.id, theirID, theirTime, state)
		return nil
	}
	ch := make(chan struct{})
	m.deferred = append(m.deferred, ch)
	state := m.state
	m.mu.Unlock()

	log.Printf("ramutex: %s DEFERS grant to %s — their (%d,%s) vs our %s, state=%s",
		m.id, theirID, theirTime, theirID, ourPair, state)

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		// The requester gave up (or the connection died). The channel stays
		// in deferred; closing it on our next Unlock is a harmless no-op.
		return ctx.Err()
	}
}

// less reports whether request (t1,id1) precedes (t2,id2) in the total order.
func less(t1 uint64, id1 string, t2 uint64, id2 string) bool {
	if t1 != t2 {
		return t1 < t2
	}
	return id1 < id2
}
