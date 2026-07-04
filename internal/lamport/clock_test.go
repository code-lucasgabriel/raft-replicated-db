package lamport

import (
	"sync"
	"testing"
)

func TestTickIsMonotonic(t *testing.T) {
	var c Clock
	prev := uint64(0)
	for i := 0; i < 100; i++ {
		got := c.Tick()
		if got <= prev {
			t.Fatalf("Tick() = %d, want > %d", got, prev)
		}
		prev = got
	}
}

func TestObserveJumpsPastRemote(t *testing.T) {
	var c Clock
	c.Tick() // t = 1

	if got := c.Observe(10); got != 11 {
		t.Fatalf("Observe(10) = %d, want 11", got)
	}
	// A remote timestamp behind us must still advance the clock by one.
	if got := c.Observe(3); got != 12 {
		t.Fatalf("Observe(3) = %d, want 12", got)
	}
}

func TestNowDoesNotAdvance(t *testing.T) {
	var c Clock
	c.Tick()
	if c.Now() != c.Now() {
		t.Fatal("Now() advanced the clock")
	}
}

// TestHappensBefore models the clock condition from the paper: a send on A
// and the causally-following receive+event on B must be ordered.
func TestHappensBefore(t *testing.T) {
	var a, b Clock
	for i := 0; i < 5; i++ {
		b.Tick() // b runs ahead on local events
	}
	sendTS := a.Tick()
	recvTS := b.Observe(sendTS)
	if recvTS <= sendTS {
		t.Fatalf("receive ts %d not after send ts %d", recvTS, sendTS)
	}
	if next := b.Tick(); next <= recvTS {
		t.Fatalf("event after receive ts %d not after %d", next, recvTS)
	}
}

// TestConcurrentUse mostly exists so `go test -race` exercises the lock.
// It also checks that n concurrent Ticks yield n distinct timestamps.
func TestConcurrentUse(t *testing.T) {
	var c Clock
	const n = 1000
	seen := make(chan uint64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			seen <- c.Tick()
		}()
	}
	wg.Wait()
	close(seen)

	dedup := make(map[uint64]bool, n)
	for ts := range seen {
		if dedup[ts] {
			t.Fatalf("duplicate timestamp %d", ts)
		}
		dedup[ts] = true
	}
	if c.Now() != n {
		t.Fatalf("final clock = %d, want %d", c.Now(), n)
	}
}
