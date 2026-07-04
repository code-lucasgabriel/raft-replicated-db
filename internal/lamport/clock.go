// Package lamport implements a Lamport logical clock (Lamport, "Time,
// Clocks, and the Ordering of Events in a Distributed System", CACM 1978).
//
// The clock defines a partial order over events that respects causality:
// if event a happens-before event b, then ts(a) < ts(b). The converse does
// not hold — two events with ts(a) < ts(b) may be concurrent. Breaking ties
// with a unique node id (as Ricart-Agrawala does) extends this to a total
// order.
//
// Usage protocol:
//   - local event or message send:   t := clock.Tick()   // stamp the message with t
//   - message receive:               clock.Observe(remoteT)
package lamport

import "sync"

// Clock is a thread-safe Lamport clock. The zero value is ready to use.
type Clock struct {
	mu sync.Mutex
	t  uint64
}

// Tick advances the clock for a local event (including a message send) and
// returns the new timestamp.
func (c *Clock) Tick() uint64 {
	c.mu.Lock()
	c.t++
	t := c.t
	c.mu.Unlock()
	return t
}

// Observe merges a timestamp received on a message: the clock jumps to
// max(local, remote) + 1, so every event that causally follows the receive
// is ordered after the send. Returns the new local timestamp.
func (c *Clock) Observe(remote uint64) uint64 {
	c.mu.Lock()
	if remote > c.t {
		c.t = remote
	}
	c.t++
	t := c.t
	c.mu.Unlock()
	return t
}

// Now returns the current timestamp without advancing the clock. Use only
// for reporting/logging — events must go through Tick or Observe.
func (c *Clock) Now() uint64 {
	c.mu.Lock()
	t := c.t
	c.mu.Unlock()
	return t
}
