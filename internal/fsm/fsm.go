// Package fsm implements raft.FSM — the only interface hashicorp/raft has
// into our state machine. Every committed log entry passes through
// FSM.Apply; periodic Snapshot/Restore lets the library compact the log.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"

	"github.com/hashicorp/raft"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/store"
)

// Command is the wire format for mutations replicated through Raft. The
// leader serialises one of these and passes it to raft.Apply; every node
// (including the leader) decodes and applies via FSM.Apply.
//
// Time and Origin stamp the command with the proposing node's Lamport clock:
// a committed log entry is, causally, a message from the leader to every
// replica, so FSM.Apply observes Time on each node. This is how follower
// clocks advance past write events they didn't originate.
//
// JSON is used for KISS — switching to protobuf later is mechanical.
type Command struct {
	Op     string `json:"op"`              // "put" | "delete"
	Key    string `json:"key"`
	Value  []byte `json:"value,omitempty"` // omitted for delete
	Time   uint64 `json:"time"`            // proposer's Lamport timestamp at propose
	Origin string `json:"origin"`          // proposing node id
}

// Encode marshals a Command into the bytes Raft replicates.
func Encode(c Command) ([]byte, error) { return json.Marshal(c) }

// FSM applies committed log entries to the underlying KV store.
//
// All methods are called by hashicorp/raft. Apply runs on every node after a
// quorum commits a log entry; Snapshot/Restore drive log compaction.
type FSM struct {
	store *store.KV
	clock *lamport.Clock
}

func New(s *store.KV, c *lamport.Clock) *FSM { return &FSM{store: s, clock: c} }

// Apply runs on each node after a log entry is committed. The return value
// is forwarded to the caller of raft.Apply on the leader (we return nil on
// success or an error describing the malformed entry).
//
// Observing cmd.Time is a node-local side effect, not FSM state: the clock
// is not snapshotted and does not influence what the store contains, so
// determinism of the replicated state is preserved. Re-observing old
// timestamps during log replay is harmless (Observe is a max).
func (f *FSM) Apply(log *raft.Log) any {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("fsm: malformed command at index %d: %w", log.Index, err)
	}
	local := f.clock.Observe(cmd.Time)
	switch cmd.Op {
	case "put":
		f.store.Put(cmd.Key, cmd.Value)
	case "delete":
		f.store.Delete(cmd.Key)
	default:
		return fmt.Errorf("fsm: unknown op %q at index %d", cmd.Op, log.Index)
	}
	stdlog.Printf("fsm: applied %s %q idx=%d lamport{origin=%s sent=%d local=%d}",
		cmd.Op, cmd.Key, log.Index, cmd.Origin, cmd.Time, local)
	return nil
}

// Snapshot returns a point-in-time snapshot of the FSM. hashicorp/raft calls
// this on its own schedule (driven by SnapshotInterval / SnapshotThreshold in
// raft.Config). The returned FSMSnapshot.Persist runs on a background
// goroutine, so we serialise the store eagerly here while we still hold
// consistency with the apply loop.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.store.Snapshot()
	if err != nil {
		return nil, err
	}
	return &snapshot{data: data}, nil
}

// Restore replaces FSM state from a snapshot, called during startup if a
// snapshot exists or when a follower catches up via InstallSnapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	return f.store.Restore(rc)
}

type snapshot struct{ data []byte }

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {}
