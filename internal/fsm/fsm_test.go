package fsm

import (
	"bytes"
	"io"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/store"
)

func mustEncode(t *testing.T, c Command) []byte {
	t.Helper()
	data, err := Encode(c)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return data
}

func TestApplyPutDelete(t *testing.T) {
	s := store.New()
	f := New(s, &lamport.Clock{})

	if res := f.Apply(&raft.Log{Index: 1, Data: mustEncode(t, Command{Op: "put", Key: "k", Value: []byte("v"), Time: 1, Origin: "node-1"})}); res != nil {
		t.Fatalf("apply put: %v", res)
	}
	if v, ok := s.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("get after put = %q, %v", v, ok)
	}

	if res := f.Apply(&raft.Log{Index: 2, Data: mustEncode(t, Command{Op: "delete", Key: "k", Time: 2, Origin: "node-1"})}); res != nil {
		t.Fatalf("apply delete: %v", res)
	}
	if _, ok := s.Get("k"); ok {
		t.Fatal("key still present after delete")
	}
}

func TestApplyObservesLamportTime(t *testing.T) {
	clock := &lamport.Clock{}
	f := New(store.New(), clock)

	f.Apply(&raft.Log{Index: 1, Data: mustEncode(t, Command{Op: "put", Key: "k", Value: []byte("v"), Time: 41, Origin: "node-2"})})
	if now := clock.Now(); now != 42 {
		t.Fatalf("clock after apply = %d, want 42 (max(0,41)+1)", now)
	}
}

func TestApplyRejectsMalformed(t *testing.T) {
	f := New(store.New(), &lamport.Clock{})
	if res := f.Apply(&raft.Log{Index: 1, Data: []byte("{not json")}); res == nil {
		t.Fatal("malformed entry did not error")
	}
	if res := f.Apply(&raft.Log{Index: 2, Data: mustEncode(t, Command{Op: "cas", Key: "k"})}); res == nil {
		t.Fatal("unknown op did not error")
	}
}

func TestSnapshotRestoreRoundtrip(t *testing.T) {
	s := store.New()
	f := New(s, &lamport.Clock{})
	f.Apply(&raft.Log{Index: 1, Data: mustEncode(t, Command{Op: "put", Key: "a", Value: []byte("1"), Time: 1})})
	f.Apply(&raft.Log{Index: 2, Data: mustEncode(t, Command{Op: "put", Key: "b", Value: []byte("2"), Time: 2})})

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}

	s2 := store.New()
	f2 := New(s2, &lamport.Clock{})
	if err := f2.Restore(io.NopCloser(bytes.NewReader(sink.buf.Bytes()))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	if v, ok := s2.Get("a"); !ok || string(v) != "1" {
		t.Fatalf("restored a = %q, %v", v, ok)
	}
	if v, ok := s2.Get("b"); !ok || string(v) != "2" {
		t.Fatalf("restored b = %q, %v", v, ok)
	}
}

// memSink is an in-memory raft.SnapshotSink for tests.
type memSink struct{ buf bytes.Buffer }

func (m *memSink) Write(p []byte) (int, error) { return m.buf.Write(p) }
func (m *memSink) Close() error                { return nil }
func (m *memSink) ID() string                  { return "mem" }
func (m *memSink) Cancel() error               { return nil }
