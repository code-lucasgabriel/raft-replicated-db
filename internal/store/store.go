// Package store is the in-memory KV state machine that the Raft FSM applies
// committed log entries to. It is intentionally tiny — the entire system's
// "business logic" is Get / Put / Delete on a map.
package store

import (
	"encoding/json"
	"io"
	"sync"
)

// KV is a thread-safe in-memory key/value store. Reads take the read lock,
// writes take the write lock. Values are copied in and out so callers cannot
// mutate stored bytes.
type KV struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func New() *KV { return &KV{data: make(map[string][]byte)} }

func (s *KV) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	v, ok := s.data[key]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	out := make([]byte, len(v))
	copy(out, v)
	return out, true
}

func (s *KV) Put(key string, value []byte) {
	v := make([]byte, len(value))
	copy(v, value)
	s.mu.Lock()
	s.data[key] = v
	s.mu.Unlock()
}

func (s *KV) Delete(key string) {
	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Snapshot returns a JSON serialisation of the store. The lock is released
// before encoding so we don't block readers/writers on disk-bound work.
func (s *KV) Snapshot() ([]byte, error) {
	s.mu.RLock()
	cp := make(map[string][]byte, len(s.data))
	for k, v := range s.data {
		buf := make([]byte, len(v))
		copy(buf, v)
		cp[k] = buf
	}
	s.mu.RUnlock()
	return json.Marshal(cp)
}

// Restore replaces the store contents from a JSON snapshot. Called by the
// FSM during recovery, before any new Apply calls.
func (s *KV) Restore(r io.Reader) error {
	var data map[string][]byte
	if err := json.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	if data == nil {
		data = make(map[string][]byte)
	}
	s.mu.Lock()
	s.data = data
	s.mu.Unlock()
	return nil
}
