package server

import (
	"context"
	"sync"

	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
)

// LeaderHintFunc returns "" when the local node is leader, otherwise the id
// of the node that should receive the write.
type LeaderHintFunc func() string

// DBService implements the client-facing key/value API. Writes are accepted
// only on the leader; followers respond with LeaderHint set. The state
// machine here is a plain in-memory map — Raft will replace direct writes
// with proposals once consensus is wired in.
type DBService struct {
	dbv1.UnimplementedDBServer

	leaderHint LeaderHintFunc

	mu    sync.RWMutex
	store map[string][]byte
}

func NewDBService(leaderHint LeaderHintFunc) *DBService {
	return &DBService{
		leaderHint: leaderHint,
		store:      make(map[string][]byte),
	}
}

func (s *DBService) Get(_ context.Context, req *dbv1.GetRequest) (*dbv1.GetResponse, error) {
	s.mu.RLock()
	v, ok := s.store[req.GetKey()]
	s.mu.RUnlock()
	return &dbv1.GetResponse{Value: v, Found: ok}, nil
}

func (s *DBService) Put(_ context.Context, req *dbv1.PutRequest) (*dbv1.PutResponse, error) {
	if hint := s.leaderHint(); hint != "" {
		return &dbv1.PutResponse{LeaderHint: hint}, nil
	}
	// TODO: route through Raft.Propose once consensus is wired in.
	s.mu.Lock()
	s.store[req.GetKey()] = req.GetValue()
	s.mu.Unlock()
	return &dbv1.PutResponse{}, nil
}

func (s *DBService) Delete(_ context.Context, req *dbv1.DeleteRequest) (*dbv1.DeleteResponse, error) {
	if hint := s.leaderHint(); hint != "" {
		return &dbv1.DeleteResponse{LeaderHint: hint}, nil
	}
	s.mu.Lock()
	delete(s.store, req.GetKey())
	s.mu.Unlock()
	return &dbv1.DeleteResponse{}, nil
}
