package server

import (
	"context"
	"errors"
	"time"

	"github.com/hashicorp/raft"

	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/fsm"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/store"
)

// applyTimeout caps how long a Put / Delete waits for Raft to commit and apply.
// Tuned for a single-DC cluster.
const applyTimeout = 5 * time.Second

// DBService is the client-facing key/value API.
//
// Writes (Put / Delete) are accepted only on the leader: the operation is
// JSON-encoded as a fsm.Command, proposed via raft.Apply, and acknowledged
// once the FSM has applied it locally (which only happens after the entry
// is committed by a quorum). On a follower, the response carries the
// leader's node id in LeaderHint so the client can retry there.
//
// Reads (Get) are served directly from the local KV store and may therefore
// be stale — see linearizability note in architecture.md.
type DBService struct {
	dbv1.UnimplementedDBServer

	raft  *raft.Raft
	store *store.KV
}

func NewDBService(r *raft.Raft, s *store.KV) *DBService {
	return &DBService{raft: r, store: s}
}

func (s *DBService) Get(_ context.Context, req *dbv1.GetRequest) (*dbv1.GetResponse, error) {
	v, ok := s.store.Get(req.GetKey())
	return &dbv1.GetResponse{
		Value:      v,
		Found:      ok,
		LeaderHint: s.leaderHint(), // empty when we are the leader
	}, nil
}

func (s *DBService) Put(_ context.Context, req *dbv1.PutRequest) (*dbv1.PutResponse, error) {
	hint, err := s.propose(fsm.Command{Op: "put", Key: req.GetKey(), Value: req.GetValue()})
	if err != nil {
		return nil, err
	}
	return &dbv1.PutResponse{LeaderHint: hint}, nil
}

func (s *DBService) Delete(_ context.Context, req *dbv1.DeleteRequest) (*dbv1.DeleteResponse, error) {
	hint, err := s.propose(fsm.Command{Op: "delete", Key: req.GetKey()})
	if err != nil {
		return nil, err
	}
	return &dbv1.DeleteResponse{LeaderHint: hint}, nil
}

// propose runs a command through Raft. Returns ("", nil) on success;
// (leaderID, nil) when we're not the leader (the client retries elsewhere);
// ("", err) on a real failure.
func (s *DBService) propose(cmd fsm.Command) (string, error) {
	if s.raft.State() != raft.Leader {
		return s.leaderHint(), nil
	}
	data, err := fsm.Encode(cmd)
	if err != nil {
		return "", err
	}
	future := s.raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return s.leaderHint(), nil
		}
		return "", err
	}
	// FSM.Apply returns either nil or an error describing a malformed entry.
	if applyErr, _ := future.Response().(error); applyErr != nil {
		return "", applyErr
	}
	return "", nil
}

// leaderHint returns "" when this node IS the leader, otherwise the leader's
// node id so the client can retry against it.
func (s *DBService) leaderHint() string {
	if s.raft.State() == raft.Leader {
		return ""
	}
	_, id := s.raft.LeaderWithID()
	return string(id)
}
