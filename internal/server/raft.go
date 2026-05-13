package server

import (
	"context"

	raftv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/raft/v1"
)

// RaftService implements the internal node-to-node consensus RPCs.
// Method bodies are stubs that always reject — the real Raft state machine
// will plug in here.
type RaftService struct {
	raftv1.UnimplementedRaftServer
}

func NewRaftService() *RaftService { return &RaftService{} }

func (s *RaftService) RequestVote(_ context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return &raftv1.RequestVoteResponse{Term: req.GetTerm(), VoteGranted: false}, nil
}

func (s *RaftService) AppendEntries(_ context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return &raftv1.AppendEntriesResponse{Term: req.GetTerm(), Success: false}, nil
}

func (s *RaftService) InstallSnapshot(_ context.Context, req *raftv1.InstallSnapshotRequest) (*raftv1.InstallSnapshotResponse, error) {
	return &raftv1.InstallSnapshotResponse{Term: req.GetTerm()}, nil
}
