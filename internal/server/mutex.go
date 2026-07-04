package server

import (
	"context"

	"google.golang.org/grpc"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
	mutexv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/mutex/v1"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/ramutex"
)

// MutexService is the server side of the Ricart-Agrawala transport: peers
// call RequestCS and the response is withheld until this node grants.
type MutexService struct {
	mutexv1.UnimplementedMutexServer

	mtx   *ramutex.Mutex
	clock *lamport.Clock
}

func NewMutexService(mtx *ramutex.Mutex, clock *lamport.Clock) *MutexService {
	return &MutexService{mtx: mtx, clock: clock}
}

// RequestCS blocks in HandleRequest until the grant decision resolves —
// immediately, or on this node's next Unlock. HandleRequest observes the
// requester's timestamp; the response is stamped as a fresh send event.
func (s *MutexService) RequestCS(ctx context.Context, req *mutexv1.RequestCSRequest) (*mutexv1.RequestCSResponse, error) {
	if err := s.mtx.HandleRequest(ctx, req.GetLamportTime(), req.GetNodeId()); err != nil {
		return nil, err
	}
	return &mutexv1.RequestCSResponse{LamportTime: s.clock.Tick()}, nil
}

// MutexTransport is the client side: ramutex calls RequestCS once per peer
// and treats the RPC's return as the grant.
type MutexTransport struct {
	self  string
	peers *Peers
	clock *lamport.Clock
}

func NewMutexTransport(self string, peers *Peers, clock *lamport.Clock) *MutexTransport {
	return &MutexTransport{self: self, peers: peers, clock: clock}
}

// RequestCS implements ramutex.Transport. WaitForReady makes the RPC queue
// while the peer connection is still establishing (e.g. during cluster
// boot) instead of failing fast; ctx still bounds the total wait.
func (t *MutexTransport) RequestCS(ctx context.Context, peerID string, ts uint64) error {
	client, err := t.peers.Mutex(peerID)
	if err != nil {
		return err
	}
	resp, err := client.RequestCS(ctx,
		&mutexv1.RequestCSRequest{NodeId: t.self, LamportTime: ts},
		grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	t.clock.Observe(resp.GetLamportTime()) // the grant is a message receive
	return nil
}
