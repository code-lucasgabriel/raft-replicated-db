package server

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
	mutexv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/mutex/v1"
)

// Peer is one node's client-facing gRPC endpoint.
type Peer struct {
	ID   string
	Addr string // host:port of the peer's gRPC server (NOT the raft transport)
}

// Peers holds one lazily-connecting gRPC client conn per remote node. Both
// peer-facing services (Mutex for Ricart-Agrawala, DB for leader
// forwarding) share the same conn.
//
// grpc.NewClient does not dial — the connection is established on first RPC
// and reconnects on failure — so construction order between cluster nodes
// doesn't matter.
type Peers struct {
	self  string
	conns map[string]*grpc.ClientConn
}

// NewPeers builds conns for every peer except self.
func NewPeers(self string, peers []Peer) (*Peers, error) {
	p := &Peers{self: self, conns: make(map[string]*grpc.ClientConn, len(peers))}
	for _, peer := range peers {
		if peer.ID == self {
			continue
		}
		conn, err := grpc.NewClient(peer.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			p.Close()
			return nil, fmt.Errorf("peers: client for %s (%s): %w", peer.ID, peer.Addr, err)
		}
		p.conns[peer.ID] = conn
	}
	return p, nil
}

// IDs returns every remote peer id.
func (p *Peers) IDs() []string {
	ids := make([]string, 0, len(p.conns))
	for id := range p.conns {
		ids = append(ids, id)
	}
	return ids
}

func (p *Peers) conn(id string) (*grpc.ClientConn, error) {
	c, ok := p.conns[id]
	if !ok {
		return nil, fmt.Errorf("peers: unknown peer %q", id)
	}
	return c, nil
}

func (p *Peers) Mutex(id string) (mutexv1.MutexClient, error) {
	c, err := p.conn(id)
	if err != nil {
		return nil, err
	}
	return mutexv1.NewMutexClient(c), nil
}

func (p *Peers) DB(id string) (dbv1.DBClient, error) {
	c, err := p.conn(id)
	if err != nil {
		return nil, err
	}
	return dbv1.NewDBClient(c), nil
}

func (p *Peers) Close() {
	for _, c := range p.conns {
		_ = c.Close()
	}
}
