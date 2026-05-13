// Package node ties together the static peer list and the gRPC server.
package node

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/server"
)

// Peer is one entry in the cluster's static seed list.
type Peer struct {
	ID   string
	Addr string
}

type Config struct {
	NodeID   string
	GRPCPort int
	// Peers is the full cluster membership, including this node.
	// Configured statically at boot; assumed stable for the lifetime of the cluster.
	Peers []Peer
}

type Node struct {
	cfg    Config
	server *server.Server
	leader atomic.Value // string; "" when no leader is known
}

func New(cfg Config) (*Node, error) {
	n := &Node{cfg: cfg}
	n.leader.Store("")

	dbSvc := server.NewDBService(n.leaderHint)
	raftSvc := server.NewRaftService()

	srv, err := server.New(cfg.GRPCPort, dbSvc, raftSvc)
	if err != nil {
		return nil, err
	}
	n.server = srv
	return n, nil
}

// leaderHint returns "" when this node is the leader, otherwise the current
// leader's id so the client can retry against it.
func (n *Node) leaderHint() string {
	leader, _ := n.leader.Load().(string)
	if leader == n.cfg.NodeID {
		return ""
	}
	return leader
}

// SetLeader is called by the Raft layer when it observes a new leader, either
// by winning an election or by accepting AppendEntries from one. The empty
// string means "no leader currently known".
func (n *Node) SetLeader(id string) {
	prev, _ := n.leader.Load().(string)
	if prev == id {
		return
	}
	n.leader.Store(id)
	if id == "" {
		log.Print("leader unset")
	} else {
		log.Printf("leader is now %s", id)
	}
}

func (n *Node) Peers() []Peer { return n.cfg.Peers }

func (n *Node) Run(ctx context.Context) error {
	log.Printf("node %s starting; %d peers; gRPC listening on %s",
		n.cfg.NodeID, len(n.cfg.Peers), n.server.Addr())

	errCh := make(chan error, 1)
	go func() { errCh <- n.server.Serve() }()

	select {
	case <-ctx.Done():
		n.server.GracefulStop()
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
