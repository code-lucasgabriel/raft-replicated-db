// Package node is the composition root: it builds the Raft node, the FSM,
// the KV store, and the gRPC server, and runs them together.
package node

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/hashicorp/raft"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/raftnode"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/server"
)

type Config struct {
	NodeID    string
	GRPCPort  int
	BindAddr  string          // local Raft TCP listen address ("host:port")
	DataDir   string          // BoltDB + snapshots
	Peers     []raftnode.Peer // full cluster membership including self (raft addresses)
	Bootstrap bool            // first node, first boot only
}

type Node struct {
	cfg     Config
	server  *server.Server
	raftN   *raftnode.Node
}

func New(cfg Config) (*Node, error) {
	rn, err := raftnode.New(raftnode.Config{
		NodeID:    cfg.NodeID,
		DataDir:   cfg.DataDir,
		BindAddr:  cfg.BindAddr,
		Peers:     cfg.Peers,
		Bootstrap: cfg.Bootstrap,
		LogOutput: os.Stderr,
	})
	if err != nil {
		return nil, err
	}

	dbSvc := server.NewDBService(rn.Raft, rn.Store)
	srv, err := server.New(cfg.GRPCPort, dbSvc)
	if err != nil {
		_ = rn.Raft.Shutdown().Error()
		return nil, err
	}

	return &Node{cfg: cfg, server: srv, raftN: rn}, nil
}

func (n *Node) Run(ctx context.Context) error {
	log.Printf("node %s starting; %d peers; raft on %s; gRPC on %s; data=%s; bootstrap=%t",
		n.cfg.NodeID, len(n.cfg.Peers), n.cfg.BindAddr, n.server.Addr(), n.cfg.DataDir, n.cfg.Bootstrap)

	go n.logLeaderChanges(ctx)

	srvErrCh := make(chan error, 1)
	go func() { srvErrCh <- n.server.Serve() }()

	select {
	case <-ctx.Done():
		n.server.GracefulStop()
		if err := n.raftN.Raft.Shutdown().Error(); err != nil {
			log.Printf("raft shutdown: %v", err)
		}
		return ctx.Err()
	case err := <-srvErrCh:
		_ = n.raftN.Raft.Shutdown().Error()
		return err
	}
}

// logLeaderChanges polls raft.State() and logs role transitions. Useful for
// operators following along by `docker compose logs -f`. hashicorp/raft has
// a LeaderCh channel that surfaces leadership changes; we use State() so we
// also notice followers learning about a new leader.
func (n *Node) logLeaderChanges(ctx context.Context) {
	var lastState raft.RaftState
	var lastLeader raft.ServerID
	first := true
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			st := n.raftN.Raft.State()
			_, leader := n.raftN.Raft.LeaderWithID()
			if first || st != lastState {
				log.Printf("raft state: %s", st)
				lastState = st
			}
			if first || leader != lastLeader {
				if leader == "" {
					log.Print("leader unknown")
				} else {
					log.Printf("leader is now %s", leader)
				}
				lastLeader = leader
			}
			first = false
		}
	}
}
