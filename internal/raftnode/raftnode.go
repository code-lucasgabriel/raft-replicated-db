// Package raftnode constructs a hashicorp/raft node wired to:
//   - a BoltDB-backed log store and stable store,
//   - a file-backed snapshot store,
//   - a native TCP transport,
//   - our FSM (which mutates the in-memory KV store).
//
// This is the seam between "we own this code" and "the library owns this code".
// Everything in this package is configuration and lifecycle; the consensus
// algorithm itself lives in github.com/hashicorp/raft.
package raftnode

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/fsm"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/store"
)

// Peer is one entry in the cluster's static seed list.
type Peer struct {
	ID   string // unique node identifier
	Addr string // host:port of the peer's Raft TCP transport
}

// Config controls how the Raft node is built.
type Config struct {
	NodeID    string         // must appear in Peers
	DataDir   string         // BoltDB files + snapshots live under here
	BindAddr  string         // local Raft TCP listen address, "host:port"
	Peers     []Peer         // full cluster membership, including self
	Bootstrap bool           // first node, first boot only — writes the initial Configuration to the log
	Clock     *lamport.Clock // node-wide Lamport clock, observed by the FSM on every apply
	LogOutput io.Writer      // hashicorp/raft and bolt logs go here; nil = stderr
}

// Node bundles the raft.Raft handle with the FSM + store it operates on.
// Callers use Raft for proposing writes and inspecting leadership; Store
// for serving local reads.
type Node struct {
	Raft  *raft.Raft
	FSM   *fsm.FSM
	Store *store.KV
}

// New constructs and starts the Raft node. On Close (driven by the caller via
// raft.Raft.Shutdown), the underlying transport and BoltDB files are released.
func New(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("raftnode: NodeID required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raftnode: DataDir required")
	}
	if cfg.BindAddr == "" {
		return nil, fmt.Errorf("raftnode: BindAddr required")
	}
	if cfg.Clock == nil {
		return nil, fmt.Errorf("raftnode: Clock required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("raftnode: mkdir DataDir: %w", err)
	}
	logOut := cfg.LogOutput
	if logOut == nil {
		logOut = os.Stderr
	}

	s := store.New()
	f := fsm.New(s, cfg.Clock)

	rcfg := raft.DefaultConfig()
	rcfg.LocalID = raft.ServerID(cfg.NodeID)
	rcfg.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Level:  hclog.Info,
		Output: logOut,
	})

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: open log store: %w", err)
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, fmt.Errorf("raftnode: open stable store: %w", err)
	}
	snapshots, err := raft.NewFileSnapshotStore(cfg.DataDir, 2, logOut)
	if err != nil {
		return nil, fmt.Errorf("raftnode: snapshot store: %w", err)
	}

	advertise, err := net.ResolveTCPAddr("tcp", cfg.BindAddr)
	if err != nil {
		return nil, fmt.Errorf("raftnode: resolve %q: %w", cfg.BindAddr, err)
	}
	transport, err := raft.NewTCPTransport(cfg.BindAddr, advertise, 3, 10*time.Second, logOut)
	if err != nil {
		return nil, fmt.Errorf("raftnode: tcp transport: %w", err)
	}

	r, err := raft.NewRaft(rcfg, f, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("raftnode: new raft: %w", err)
	}

	// Bootstrap is a one-shot, one-node-only operation. On a virgin cluster
	// it writes the initial Configuration (the static peer list) to the log;
	// every other node receives that Configuration via AppendEntries. On a
	// node that already has state, this is a no-op.
	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapshots)
		if err != nil {
			return nil, fmt.Errorf("raftnode: HasExistingState: %w", err)
		}
		if !hasState {
			servers := make([]raft.Server, 0, len(cfg.Peers))
			for _, p := range cfg.Peers {
				servers = append(servers, raft.Server{
					ID:      raft.ServerID(p.ID),
					Address: raft.ServerAddress(p.Addr),
				})
			}
			if err := r.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil {
				return nil, fmt.Errorf("raftnode: bootstrap: %w", err)
			}
		}
	}

	return &Node{Raft: r, FSM: f, Store: s}, nil
}
