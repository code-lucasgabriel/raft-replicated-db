package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/node"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/raftnode"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/server"
)

func main() {
	cfg := node.Config{
		NodeID:    envOr("NODE_ID", "node-1"),
		GRPCPort:  mustAtoi(envOr("NODE_PORT", "5000")),
		DataDir:   envOr("DATA_DIR", "/var/lib/raft-db"),
		Peers:     mustParsePeers(envOr("PEERS", "node-1=node-1:7000")),
		Bootstrap: envOr("BOOTSTRAP", "false") == "true",
	}
	// Local Raft bind address is the entry in PEERS that matches NODE_ID.
	cfg.BindAddr = peerAddrOrDie(cfg.NodeID, cfg.Peers)
	// gRPC peer endpoints (Ricart-Agrawala + leader forwarding). When
	// GRPC_PEERS is unset, derive them: same host as the Raft address, gRPC
	// port — right for docker-compose, where every node uses one NODE_PORT.
	// Multi-node-on-localhost setups (distinct ports) must set GRPC_PEERS.
	cfg.GRPCPeers = grpcPeers(os.Getenv("GRPC_PEERS"), cfg.Peers, cfg.GRPCPort)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	n, err := node.New(cfg)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}
	if err := n.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("node run: %v", err)
	}
}

// mustParsePeers parses "id1=addr1,id2=addr2,..." into the peer list. The
// addresses are the Raft TCP transport endpoints, not the client gRPC port.
func mustParsePeers(raw string) []raftnode.Peer {
	parts := strings.Split(raw, ",")
	peers := make([]raftnode.Peer, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq <= 0 || eq == len(p)-1 {
			log.Fatalf("invalid PEERS entry %q (expected id=addr)", p)
		}
		peers = append(peers, raftnode.Peer{ID: p[:eq], Addr: p[eq+1:]})
	}
	if len(peers) == 0 {
		log.Fatal("PEERS must contain at least one entry")
	}
	return peers
}

// grpcPeers parses GRPC_PEERS ("id1=addr1,...") or, when raw is empty,
// derives each peer's gRPC endpoint from its Raft host + the shared gRPC
// port.
func grpcPeers(raw string, raftPeers []raftnode.Peer, grpcPort int) []server.Peer {
	if raw != "" {
		parsed := mustParsePeers(raw)
		peers := make([]server.Peer, 0, len(parsed))
		for _, p := range parsed {
			peers = append(peers, server.Peer{ID: p.ID, Addr: p.Addr})
		}
		return peers
	}
	peers := make([]server.Peer, 0, len(raftPeers))
	for _, p := range raftPeers {
		host, _, err := net.SplitHostPort(p.Addr)
		if err != nil {
			log.Fatalf("invalid peer address %q: %v", p.Addr, err)
		}
		peers = append(peers, server.Peer{ID: p.ID, Addr: net.JoinHostPort(host, strconv.Itoa(grpcPort))})
	}
	return peers
}

func peerAddrOrDie(id string, peers []raftnode.Peer) string {
	for _, p := range peers {
		if p.ID == id {
			return p.Addr
		}
	}
	log.Fatalf("NODE_ID %q not found in PEERS", id)
	return ""
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalf("invalid int %q: %v", s, err)
	}
	return n
}
