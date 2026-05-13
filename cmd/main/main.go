package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/node"
)

func main() {
	cfg := node.Config{
		NodeID:   envOr("NODE_ID", "node-1"),
		GRPCPort: mustAtoi(envOr("NODE_PORT", "5000")),
		Peers:    mustParsePeers(envOr("PEERS", "node-1=localhost:5000")),
	}

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

// mustParsePeers parses "id1=addr1,id2=addr2,..." into the peer list.
func mustParsePeers(raw string) []node.Peer {
	parts := strings.Split(raw, ",")
	peers := make([]node.Peer, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		eq := strings.IndexByte(p, '=')
		if eq <= 0 || eq == len(p)-1 {
			log.Fatalf("invalid PEERS entry %q (expected id=addr)", p)
		}
		peers = append(peers, node.Peer{ID: p[:eq], Addr: p[eq+1:]})
	}
	if len(peers) == 0 {
		log.Fatal("PEERS must contain at least one entry")
	}
	return peers
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
