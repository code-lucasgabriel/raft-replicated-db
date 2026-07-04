// Package server hosts the gRPC server carrying two services on one port:
// the client-facing DB API and the peer-facing Ricart-Agrawala Mutex API.
// Raft cluster traffic uses a separate TCP transport on a different port —
// see internal/raftnode.
package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
	mutexv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/mutex/v1"
)

type Server struct {
	grpc *grpc.Server
	lis  net.Listener
}

func New(port int, db *DBService, mtx *MutexService) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen :%d: %w", port, err)
	}
	gs := grpc.NewServer()
	dbv1.RegisterDBServer(gs, db)
	mutexv1.RegisterMutexServer(gs, mtx)
	return &Server{grpc: gs, lis: lis}, nil
}

func (s *Server) Serve() error  { return s.grpc.Serve(s.lis) }
func (s *Server) GracefulStop() { s.grpc.GracefulStop() }
func (s *Server) Addr() string  { return s.lis.Addr().String() }
