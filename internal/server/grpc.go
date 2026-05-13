// Package server hosts the client DB API and the internal Raft API on a
// single gRPC server.
package server

import (
	"fmt"
	"net"

	"google.golang.org/grpc"

	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
	raftv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/raft/v1"
)

type Server struct {
	grpc *grpc.Server
	lis  net.Listener
}

func New(port int, db *DBService, raft *RaftService) (*Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen :%d: %w", port, err)
	}
	gs := grpc.NewServer()
	dbv1.RegisterDBServer(gs, db)
	raftv1.RegisterRaftServer(gs, raft)
	return &Server{grpc: gs, lis: lis}, nil
}

func (s *Server) Serve() error  { return s.grpc.Serve(s.lis) }
func (s *Server) GracefulStop() { s.grpc.GracefulStop() }
func (s *Server) Addr() string  { return s.lis.Addr().String() }
