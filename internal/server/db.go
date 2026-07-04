package server

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/fsm"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/ramutex"
	"github.com/code-lucasgabriel/raft-replicated-db/internal/store"
)

// applyTimeout caps how long a Put / Delete waits for Raft to commit and apply.
// Tuned for a single-DC cluster.
const applyTimeout = 5 * time.Second

// lockTimeout caps how long an Incr waits to enter the distributed critical
// section when the client didn't set its own deadline. Ricart-Agrawala needs
// a grant from EVERY peer, so a single dead node blocks acquisition until
// this fires — the availability trade-off mutual exclusion makes and Raft
// (quorum-based) doesn't.
const lockTimeout = 30 * time.Second

// DBService is the client-facing key/value API.
//
// Writes (Put / Delete) are accepted only on the leader: the operation is
// JSON-encoded as a fsm.Command, proposed via raft.Apply, and acknowledged
// once the FSM has applied it locally (which only happens after the entry
// is committed by a quorum). On a follower, the response carries the
// leader's node id in LeaderHint so the client can retry there.
//
// Reads (Get) are served directly from the local KV store and may therefore
// be stale — see linearizability note in architecture.md.
//
// Lamport protocol: every handler treats the incoming request as a message
// receive (clock.Observe) and stamps its response as a send (clock.Tick).
// Proposed commands carry the timestamp of the propose event, which every
// replica observes in FSM.Apply.
type DBService struct {
	dbv1.UnimplementedDBServer

	nodeID string
	raft   *raft.Raft
	store  *store.KV
	clock  *lamport.Clock
	mtx    *ramutex.Mutex
	peers  *Peers
}

func NewDBService(nodeID string, r *raft.Raft, s *store.KV, c *lamport.Clock, mtx *ramutex.Mutex, peers *Peers) *DBService {
	return &DBService{nodeID: nodeID, raft: r, store: s, clock: c, mtx: mtx, peers: peers}
}

func (s *DBService) Get(_ context.Context, req *dbv1.GetRequest) (*dbv1.GetResponse, error) {
	s.clock.Observe(req.GetLamportTime())
	v, ok := s.store.Get(req.GetKey())
	return &dbv1.GetResponse{
		Value:       v,
		Found:       ok,
		LeaderHint:  s.leaderHint(), // empty when we are the leader
		LamportTime: s.clock.Tick(),
	}, nil
}

func (s *DBService) Put(_ context.Context, req *dbv1.PutRequest) (*dbv1.PutResponse, error) {
	s.clock.Observe(req.GetLamportTime())
	hint, err := s.propose(fsm.Command{Op: "put", Key: req.GetKey(), Value: req.GetValue()})
	if err != nil {
		return nil, err
	}
	return &dbv1.PutResponse{LeaderHint: hint, LamportTime: s.clock.Tick()}, nil
}

func (s *DBService) Delete(_ context.Context, req *dbv1.DeleteRequest) (*dbv1.DeleteResponse, error) {
	s.clock.Observe(req.GetLamportTime())
	hint, err := s.propose(fsm.Command{Op: "delete", Key: req.GetKey()})
	if err != nil {
		return nil, err
	}
	return &dbv1.DeleteResponse{LeaderHint: hint, LamportTime: s.clock.Tick()}, nil
}

// Incr atomically increments a numeric key cluster-wide. Unlike Put/Delete
// it works on ANY node: the receiving node enters the Ricart-Agrawala
// critical section (so no other locked Incr runs anywhere in the cluster),
// then performs read-modify-write against the Raft leader, then releases.
//
// With unsafe=true the lock is skipped. That serves the lost-update
// experiment (concurrent unsafe Incrs race on read-modify-write) and
// internal forwarding: a node already inside the CS forwards to the leader
// with unsafe=true — taking the lock again there would deadlock, since the
// leader would wait for a grant this node can't give while holding the CS.
func (s *DBService) Incr(ctx context.Context, req *dbv1.IncrRequest) (*dbv1.IncrResponse, error) {
	s.clock.Observe(req.GetLamportTime())

	if !req.GetUnsafe() {
		lockCtx := ctx
		if _, ok := ctx.Deadline(); !ok {
			var cancel context.CancelFunc
			lockCtx, cancel = context.WithTimeout(ctx, lockTimeout)
			defer cancel()
		}
		if err := s.mtx.Lock(lockCtx); err != nil {
			return nil, status.Errorf(codes.Unavailable, "acquire distributed lock: %v", err)
		}
		defer s.mtx.Unlock()
	}

	newVal, err := s.execIncr(ctx, req.GetKey(), req.GetDelta())
	if err != nil {
		return nil, err
	}
	return &dbv1.IncrResponse{NewValue: newVal, LamportTime: s.clock.Tick()}, nil
}

// execIncr performs the read-modify-write on the current Raft leader,
// retrying through leader changes. Local leader path: a raft Barrier first,
// so a freshly elected leader has applied every previously committed entry
// before we read (otherwise the read could miss the previous CS holder's
// write and lose an update despite the lock).
func (s *DBService) execIncr(ctx context.Context, key string, delta int64) (int64, error) {
	const attempts = 3
	for i := 0; i < attempts; i++ {
		if err := ctx.Err(); err != nil {
			return 0, status.FromContextError(err).Err()
		}
		_, leaderID := s.raft.LeaderWithID()
		switch {
		case leaderID == "":
			// No leader (election in progress) — wait and re-resolve.
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ctx.Done():
				return 0, status.FromContextError(ctx.Err()).Err()
			}

		case string(leaderID) == s.nodeID:
			newVal, retry, err := s.localIncr(key, delta)
			if err != nil {
				return 0, err
			}
			if !retry {
				return newVal, nil
			}
			// Lost leadership mid-operation; re-resolve.

		default:
			client, err := s.peers.DB(string(leaderID))
			if err != nil {
				return 0, status.Errorf(codes.Internal, "leader %s not in peer set: %v", leaderID, err)
			}
			resp, err := client.Incr(ctx, &dbv1.IncrRequest{
				Key:         key,
				Delta:       delta,
				Unsafe:      true, // the CS is already held by this node (or deliberately skipped)
				LamportTime: s.clock.Tick(),
			})
			if err != nil {
				// The presumed leader may have just lost leadership or died;
				// re-resolve and retry unless the error is the value's fault.
				if status.Code(err) == codes.InvalidArgument {
					return 0, err
				}
				continue
			}
			s.clock.Observe(resp.GetLamportTime())
			return resp.GetNewValue(), nil
		}
	}
	return 0, status.Errorf(codes.Unavailable, "incr %q: no stable leader after %d attempts", key, attempts)
}

// localIncr runs the read-modify-write on this node while it is leader.
// retry=true means leadership was lost and the caller should re-resolve.
func (s *DBService) localIncr(key string, delta int64) (newVal int64, retry bool, err error) {
	// Barrier: block until everything committed before now is applied to
	// our FSM. Covers the window right after winning an election.
	if err := s.raft.Barrier(applyTimeout).Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return 0, true, nil
		}
		return 0, false, status.Errorf(codes.Unavailable, "raft barrier: %v", err)
	}

	cur := int64(0)
	if raw, ok := s.store.Get(key); ok {
		cur, err = strconv.ParseInt(string(raw), 10, 64)
		if err != nil {
			return 0, false, status.Errorf(codes.InvalidArgument, "key %q holds non-numeric value %q", key, raw)
		}
	}
	newVal = cur + delta

	hint, err := s.propose(fsm.Command{Op: "put", Key: key, Value: []byte(strconv.FormatInt(newVal, 10))})
	if err != nil {
		return 0, false, err
	}
	if hint != "" {
		return 0, true, nil // deposed between Barrier and Apply
	}
	return newVal, false, nil
}

// propose runs a command through Raft. Returns ("", nil) on success;
// (leaderID, nil) when we're not the leader (the client retries elsewhere);
// ("", err) on a real failure.
func (s *DBService) propose(cmd fsm.Command) (string, error) {
	if s.raft.State() != raft.Leader {
		return s.leaderHint(), nil
	}
	// Stamp the propose event. Handing the entry to Raft is the "send" of a
	// message that every replica receives in FSM.Apply.
	cmd.Time = s.clock.Tick()
	cmd.Origin = s.nodeID
	data, err := fsm.Encode(cmd)
	if err != nil {
		return "", err
	}
	future := s.raft.Apply(data, applyTimeout)
	if err := future.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return s.leaderHint(), nil
		}
		return "", err
	}
	// FSM.Apply returns either nil or an error describing a malformed entry.
	if applyErr, _ := future.Response().(error); applyErr != nil {
		return "", applyErr
	}
	return "", nil
}

// leaderHint returns "" when this node IS the leader, otherwise the leader's
// node id so the client can retry against it.
func (s *DBService) leaderHint() string {
	if s.raft.State() == raft.Leader {
		return ""
	}
	_, id := s.raft.LeaderWithID()
	return string(id)
}
