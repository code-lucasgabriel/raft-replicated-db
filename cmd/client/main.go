// Command client is a small CLI for talking to the cluster. It maintains
// its own Lamport clock (a client is a process in the distributed system
// too: it stamps requests and observes response timestamps) and follows
// LeaderHint redirects on writes.
//
// Usage:
//
//	client [-nodes id=addr,...] [-node id] [-timeout 30s] <command>
//
//	get KEY                          read from one node (possibly stale)
//	put KEY VALUE                    write through the Raft leader
//	del KEY                          delete through the Raft leader
//	incr KEY [DELTA]                 cluster-wide atomic increment (Ricart-Agrawala)
//	incr-unsafe KEY [DELTA]          increment WITHOUT the distributed lock
//	bench-incr KEY [-n 30] [-c 3] [-unsafe]
//	                                 fire N increments across all nodes with C
//	                                 workers, then audit the final value on
//	                                 every replica — the lost-update experiment
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/code-lucasgabriel/raft-replicated-db/internal/lamport"
	dbv1 "github.com/code-lucasgabriel/raft-replicated-db/internal/pb/db/v1"
)

const defaultNodes = "node-1=127.0.0.1:5001,node-2=127.0.0.1:5002,node-3=127.0.0.1:5003"

func main() {
	log.SetFlags(0)

	nodesFlag := flag.String("nodes", defaultNodes, "cluster membership as id=addr,... (gRPC endpoints)")
	nodeFlag := flag.String("node", "", "node id to send the command to (default: first of -nodes)")
	timeoutFlag := flag.Duration("timeout", 30*time.Second, "per-operation timeout")
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}

	c, err := dial(*nodesFlag)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer c.close()

	target := *nodeFlag
	if target == "" {
		target = c.order[0]
	}
	if _, ok := c.clients[target]; !ok {
		log.Fatalf("unknown node %q (have %s)", target, strings.Join(c.order, ", "))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeoutFlag)
	defer cancel()

	args := flag.Args()
	switch cmd, rest := args[0], args[1:]; cmd {
	case "get":
		expectArgs(rest, 1, "get KEY")
		err = c.get(ctx, target, rest[0])
	case "put":
		expectArgs(rest, 2, "put KEY VALUE")
		err = c.put(ctx, target, rest[0], []byte(rest[1]))
	case "del":
		expectArgs(rest, 1, "del KEY")
		err = c.del(ctx, target, rest[0])
	case "incr":
		err = c.incr(ctx, target, rest, false)
	case "incr-unsafe":
		err = c.incr(ctx, target, rest, true)
	case "bench-incr":
		err = c.benchIncr(ctx, rest)
	default:
		log.Fatalf("unknown command %q", cmd)
	}
	if err != nil {
		log.Fatalf("%s: %v", args[0], err)
	}
}

func expectArgs(rest []string, n int, usage string) {
	if len(rest) != n {
		log.Fatalf("usage: %s", usage)
	}
}

// cluster is the client's view: one conn per node, a Lamport clock, and the
// id=addr map that lets LeaderHint redirects be followed by id.
type cluster struct {
	order   []string
	clients map[string]dbv1.DBClient
	conns   []*grpc.ClientConn
	clock   lamport.Clock
}

func dial(nodes string) (*cluster, error) {
	c := &cluster{clients: make(map[string]dbv1.DBClient)}
	for _, entry := range strings.Split(nodes, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		id, addr, ok := strings.Cut(entry, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("invalid -nodes entry %q (want id=addr)", entry)
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("node %s (%s): %w", id, addr, err)
		}
		c.order = append(c.order, id)
		c.clients[id] = dbv1.NewDBClient(conn)
		c.conns = append(c.conns, conn)
	}
	if len(c.order) == 0 {
		return nil, fmt.Errorf("-nodes is empty")
	}
	return c, nil
}

func (c *cluster) close() {
	for _, conn := range c.conns {
		_ = conn.Close()
	}
}

func (c *cluster) get(ctx context.Context, node, key string) error {
	resp, err := c.clients[node].Get(ctx, &dbv1.GetRequest{Key: key, LamportTime: c.clock.Tick()})
	if err != nil {
		return err
	}
	local := c.clock.Observe(resp.GetLamportTime())
	hint := resp.GetLeaderHint()
	role := "leader"
	if hint != "" {
		role = "follower of " + hint
	}
	if !resp.GetFound() {
		fmt.Printf("(nil)  served-by=%s (%s) lamport{node=%d client=%d}\n",
			node, role, resp.GetLamportTime(), local)
		return nil
	}
	fmt.Printf("%s  served-by=%s (%s) lamport{node=%d client=%d}\n",
		resp.GetValue(), node, role, resp.GetLamportTime(), local)
	return nil
}

// put follows LeaderHint redirects: a follower answers with the leader's id
// and the client re-sends there. Bounded by the cluster size — a correct
// hint chain can't be longer than that.
func (c *cluster) put(ctx context.Context, node, key string, value []byte) error {
	for hop := 0; hop <= len(c.order); hop++ {
		resp, err := c.clients[node].Put(ctx, &dbv1.PutRequest{Key: key, Value: value, LamportTime: c.clock.Tick()})
		if err != nil {
			return err
		}
		local := c.clock.Observe(resp.GetLamportTime())
		if hint := resp.GetLeaderHint(); hint != "" {
			fmt.Printf("redirected: %s is not the leader, retrying on %s\n", node, hint)
			if _, ok := c.clients[hint]; !ok {
				return fmt.Errorf("leader hint %q not in -nodes", hint)
			}
			node = hint
			continue
		}
		fmt.Printf("OK  committed-by=%s hops=%d lamport{node=%d client=%d}\n",
			node, hop, resp.GetLamportTime(), local)
		return nil
	}
	return fmt.Errorf("no leader after %d redirects", len(c.order))
}

func (c *cluster) del(ctx context.Context, node, key string) error {
	for hop := 0; hop <= len(c.order); hop++ {
		resp, err := c.clients[node].Delete(ctx, &dbv1.DeleteRequest{Key: key, LamportTime: c.clock.Tick()})
		if err != nil {
			return err
		}
		local := c.clock.Observe(resp.GetLamportTime())
		if hint := resp.GetLeaderHint(); hint != "" {
			fmt.Printf("redirected: %s is not the leader, retrying on %s\n", node, hint)
			if _, ok := c.clients[hint]; !ok {
				return fmt.Errorf("leader hint %q not in -nodes", hint)
			}
			node = hint
			continue
		}
		fmt.Printf("OK  committed-by=%s hops=%d lamport{node=%d client=%d}\n",
			node, hop, resp.GetLamportTime(), local)
		return nil
	}
	return fmt.Errorf("no leader after %d redirects", len(c.order))
}

func (c *cluster) incr(ctx context.Context, node string, rest []string, unsafe bool) error {
	if len(rest) < 1 || len(rest) > 2 {
		return fmt.Errorf("usage: incr KEY [DELTA]")
	}
	delta := int64(1)
	if len(rest) == 2 {
		var err error
		if delta, err = strconv.ParseInt(rest[1], 10, 64); err != nil {
			return fmt.Errorf("invalid delta %q: %w", rest[1], err)
		}
	}
	resp, err := c.clients[node].Incr(ctx, &dbv1.IncrRequest{
		Key: rest[0], Delta: delta, Unsafe: unsafe, LamportTime: c.clock.Tick(),
	})
	if err != nil {
		return err
	}
	local := c.clock.Observe(resp.GetLamportTime())
	fmt.Printf("%d  via=%s locked=%t lamport{node=%d client=%d}\n",
		resp.GetNewValue(), node, !unsafe, resp.GetLamportTime(), local)
	return nil
}

// benchIncr is the mutual-exclusion experiment: N read-modify-write
// increments spread across every node with C concurrent workers. With the
// lock the final value must equal N; without it, overlapping get->put
// windows silently drop increments.
func (c *cluster) benchIncr(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("bench-incr", flag.ExitOnError)
	n := fs.Int("n", 30, "total increments")
	workers := fs.Int("c", 3, "concurrent workers")
	unsafe := fs.Bool("unsafe", false, "skip the distributed lock")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: bench-incr [-n N] [-c C] [-unsafe] KEY")
	}
	key := fs.Arg(0)

	mode := "LOCKED (Ricart-Agrawala)"
	if *unsafe {
		mode = "UNSAFE (no lock)"
	}

	baseline := int64(0)
	if resp, err := c.clients[c.order[0]].Get(ctx, &dbv1.GetRequest{Key: key, LamportTime: c.clock.Tick()}); err == nil && resp.GetFound() {
		c.clock.Observe(resp.GetLamportTime())
		if v, err := strconv.ParseInt(string(resp.GetValue()), 10, 64); err == nil {
			baseline = v
		}
	}

	fmt.Printf("firing %d increments of %q (baseline %d) across %d nodes, %d workers, mode=%s\n",
		*n, key, baseline, len(c.order), *workers, mode)

	var next, failed atomic.Int64
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := next.Add(1) - 1
				if i >= int64(*n) {
					return
				}
				node := c.order[int(i)%len(c.order)] // spread over every node
				_, err := c.clients[node].Incr(ctx, &dbv1.IncrRequest{
					Key: key, Delta: 1, Unsafe: *unsafe, LamportTime: c.clock.Tick(),
				})
				if err != nil {
					failed.Add(1)
					fmt.Printf("  incr %d via %s FAILED: %v\n", i, node, err)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	// Let follower FSMs catch up before auditing replicas.
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("done in %s (%.1f incr/s), %d failed\n",
		elapsed.Round(time.Millisecond), float64(*n)/elapsed.Seconds(), failed.Load())
	succeeded := int64(*n) - failed.Load()
	expected := baseline + succeeded
	for _, node := range c.order {
		resp, err := c.clients[node].Get(ctx, &dbv1.GetRequest{Key: key, LamportTime: c.clock.Tick()})
		if err != nil {
			fmt.Printf("  %s: get failed: %v\n", node, err)
			continue
		}
		c.clock.Observe(resp.GetLamportTime())
		fmt.Printf("  %s reads %q = %s\n", node, key, resp.GetValue())
	}
	fmt.Printf("expected final value: %d (baseline %d + %d successful increments)\n",
		expected, baseline, succeeded)
	if *unsafe {
		fmt.Println("any shortfall vs expected = lost updates from racing read-modify-write")
	}
	return nil
}
