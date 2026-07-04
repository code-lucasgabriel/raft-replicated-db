# Architecture

A replicated in-memory KV store backed by [`hashicorp/raft`](https://github.com/hashicorp/raft), extended with Lamport logical clocks on every message channel and Ricart-Agrawala distributed mutual exclusion powering an atomic cluster-wide `Incr`. This document covers component layout, data flow, persistence, failure modes, and the architectural decisions worth pushing on. An HTML companion with the same content lives at `docs/architecture.html`.

## 1. Scope

- **What it is:** a 3-node, static-membership KV store. Clients speak gRPC. Writes go through Raft; reads are local. Every message carries a Lamport timestamp; `Incr` runs read-modify-write inside a Ricart-Agrawala critical section.
- **What it isn't:** sharded, dynamically-membership'd, multi-region, secure-on-the-wire, observability-rich. Each of these is a deliberate non-goal for the learning version; the doc flags where each would land.

## 2. Topology

Three nodes, identical binary, two listen ports each. No external coordinator (etcd / Consul / ZK). Membership comes from the `PEERS` env var baked at boot.

```
                  ┌───────────────────────┐         ┌───────────────────────┐
   client ─gRPC─▶ │ node-1                │ ─raft─▶ │ node-2                │
                  │  :5000  DB+Mutex gRPC │         │  :5000  DB+Mutex gRPC │
                  │  :7000  raft TCP      │ ◀─raft─ │  :7000  raft TCP      │
                  │  /var/lib/raft-db     │         │  /var/lib/raft-db     │
                  └─────────┬─────────────┘         └───────────┬───────────┘
                            │ raft (AppendEntries, RequestVote, InstallSnapshot)
                            ▼
                  ┌───────────────────────┐
                  │ node-3                │
                  │  :5000  DB+Mutex gRPC │
                  │  :7000  raft TCP      │
                  │  /var/lib/raft-db     │
                  └───────────────────────┘
```

Two ports per node, **separate transports**:

| Port | Protocol            | Carries                                                                    |
|------|---------------------|-----------------------------------------------------------------------------|
| 5000 | gRPC (HTTP/2)       | client DB API (`Get`, `Put`, `Delete`, `Incr`) + peer `Mutex` service (Ricart-Agrawala) |
| 7000 | hashicorp/raft TCP  | `RequestVote`, `AppendEntries`, `InstallSnapshot` between peers             |

Port 5000 hosts two gRPC services on one server: `DB` for clients and `Mutex` for peers. Peer nodes dial each other's gRPC port (from `GRPC_PEERS`) for two purposes: Ricart-Agrawala REQUEST/grant traffic and forwarding `Incr`'s write to the current leader.

The single-port-via-gRPC alternative ([`Jille/raft-grpc-transport`](https://github.com/Jille/raft-grpc-transport)) was considered and rejected: it adds a third-party dep and re-encodes hashicorp/raft's internal messages without changing semantics. Two ports is one extra line of docker-compose and zero extra deps.

## 3. Component map

```
cmd/main/                  node entrypoint; env-var parsing
cmd/client/                CLI client: get/put/del/incr + the bench-incr experiment
internal/
  node/                    composition root: builds raftnode + server + mutex, drives lifecycle
  raftnode/                hashicorp/raft wiring: stores, transport, bootstrap, log output
  fsm/                     raft.FSM: decodes Commands, mutates the store, snapshot/restore
  store/                   in-memory map[string][]byte with RWMutex; Snapshot/Restore as JSON
  lamport/                 Lamport logical clock: Tick (local/send), Observe (receive)
  ramutex/                 Ricart-Agrawala algorithm core, transport-agnostic
  server/                  gRPC services: DB (clients), Mutex (peers), peer conn pool
  pb/                      generated protobuf bindings
proto/db/v1/db.proto       client API schema
proto/mutex/v1/mutex.proto peer mutex transport schema
```

Dependency direction (anything left depends on what's to its right):

```
cmd ─▶ node ─▶ {server, raftnode, ramutex}
         └─▶ raftnode ─▶ {fsm, store, lamport, hashicorp/raft, raft-boltdb}
         └─▶ server   ─▶ {hashicorp/raft, fsm, store, lamport, ramutex}
         └─▶ ramutex  ─▶ {lamport}
```

`server` and `raftnode` are siblings — they don't know about each other. `node` is the only package that wires them together. This is what lets you swap the gRPC layer (REST, JSON-RPC, anything) without touching the consensus layer, and vice versa. `ramutex` follows the same discipline: the algorithm core speaks to peers only through a one-method `Transport` interface; the gRPC implementation of that interface lives in `server`, so the algorithm is unit-testable with an in-process transport.

## 4. The write path

A `Put(k, v)` from a client:

```
1.  Client gRPC ──▶  node-X DBService.Put
2.  if node-X is NOT leader:
        return PutResponse{LeaderHint: <leader id>}      ← client retries on hinted node
3.  cmd = Command{Op:"put", Key:k, Value:v}
    data = json.Marshal(cmd)
4.  future = raft.Apply(data, 5s)
5.  hashicorp/raft on the leader:
        append entry to local log (BoltDB)
        broadcast AppendEntries to followers
        wait until majority of followers ack the entry
        advance commitIndex past the entry
        invoke FSM.Apply(entry) on every node (including self)
6.  FSM.Apply:
        json.Unmarshal(entry.Data, &cmd)
        store.Put(cmd.Key, cmd.Value)
7.  future.Error() returns nil  ──▶  DBService returns PutResponse{}
```

Key invariants enforced by this path:

- **At most one in-flight commit per entry.** The leader pipelines AppendEntries, but each entry has a single commit point: the index at which a majority has acknowledged.
- **FSM.Apply runs in log order on every node.** hashicorp/raft serialises FSM dispatch; the FSM does not need locks of its own beyond what the underlying store provides.
- **Acknowledgement-after-apply, not after-commit.** The leader returns success only after its own FSM has applied the entry, so the leader's read-after-write is consistent for that client. Other replicas may still be slightly behind.

`Delete(k)` is identical with `Op:"delete"`.

## 5. The read path

`Get(k)` reads from the local node's `store.KV` directly, without going through Raft.

```
1.  Client gRPC ──▶  node-X DBService.Get
2.  v, ok = store.Get(key)
3.  return GetResponse{Value: v, Found: ok, LeaderHint: <leader id if not self>}
```

This is **fast** (no quorum round-trip) and **potentially stale** (follower may be a few entries behind the leader). The `LeaderHint` is populated when the local node is not the leader, so a client that needs linearizability can retry on the leader.

A linearizable read on the leader is still not strictly linearizable in hashicorp/raft without `VerifyLeader` — the leader could be deposed without knowing it yet. The proper construction is **ReadIndex** (paper §8): on a `Get`, the leader confirms it's still leader via a quorum heartbeat, then serves the read from its state machine once `lastApplied ≥ readIndex`. hashicorp/raft exposes `VerifyLeader()` for the first half; the rest is straightforward. Not implemented here.

## 6. Persistence

Each node owns a directory (`DATA_DIR=/var/lib/raft-db` in containers, mounted as a named volume). hashicorp/raft writes three things to it:

```
/var/lib/raft-db/
  raft-log.db            ← BoltDB: replicated log entries (LogStore)
  raft-stable.db         ← BoltDB: currentTerm, votedFor (StableStore)
  snapshots/             ← FileSnapshotStore: periodic FSM snapshots + metadata
    1-5-1700000000000-meta.json
    1-5-1700000000000-state.bin
    ...
```

**LogStore** holds every replicated entry from index 1 onward, until log compaction trims earlier entries. Loss of `raft-log.db` on a node = that node has to re-receive the log from the leader (via AppendEntries with a low `prevLogIndex`, or via `InstallSnapshot` if the leader has already compacted past it).

**StableStore** holds the small handful of fields Raft must persist synchronously on every term change or vote. Critical for safety: losing `currentTerm` and `votedFor` violates Raft's single-vote-per-term guarantee on restart. BoltDB's `Sync: true` on these writes is what we trust.

**SnapshotStore** holds FSM snapshots. hashicorp/raft triggers a snapshot when the log grows past `SnapshotThreshold` entries (default 8192) and the interval since the last one exceeds `SnapshotInterval` (default 2 minutes). After a successful snapshot, the prefix of the log up to the snapshot's `lastIncludedIndex` is truncated.

A follower so far behind that the leader has already compacted past it receives an `InstallSnapshot` RPC, restores the FSM, and then resumes normal AppendEntries from the post-snapshot index.

## 7. Bootstrap and membership

The first time the cluster runs against virgin volumes, **exactly one node** (`BOOTSTRAP=true` — node-1 in docker-compose) calls `raft.BootstrapCluster` with the full peer list. This writes a single `Configuration` entry to that node's log; the entry then replicates to the other two nodes via AppendEntries. They start as followers, see the leader, and from that moment the cluster is up.

On any subsequent boot — including node-1 with `BOOTSTRAP=true` still set — `raft.HasExistingState` short-circuits the bootstrap: state already exists in BoltDB, so the library refuses to re-bootstrap and we skip.

This codebase has **no dynamic membership**. `raft.AddVoter` / `raft.RemoveVoter` are not exposed. The expectation is that the `PEERS` list does not change at runtime; if you need to add a node, you'd either:

1. Add an `AdminService` gRPC with `AddVoter` / `RemoveVoter` (proper way), or
2. Stop the cluster, run `raft.RecoverCluster` with the new peer set, restart (disaster-recovery path).

Both are deferred. The static-VM assumption is the project's design constraint.

## 8. Failure modes

| Failure                                | Effect                                                                              | Recovery                                                                                                   |
|----------------------------------------|-------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|
| Single follower down                   | Quorum (2/3) still met. Leader continues; follower's log gaps when it returns.      | Follower restarts, leader replays AppendEntries from its `nextIndex` until caught up.                       |
| Leader down                            | No writes accepted (`raft.Apply` returns `ErrLeadershipLost` or times out).         | Surviving majority holds an election; new leader emerges in ~150-300ms (hashicorp/raft default).            |
| Network partition (2:1)                | Majority side keeps committing. Minority side cannot elect a leader, returns hints. | Heals automatically when the partition resolves; minority side catches up via AppendEntries / InstallSnap.  |
| Network partition (1:1:1)              | No majority. No writes commit. Reads still served (stale).                          | One pair must re-join before progress resumes.                                                              |
| Total outage, BoltDB intact            | Cluster restarts; each node reads `raft-log.db` + `raft-stable.db` from disk.       | Election happens, log replays into the FSM, service resumes from `lastApplied`.                              |
| Total outage, BoltDB corrupt           | Bootstrap won't help (state exists or is partially written).                        | `raft.RecoverCluster` with a hand-written `peers.json` is the documented disaster path. Out of scope here.   |
| Single node BoltDB lost (volume nuked) | That node looks brand-new to itself but is still a voter in the live config.        | On boot the live leader sends a snapshot + AppendEntries; node catches up. No bootstrap needed.              |
| Any node down, locked `Incr` issued    | Ricart-Agrawala needs a grant from EVERY peer: acquisition blocks, `Incr` fails at the lock timeout. Raft ops (`Put`/`Get`/`Delete`) continue unaffected. | Restart the node. This all-peers fragility is inherent to RA — see §11 for the contrast with quorum-based approaches. |

The "single node volume nuked" case is the one most people get wrong on a first build: don't try to re-bootstrap. Just bring the node back with the same `NODE_ID` and empty `DATA_DIR`; the leader will repair it.

## 9. State machine semantics

The FSM is intentionally trivial — a `map[string][]byte`. The interesting properties come from how it's driven:

- **Deterministic.** `Apply` only reads the entry; no clocks, no randomness, no I/O. Two replicas applying the same log starting from the same snapshot will reach byte-identical states.
- **Idempotent at the command level.** A `Put k=v` applied twice is still `k=v`; a `Delete k` after a previous `Delete k` is a no-op. The Raft log itself never duplicates entries (each commits at a unique index), but command-level idempotence matters for client retries after a leader-hint redirect.
- **No transactions, no compare-and-swap.** The wire format (`Command`) has only `put` and `delete`. Adding `cas { key, expected, new }` is a one-field extension to `Command` and ~10 lines in `FSM.Apply`. The architecture supports it; the schema doesn't yet.

The choice of JSON for the `Command` encoding is a KISS call: legible in logs, no codegen, easy to extend. Switching to protobuf is mechanical and would buy ~20% smaller log entries.

## 10. Logical time

Every process — the three nodes *and* the CLI client — owns one Lamport clock (`internal/lamport`), following the two rules from Lamport 1978: `Tick()` before a local event or message send, `Observe(remote) = max(local, remote) + 1` on receive. Three message channels carry timestamps:

| Channel | Send event | Receive event |
|---|---|---|
| client ↔ node | request/response `lamport_time` fields; both sides Tick on send | both sides Observe |
| leader → replicas | `Command{Time, Origin}` stamped at `raft.Apply` propose | every node's `FSM.Apply` Observes — a committed log entry *is* a message from the leader to each replica |
| peer ↔ peer (mutex) | REQUEST carries the requester's fixed timestamp; the grant response carries the granter's clock | both sides Observe |

Two properties worth internalizing:

- **The clock is node state, not FSM state.** `FSM.Apply` observing `cmd.Time` is a local side effect: it isn't snapshotted and can't influence the replicated store, so apply determinism survives. Replaying old entries after a restart re-observes old timestamps — harmless, `Observe` is a max.
- **Lamport order is partial; ids make it total.** `ts(a) < ts(b)` does not imply a happened before b (they may be concurrent). Ricart-Agrawala needs a *total* order, so it compares `(timestamp, node id)` pairs — that tie-break is exactly why the mutex and the clock ship together.

## 11. Distributed mutual exclusion and `Incr`

`internal/ramutex` implements Ricart & Agrawala 1981. To enter the critical section a node stamps one REQUEST with its Lamport clock and sends it to every peer, entering only when **all** of them grant. A receiver grants immediately unless it holds the CS or is requesting with an earlier `(timestamp, id)` pair — then it defers the grant until its own exit. 2·(N−1) messages per entry, the paper's optimal count.

The paper's REQUEST/REPLY pair maps onto **one blocking gRPC call**: `Mutex.RequestCS` carries the REQUEST; the response is the grant; deferring = not responding yet. That turns the trickiest part of the protocol (the deferred-reply queue) into an idiomatic Go structure: a slice of channels closed on `Unlock`.

Correctness hangs on two details, both enforced under a single mutex in `ramutex`:

- the request timestamp is fixed **before** any REQUEST leaves and never changes mid-request, so every pairwise conflict compares identical pairs on both sides;
- `Observe` runs before the grant/defer decision, so if node A saw B's request before requesting itself, A's timestamp is strictly greater — both sides agree who came first.

`Incr(key)` makes the lock load-bearing. It is a read-modify-write — exactly the operation the KV API cannot do atomically (there is no CAS) — and it works on *any* node:

```
1.  node-X receives Incr           (any node, no leader hint needed)
2.  ramutex.Lock()                 REQUEST to all peers, wait for all grants
3.  resolve the Raft leader
      X is leader:   raft.Barrier() → store.Get → propose Put
      X is follower: forward Incr{unsafe:true} to the leader over peer gRPC
4.  ramutex.Unlock()               release deferred grants
```

Step 3's `Barrier` closes a subtle window: a freshly elected leader has *committed* the previous CS holder's write but may not have *applied* it yet; reading before the barrier could lose an update despite the lock. The forwarded call sets `unsafe=true` because the CS is already held at the origin — taking it again on the leader would deadlock waiting for a grant the origin can't give while inside the CS.

The `unsafe` flag is also client-reachable on purpose: `bench-incr -unsafe` is the control arm of the lost-update experiment (concurrent unlocked increments race and drop updates; locked runs land exactly on `baseline + N`).

**The availability contrast with Raft is the lesson of this section.** Raft makes progress with any majority (1 of 3 nodes down is fine). Ricart-Agrawala requires a grant from *every* peer — one dead node blocks all lock acquisitions until its timeout. Quorum-based mutual exclusion (Maekawa) or lease-based locks trade that off differently; here the fragility is kept visible because the contrast is instructive.

## 12. Process layout

A single binary (`cmd/main`) runs per node. Inside the binary:

```
main goroutine
  └─ node.Run
       ├─ goroutine: gRPC server (server.Server.Serve) — DB + Mutex services,
       │             one goroutine per in-flight RPC (deferred grants park here)
       └─ goroutine: leader-change logger (polls raft.State / raft.LeaderWithID every 500ms)

hashicorp/raft (internal, library-owned)
  ├─ FSM dispatch goroutine
  ├─ leader loop (when leader)
  ├─ follower loop (when follower)
  ├─ candidate loop (during elections)
  ├─ snapshot loop
  └─ TCP transport accept + per-peer goroutines
```

Shutdown is straightforward: on `SIGINT`/`SIGTERM`, `cmd/main` cancels the root context. `node.Run` calls `server.GracefulStop` (draining in-flight gRPC), then `raft.Raft.Shutdown` (closing the transport, flushing BoltDB, releasing locks). The transport's TCP listener and the gRPC listener are both closed by these calls.

## 13. Choices worth pushing on

If you're reading this with a reviewer hat on, these are the load-bearing decisions:

- **hashicorp/raft over etcd-io/raft.** Cleaner FSM abstraction; etcd-io/raft is "you drive the state machine, you ship the messages, you persist the log" — more flexible but blurs the library boundary. For learning architecture rather than the protocol internals, hashicorp/raft surfaces decisions at the API.
- **Two-port over single-port gRPC transport.** Avoids a third-party dep and decouples the two protocols. Cost: two listen ports per node, slightly more docker-compose noise.
- **Local reads, not linearizable.** ReadIndex via `VerifyLeader` is a half-day of work and the right next step if linearizability matters. Today, reads are fast but stale.
- **JSON commands.** Trades ~20% size for simplicity. Replacing with protobuf is purely mechanical.
- **Static membership, single-node bootstrap.** Rules out the membership-management complexity (`AddVoter` / `RemoveVoter` / config-change quorum subtleties) entirely. Realistic for the project's "stable VMs with fixed IPs" assumption.
- **Ricart-Agrawala over a Raft-backed lock service.** A lock FSM on top of Raft would be more available (majority quorum vs all-peers) and is how production systems do it (Chubby, etcd locks). RA was chosen because it is a *distinct* algorithm with its own message exchange — and because it composes with the Lamport clock instead of leaving it decorative.
- **Blocking RPC as REQUEST/REPLY.** Holding the gRPC response open encodes the deferred reply naturally. Cost: one parked goroutine + HTTP/2 stream per deferred grant — irrelevant at N=3, a real consideration at large N or with slow CS holders.

## 14. References

- [In Search of an Understandable Consensus Algorithm (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf) — the paper. §5 (basic Raft), §7 (log compaction), §8 (clients / linearizability), §9 (membership changes).
- [Time, Clocks, and the Ordering of Events in a Distributed System (Lamport, CACM 1978)](https://lamport.azurewebsites.net/pubs/time-clocks.pdf) — the logical-clock rules implemented in `internal/lamport`, and the happens-before relation the mutex depends on.
- [An Optimal Algorithm for Mutual Exclusion in Computer Networks (Ricart & Agrawala, CACM 1981)](https://dl.acm.org/doi/10.1145/358527.358537) — the algorithm implemented in `internal/ramutex`, including the 2·(N−1) message-count optimality argument.
- [`hashicorp/raft` docs](https://pkg.go.dev/github.com/hashicorp/raft) — Go API reference. The `raft.Config`, `raft.FSM`, `raft.LogStore`, and `raft.SnapshotStore` interfaces are what shape this codebase.
- [`hashicorp/raft-boltdb/v2`](https://pkg.go.dev/github.com/hashicorp/raft-boltdb/v2) — the BoltDB-backed `LogStore` + `StableStore` used here.
- Consul / Nomad source — production reference implementations that use hashicorp/raft. Look at how they handle membership and snapshots if you want to see the full operational shape.
