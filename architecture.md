# Architecture

A replicated in-memory KV store backed by [`hashicorp/raft`](https://github.com/hashicorp/raft). This document covers component layout, data flow, persistence, failure modes, and the architectural decisions worth pushing on. An HTML companion with the same content lives at `docs/architecture.html`.

## 1. Scope

- **What it is:** a 3-node, static-membership KV store. Clients speak gRPC. Writes go through Raft; reads are local.
- **What it isn't:** sharded, dynamically-membership'd, multi-region, secure-on-the-wire, observability-rich. Each of these is a deliberate non-goal for the learning version; the doc flags where each would land.

## 2. Topology

Three nodes, identical binary, two listen ports each. No external coordinator (etcd / Consul / ZK). Membership comes from the `PEERS` env var baked at boot.

```
                  ┌───────────────────────┐         ┌───────────────────────┐
   client ─gRPC─▶ │ node-1                │ ─raft─▶ │ node-2                │
                  │  :5000  DBService     │         │  :5000  DBService     │
                  │  :7000  raft TCP      │ ◀─raft─ │  :7000  raft TCP      │
                  │  /var/lib/raft-db     │         │  /var/lib/raft-db     │
                  └─────────┬─────────────┘         └───────────┬───────────┘
                            │ raft (AppendEntries, RequestVote, InstallSnapshot)
                            ▼
                  ┌───────────────────────┐
                  │ node-3                │
                  │  :5000  DBService     │
                  │  :7000  raft TCP      │
                  │  /var/lib/raft-db     │
                  └───────────────────────┘
```

Two ports per node, **separate transports**:

| Port | Protocol            | Carries                                                         |
|------|---------------------|-----------------------------------------------------------------|
| 5000 | gRPC (HTTP/2)       | client DB API (`Get`, `Put`, `Delete`)                          |
| 7000 | hashicorp/raft TCP  | `RequestVote`, `AppendEntries`, `InstallSnapshot` between peers |

The single-port-via-gRPC alternative ([`Jille/raft-grpc-transport`](https://github.com/Jille/raft-grpc-transport)) was considered and rejected: it adds a third-party dep and re-encodes hashicorp/raft's internal messages without changing semantics. Two ports is one extra line of docker-compose and zero extra deps.

## 3. Component map

```
cmd/main/                  process entrypoint; env-var parsing
internal/
  node/                    composition root: builds raftnode + server, drives lifecycle
  raftnode/                hashicorp/raft wiring: stores, transport, bootstrap, log output
  fsm/                     raft.FSM: decodes Commands, mutates the store, snapshot/restore
  store/                   in-memory map[string][]byte with RWMutex; Snapshot/Restore as JSON
  server/                  client gRPC server; DB service that routes writes through raft.Apply
  pb/db/v1/                generated protobuf bindings
proto/db/v1/db.proto       client API schema
```

Dependency direction (anything left depends on what's to its right):

```
cmd ─▶ node ─▶ {server, raftnode}
         └─▶ raftnode ─▶ {fsm, store, hashicorp/raft, raft-boltdb}
         └─▶ server   ─▶ {hashicorp/raft, fsm, store}
```

`server` and `raftnode` are siblings — they don't know about each other. `node` is the only package that wires them together. This is what lets you swap the gRPC layer (REST, JSON-RPC, anything) without touching the consensus layer, and vice versa.

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

The "single node volume nuked" case is the one most people get wrong on a first build: don't try to re-bootstrap. Just bring the node back with the same `NODE_ID` and empty `DATA_DIR`; the leader will repair it.

## 9. State machine semantics

The FSM is intentionally trivial — a `map[string][]byte`. The interesting properties come from how it's driven:

- **Deterministic.** `Apply` only reads the entry; no clocks, no randomness, no I/O. Two replicas applying the same log starting from the same snapshot will reach byte-identical states.
- **Idempotent at the command level.** A `Put k=v` applied twice is still `k=v`; a `Delete k` after a previous `Delete k` is a no-op. The Raft log itself never duplicates entries (each commits at a unique index), but command-level idempotence matters for client retries after a leader-hint redirect.
- **No transactions, no compare-and-swap.** The wire format (`Command`) has only `put` and `delete`. Adding `cas { key, expected, new }` is a one-field extension to `Command` and ~10 lines in `FSM.Apply`. The architecture supports it; the schema doesn't yet.

The choice of JSON for the `Command` encoding is a KISS call: legible in logs, no codegen, easy to extend. Switching to protobuf is mechanical and would buy ~20% smaller log entries.

## 10. Process layout

A single binary (`cmd/main`) runs per node. Inside the binary:

```
main goroutine
  └─ node.Run
       ├─ goroutine: gRPC server (server.Server.Serve)
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

## 11. Choices worth pushing on

If you're reading this with a reviewer hat on, these are the load-bearing decisions:

- **hashicorp/raft over etcd-io/raft.** Cleaner FSM abstraction; etcd-io/raft is "you drive the state machine, you ship the messages, you persist the log" — more flexible but blurs the library boundary. For learning architecture rather than the protocol internals, hashicorp/raft surfaces decisions at the API.
- **Two-port over single-port gRPC transport.** Avoids a third-party dep and decouples the two protocols. Cost: two listen ports per node, slightly more docker-compose noise.
- **Local reads, not linearizable.** ReadIndex via `VerifyLeader` is a half-day of work and the right next step if linearizability matters. Today, reads are fast but stale.
- **JSON commands.** Trades ~20% size for simplicity. Replacing with protobuf is purely mechanical.
- **Static membership, single-node bootstrap.** Rules out the membership-management complexity (`AddVoter` / `RemoveVoter` / config-change quorum subtleties) entirely. Realistic for the project's "stable VMs with fixed IPs" assumption.

## 12. References

- [In Search of an Understandable Consensus Algorithm (Ongaro & Ousterhout, 2014)](https://raft.github.io/raft.pdf) — the paper. §5 (basic Raft), §7 (log compaction), §8 (clients / linearizability), §9 (membership changes).
- [`hashicorp/raft` docs](https://pkg.go.dev/github.com/hashicorp/raft) — Go API reference. The `raft.Config`, `raft.FSM`, `raft.LogStore`, and `raft.SnapshotStore` interfaces are what shape this codebase.
- [`hashicorp/raft-boltdb/v2`](https://pkg.go.dev/github.com/hashicorp/raft-boltdb/v2) — the BoltDB-backed `LogStore` + `StableStore` used here.
- Consul / Nomad source — production reference implementations that use hashicorp/raft. Look at how they handle membership and snapshots if you want to see the full operational shape.
