# raft-replicated-db

A 3-node, replicated in-memory KV store backed by [`hashicorp/raft`](https://github.com/hashicorp/raft) with BoltDB-backed log and snapshot storage. Clients speak gRPC; cluster traffic uses Raft's native TCP transport on a separate port.

The project is a learning vehicle &mdash; the goal is to study how a Raft-backed system is wired together (FSM contract, leader redirect, persistence layout, failure modes), not to re-implement consensus. See **`architecture.md`** (or [`docs/architecture.html`](docs/architecture.html)) for the design walkthrough.

## Topology

| Port | Protocol           | Carries                                                         |
|------|--------------------|-----------------------------------------------------------------|
| 5000 | gRPC (HTTP/2)      | client DB API (`Get`, `Put`, `Delete`)                          |
| 7000 | hashicorp/raft TCP | `RequestVote`, `AppendEntries`, `InstallSnapshot` between peers |

No external coordinator. Membership is a static `PEERS` env var; the first node bootstraps the cluster on its first boot via `raft.BootstrapCluster`.

## Layout

```
proto/db/v1/db.proto      # client-facing key/value API
internal/
  pb/db/v1/               # generated gRPC bindings (run `make proto`)
  store/                  # in-memory KV state machine
  fsm/                    # raft.FSM: decodes Commands, mutates the store
  raftnode/               # hashicorp/raft wiring (BoltDB stores, TCP transport, bootstrap)
  server/                 # client gRPC server that proposes writes via raft.Apply
  node/                   # composition root
cmd/main/main.go          # entrypoint
architecture.md           # design walkthrough (read this)
docs/architecture.html    # HTML version with diagrams
```

## Build & run

Prerequisites: Go 1.26+, [buf](https://buf.build) for proto generation, Docker.

```sh
make proto    # generate internal/pb/** from .proto
make tidy     # populate go.sum
make build    # binary at bin/node
docker compose up --build
```

The compose file brings up `node-1`, `node-2`, `node-3` with per-node named volumes for their BoltDB files. Client ports are mapped to the host at `127.0.0.1:5001`, `:5002`, `:5003`.

Once running, the cluster bootstraps on `node-1` (it has `BOOTSTRAP=true`); `node-2` and `node-3` receive the configuration via AppendEntries and join automatically. You'll see `raft state: Leader` on one node and `raft state: Follower` on the others in the logs.

## Env vars

| var          | default                                                              | meaning                                                  |
|--------------|----------------------------------------------------------------------|----------------------------------------------------------|
| `NODE_ID`    | `node-1`                                                             | unique id; must appear in `PEERS`                        |
| `NODE_PORT`  | `5000`                                                               | gRPC client listen port                                  |
| `DATA_DIR`   | `/var/lib/raft-db`                                                   | BoltDB files + snapshot directory                        |
| `PEERS`      | `node-1=node-1:7000`                                                 | full cluster membership (Raft transport addresses)       |
| `BOOTSTRAP`  | `false`                                                              | set to `true` on exactly one node, only its first boot   |

## Talking to the cluster

The DB API is plain gRPC (`proto/db/v1/db.proto`). A `Put` against a follower returns `LeaderHint` so the client can retry against the leader. A `Get` is served from the local replica (potentially stale &mdash; see the linearizability note in `architecture.md`).

## Status

Working end-to-end via hashicorp/raft. No linearizable reads, no dynamic membership, no mTLS. Each of those is called out in `architecture.md` &sect;5, &sect;7, &sect;11 with the place it would land.
