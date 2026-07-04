# raft-replicated-db

A 3-node, replicated in-memory KV store built for MC714 (Distributed Systems, Unicamp). It implements the three algorithms the assignment asks for, all over real message passing (gRPC + Raft's TCP transport):

| Algorithm | Where | Role in the system |
|---|---|---|
| **Consensus / leader election** — Raft | [`hashicorp/raft`](https://github.com/hashicorp/raft) wired in `internal/raftnode` | replicates every write through a quorum-committed log |
| **Lamport logical clocks** (Lamport 1978) | `internal/lamport`, threaded through every message | timestamps client RPCs, replicated commands, and mutex messages |
| **Mutual exclusion** — Ricart-Agrawala (1981) | `internal/ramutex` + peer gRPC `Mutex` service | cluster-wide critical section powering the atomic `Incr` |

Clients speak gRPC; cluster consensus traffic uses Raft's native TCP transport on a separate port. See **`architecture.md`** (or [`docs/architecture.html`](docs/architecture.html)) for the design walkthrough and **`docs/relatorio.md`** for the lab report (pt-BR).

## Topology

| Port | Protocol           | Carries                                                              |
|------|--------------------|-----------------------------------------------------------------------|
| 5000 | gRPC (HTTP/2)      | client DB API (`Get`, `Put`, `Delete`, `Incr`) + peer `Mutex` service |
| 7000 | hashicorp/raft TCP | `RequestVote`, `AppendEntries`, `InstallSnapshot` between peers        |

No external coordinator. Membership is a static `PEERS` env var; the first node bootstraps the cluster on its first boot via `raft.BootstrapCluster`.

## Layout

```
proto/db/v1/db.proto      # client-facing key/value API
proto/mutex/v1/mutex.proto# peer-facing Ricart-Agrawala transport
internal/
  pb/                     # generated gRPC bindings (run `make proto`)
  lamport/                # Lamport logical clock (Tick / Observe)
  ramutex/                # Ricart-Agrawala algorithm core (transport-agnostic)
  store/                  # in-memory KV state machine
  fsm/                    # raft.FSM: decodes Commands, mutates the store
  raftnode/               # hashicorp/raft wiring (BoltDB stores, TCP transport, bootstrap)
  server/                 # gRPC services: DB (clients), Mutex (peers), peer conns
  node/                   # composition root
cmd/main/main.go          # node entrypoint
cmd/client/main.go        # CLI client (get/put/del/incr/bench-incr)
architecture.md           # design walkthrough (read this)
docs/architecture.html    # HTML version with diagrams
docs/relatorio.md         # lab report (pt-BR)
```

## Build & run

Prerequisites: Go 1.26+, [buf](https://buf.build) for proto generation, Docker.

```sh
make proto    # generate internal/pb/** from .proto
make build    # binaries at bin/node and bin/client
make test     # unit tests with the race detector
docker compose up --build
```

The compose file brings up `node-1`, `node-2`, `node-3` with per-node named volumes for their BoltDB files. Client ports are mapped to the host at `127.0.0.1:5001`, `:5002`, `:5003`.

Once running, the cluster bootstraps on `node-1` (it has `BOOTSTRAP=true`); `node-2` and `node-3` receive the configuration via AppendEntries and join automatically. You'll see `raft state: Leader` on one node and `raft state: Follower` on the others in the logs.

## Talking to the cluster

```sh
bin/client put greeting hello        # write (redirects to the leader if needed)
bin/client -node node-3 get greeting # read from a specific replica
bin/client del greeting
bin/client incr counter              # atomic increment under Ricart-Agrawala
bin/client incr-unsafe counter       # increment WITHOUT the lock
```

The client defaults to `-nodes node-1=127.0.0.1:5001,node-2=127.0.0.1:5002,node-3=127.0.0.1:5003`, keeps its own Lamport clock (printed on every response), and follows `LeaderHint` redirects automatically.

### The mutual-exclusion experiment

`Incr` is a read-modify-write: read the value, add, write back. Concurrent unlocked increments to the same key race and lose updates; under Ricart-Agrawala they serialize cluster-wide:

```sh
bin/client put counter 0
bin/client bench-incr -n 30 -c 3 -unsafe counter   # final value < 30: lost updates
bin/client put counter 0
bin/client bench-incr -n 30 -c 3 counter           # final value == 30, every time
```

Watch `docker compose logs -f` while it runs — nodes log every REQUEST/grant/defer decision with the `(timestamp, id)` pairs that decide priority.

### The leader-election experiment

```sh
docker compose logs -f | grep -E 'raft state|leader'   # watch roles
docker compose stop node-1                             # kill the current leader
bin/client put k v                                     # still works: new leader elected
docker compose start node-1                            # rejoins as follower, catches up
```

## Env vars

| var          | default                    | meaning                                                        |
|--------------|----------------------------|----------------------------------------------------------------|
| `NODE_ID`    | `node-1`                   | unique id; must appear in `PEERS`                              |
| `NODE_PORT`  | `5000`                     | gRPC listen port (client DB API + peer mutex service)          |
| `DATA_DIR`   | `/var/lib/raft-db`         | BoltDB files + snapshot directory                              |
| `PEERS`      | `node-1=node-1:7000`       | full cluster membership (Raft transport addresses)             |
| `GRPC_PEERS` | derived: raft host + gRPC port | full cluster membership (gRPC addresses); set explicitly when nodes share a host with distinct ports |
| `BOOTSTRAP`  | `false`                    | set to `true` on exactly one node, only its first boot         |

## Status

Working end-to-end: quorum-replicated writes, leader failover, Lamport-timestamped messages on every channel, and a Ricart-Agrawala critical section demonstrably preventing lost updates. Not implemented (deliberately, see `architecture.md` §11): linearizable reads, dynamic membership, mTLS.
