# raft-replicated-db

Simulation of database replication with Raft for leader election and consensus.

Pure peer-to-peer topology: each node boots with a static list of every peer
in the cluster (the `PEERS` env var). Nodes assume stable VMs with fixed IPs;
membership doesn't change at runtime.

## Layout

```
proto/
  db/v1/db.proto          # client-facing key/value API
  raft/v1/raft.proto      # internal node-to-node consensus RPCs
internal/
  pb/                     # generated Go bindings (run `make proto`)
  server/                 # gRPC server hosting both services
  node/                   # holds the peer list and ties everything together
cmd/main/main.go          # entrypoint
```

## Build & run

Prerequisites: Go 1.26+, [buf](https://buf.build) for proto generation, Docker.

```sh
make proto    # generate internal/pb/** from .proto
make tidy     # populate go.sum
make build    # binary at bin/node
docker compose up --build
```

Env vars consumed by `cmd/main`:

| var          | default                  | meaning                                              |
|--------------|--------------------------|------------------------------------------------------|
| `NODE_ID`    | `node-1`                 | unique id for this node, must appear in `PEERS`      |
| `NODE_PORT`  | `5000`                   | gRPC listen port                                      |
| `PEERS`      | `node-1=localhost:5000`  | full cluster membership: `id1=addr1,id2=addr2,...`   |

## Status

Scaffolding only. The Raft state machine in `internal/server/raft.go` is a
stub: `RequestVote`, `AppendEntries`, and `InstallSnapshot` accept the request
shape but always reject. Same for `Put` / `Delete` on the leader — they write
directly to the in-memory map instead of going through a proposal pipeline.
