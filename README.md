# KVRaft — a fault-tolerant key-value store on Raft

[![CI](https://github.com/atharv-pdarshi/kvraft/actions/workflows/ci.yml/badge.svg)](https://github.com/atharv-pdarshi/kvraft/actions/workflows/ci.yml)

A 5-node, fault-tolerant key-value store built on the [Raft consensus
algorithm](https://raft.github.io/raft.pdf): replicated logs, leader election and
majority-based commitment, written from scratch in Go. `go test ./...` runs a
5-node cluster in memory and puts it through crashes, restarts and partitions;
`docker compose up` runs the same five nodes as containers that clients reach
over gRPC.

The Raft core knows nothing about keys and values — its log carries opaque
command bytes — so it is tested entirely on its own, and the key-value store is
a thin state machine on top.

## What it does

- **Leader election** via RequestVote/AppendEntries RPCs, randomized election
  timeouts and heartbeats.
- **Log replication** with fast backtracking (a follower a thousand entries
  behind, holding conflicting entries, catches up in well under a second).
- **Majority commitment** with Raft's current-term rule and a no-op entry on
  election, so the Figure 8 anomaly cannot un-commit an entry.
- **Crash recovery**: term, vote and log are fsynced to disk (atomic state file,
  CRC'd append-only log with torn-write recovery), so a restarted node comes
  back as itself and catches up.
- **Exactly-once writes**: client id + seq dedup means a retried Put is applied
  once. Reads go through the log, so they are linearizable.
- **Tolerates 2 failures**: with 5 nodes the majority is 3, so any 2 can crash,
  restart or be partitioned away and every acknowledged write survives.

## Architecture

```
  one node
  +-------------------------------------------------------------+
  |  gRPC KV service -> kv.Server --Start(cmd)--> raft.Node      |
  |                       ^                          |           |
  |                       | ApplyCh (committed)      | RequestVote/
  |                  kv.Store (map)                  | AppendEntries
  |                                                  v           |
  |            storage.File (state + CRC log, fsync'd)           |
  +-------------------------------------------------------------+
                          |  gRPC Raft service (peer RPCs)
       node1 <-----------+-----------> node2 ... node5
```

- `raft/` — the consensus library (Figure 2, implemented box by box). Opaque
  command bytes only; no keys/values.
- `kv/` — the key-value state machine and the Raft-backed server (dedup,
  linearizable reads, index→waiter map).
- `transport/` — the `raft.Transport` interface with two implementations: an
  in-memory network (cut/delay/drop/partition) for tests, and gRPC for real
  clusters.
- `storage/` — the `raft.Storage` interface with in-memory and file backends.
- `client/` — the client library (leader hints, retries, history recorder).
- `tests/` — the in-memory cluster harness and every cluster-level test.
- `cmd/` — `kvnode`, `kvctl`, `loadgen`.

## Run the tests (the in-memory proof)

```bash
make test        # go test ./...
make test-race   # go test -race ./...   ← the real proof
```

The harness spins up five Raft nodes on one in-memory network and, throughout
every test, checks that no two nodes ever apply different commands at the same
index.

## Run five real nodes

### With Docker

```bash
cd deploy && docker compose up --build -d          # five containers
kvctl -peers node1=localhost:8001,node2=localhost:8002,node3=localhost:8003,node4=localhost:8004,node5=localhost:8005 status
kvctl -peers ...:8001,...  put foo bar
kvctl -peers ...:8001,...  get foo
docker compose stop node2 node3                    # writes still succeed (3 of 5)
```

### Without Docker (five local processes)

```bash
make build
./scripts/demo.sh    # 5 nodes, write 100, kill leader + one more, write 100, restart, read 200
```

## Fault-tolerance results

Every scenario runs a 5-node cluster with clients writing throughout, then checks
election safety, log matching, state-machine convergence and durability (every
acknowledged write is readable at the end). All under the race detector.

| Scenario | What it does | Runs |
| --- | --- | --- |
| `TestLeaderCrash` | crash the leader | 10/10 |
| `TestRepeatedLeaderCrash` | crash each new leader, 5× | 10/10 |
| `TestTwoFollowersDown` | crash 2 followers | 10/10 |
| `TestLeaderAndFollowerDown` | crash the leader and a follower | 10/10 |
| `TestThreeDown` | crash 3, then restore a majority | 10/10 |
| `TestPartitionLeaderInMinority` | split leader into a 2-node side | 10/10 |
| `TestPartitionLeaderInMajority` | split leader into a 3-node side | 10/10 |
| `TestFlakyNetwork` | 20% drop + delay, 4s | 10/10 |
| `TestChaos` | random crashes/restarts/partitions, ≤2 down | 10/10 |

Core Raft tests (election, replication, backtracking, commit, persistence,
key-value) each pass 20/20 under `-race`. Run the real-cluster version with
`scripts/chaos.sh` against `docker compose` plus `cmd/loadgen`.

## Design choices

- **Fixed 5-node membership.** Majority stays 3 and "2 failures" is well
  defined. Dynamic membership changes are deliberately out of scope.
- **Reads through the log.** Simple and linearizable. (ReadIndex would be a
  faster alternative; noted as future work.)
- **Exactly-once writes** via per-client `seq` dedup, so client retries after a
  lost reply don't double-apply.
- **One port per node** serves both the Raft and KV gRPC services.

## Known limits

- No snapshots / log compaction: the log grows without bound. (The storage
  interface leaves room for an `InstallSnapshot` RPC.)
- No dynamic membership changes.
- No sharding — a single Raft group.

## Claim map

| Claim | Built in | Proven by |
| --- | --- | --- |
| 5-node | Stage 8 | compose cluster, in-memory harness |
| Fault-tolerant KV store | Stage 7 | `TestKVLeaderChange`, `TestKVConcurrentClients` |
| Raft consensus, replicated logs | Stages 3–4 | `TestBasicAgree`, log-matching invariant |
| Majority-based commitment | Stage 5 | `TestCommitWithTwoDown`, `TestNoCommitWithThreeDown`, `TestFigure8` |
| Leader election, timeouts | Stage 3 | `TestInitialElection`, `TestReElection`, `TestManyElections` |
| Log replication via AppendEntries | Stage 4 | `TestFollowerCatchUp` |
| Backtracking | Stage 4 | `TestConflictingLogs`, `TestBacktrackingSpeed` |
| Leader-crash recovery | Stage 9 | `TestLeaderCrash`, `TestRepeatedLeaderCrash` |
| Restarts | Stages 6, 9 | `TestRestartFollower`, `TestRestartAll` |
| Partitions | Stages 2, 9 | `TestPartitionLeaderInMinority/Majority` |
| Tolerating 2 failures | Stages 5, 9 | `TestTwoFollowersDown`, `TestChaos` |
| Preserving committed state | Stages 6, 7, 9 | durability invariant in every scenario |

## Layout

```
cmd/{kvnode,kvctl,loadgen}   binaries
proto/{kv,raft}.proto        gRPC/protobuf definitions
raft/                        consensus library
kv/                          state machine + Raft-backed server
transport/                   in-memory + gRPC transports
storage/                     in-memory + file storage
client/                      client library
tests/                       cluster harness + all tests
deploy/docker-compose.yml    5-node cluster
scripts/{demo,chaos}.sh      demo and fault injection
```

Built from the plan in [`docs/PLAN.md`](docs/PLAN.md).
