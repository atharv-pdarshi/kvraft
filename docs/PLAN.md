# KVRaft — Build-from-Scratch Plan

Nov 19, 2026 · @Apurva Priyadarshi

## Goal

Build KVRaft again from an empty folder, in 11 stages, until every word of your project description is true and backed by a test. Each stage leaves you with something that runs and is tested, so you're never stuck with half a system.

**Target description:**

> Built a 5-node fault-tolerant key-value store using Raft consensus, replicated logs, and majority-based commitment. Programmed leader election and log replication via RequestVote/AppendEntries RPCs, timeouts, and backtracking. Validated fault recovery across leader crashes, restarts, partitions, tolerating 2 failures preserving committed state.

**What you'll have at the end:** a Go repository where `go test ./...` runs a 5-node Raft cluster in memory and puts it through crashes, restarts and partitions, and `docker compose up` runs the same 5 nodes as real containers that clients reach over gRPC.

**Time:** about 3–4 weeks at a few focused hours a day. Stages 3–6 (the Raft core) take about half of it.

**How to use this plan**

- Work strictly in order. Each stage depends on the ones before it.
- Don't move on until the stage's **Done when** check passes, including under `go test -race`.
- Commit at the end of every stage with a message like `stage 3: leader election`. Your commit history then shows how you built it.
- Keep Figure 2 of the [Raft paper](https://raft.github.io/raft.pdf) open the whole time. Stages 3–6 are essentially "implement Figure 2, one box at a time".
- You can reuse ideas and the `.proto` files from the existing repo, but write the Raft code fresh. The [KVRaft project report](https://claude.ai/artifact/PoXbXGBqihYWLXAyT24Aba), section 13, lists the mistakes to avoid.

## At a glance

&#91;embedded content: build roadmap · 11 stages, about 3–4 weeks\]

Stages 0–2 are foundations, 3–6 are the Raft core, and 7–10 turn it into a product and prove it. Each stage ends with a "Done when" check; don't start the next one until it passes.

## Stage 0: Prerequisites (2–3 days)

**Goal:** know the tools well enough that the Raft stages are about Raft, not about fighting Go.

### Go concurrency (the source of most Raft bugs)

- [ ] Goroutines, `sync.Mutex`, `sync.WaitGroup`, `sync.Cond`.
- [ ] Channels: buffered vs unbuffered, `select`, closing a channel safely.
- [ ] `context.Context` with timeouts and cancellation.
- [ ] `time.Timer` and `time.Ticker`, including how to reset a timer.
- [ ] Run a deliberately racy program with `go run -race` and read the report.
- [ ] Watch MIT 6.824 Lecture 5 ("Go, Threads, and Raft"). It shows the exact locking patterns Raft needs.

Three rules to remember: never hold a lock while making a network call; Go mutexes aren't re-entrant, so don't lock twice; always unlock with `defer` so a panic or early return can't leave a lock held.

### gRPC and Protocol Buffers

- [ ] Install `protoc`, `protoc-gen-go` and `protoc-gen-go-grpc`.
- [ ] Follow the official [gRPC Go basics tutorial](https://grpc.io/docs/tutorials/basic/go/): write a `.proto`, generate code, write a server and a client.

### Raft itself

- [ ] Read the [Raft paper](https://raft.github.io/raft.pdf), sections 5 and 8, twice. Learn Figure 2 well enough to redraw it from memory.
- [ ] Read the [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/) once now, and again during Stage 4.
- [ ] Rewatch Kleppmann 6.2 if anything in Figure 2 is unclear.

**Done when:** you can draw Figure 2 from memory, and you've built and run a small gRPC server and client.

## Stage 1: Project setup and a single-node key-value server (1–2 days)

**Goal:** a working key-value server on one machine, reachable over gRPC. No Raft yet. This gives you the client API and project layout that everything else plugs into.

### Project layout

```
kvraft/
├── cmd/
│   ├── kvnode/main.go      start one node
│   └── kvctl/main.go       command-line client
├── proto/
│   ├── raft.proto          RequestVote, AppendEntries (Stage 8)
│   └── kv.proto            Put, Get, Delete
├── raft/                   the consensus library (Stages 2–6)
├── kv/                     the state machine and server (Stage 7)
├── transport/              in-memory and gRPC networks
├── storage/                files on disk (Stage 6)
├── client/                 client library
├── tests/                  cluster and fault tests
├── deploy/docker-compose.yml
└── README.md
```

Keep `raft/` free of anything about keys and values. It should only know about "commands" as bytes, so it can be tested on its own.

### Tasks

- [ ] `git init`, `go mod init github.com/<you>/kvraft`, add a `.gitignore` for `data/`.
- [ ] Write `kv.proto`: `Put(key, value)`, `Get(key)`, `Delete(key)`. Add `client_id` and `seq` fields to write requests now (used in Stage 7), and `leader_hint` to every response.
- [ ] Generate the Go code with a `Makefile` target (`make proto`).
- [ ] Write a `kv.Store` with a map and a mutex, plus `Apply(cmd)` that runs one command. Make commands a protobuf message (`Command{op, key, value}`), not a space-separated string.
- [ ] Write a gRPC server that calls the store directly, and `kvctl put|get|delete` to call it.
- [ ] Unit tests for `kv.Store`.

**Done when:** `kvnode` starts, and `kvctl put foo bar` then `kvctl get foo` prints `bar`.

## Stage 2: Raft skeleton and an in-memory test network (2 days)

**Goal:** the shape of a Raft node, plus a fake network that can run 5 nodes inside one test and cut, delay or drop messages between them. Build the test tools *before* the algorithm: every later stage, and every claim about crashes and partitions, is proven with them.

### The Raft node

- [ ] `raft.Config{ID, Peers []string, ElectionTimeoutMin/Max, HeartbeatInterval}`. In tests use short timeouts (about 150–300 ms elections, 50 ms heartbeats).
- [ ] `raft.Node` struct with every field from Figure 2's "State" box: `currentTerm`, `votedFor`, `log`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`, plus `role` (Follower, Candidate or Leader) and one mutex.
- [ ] Start the log with a dummy entry at index 0 (term 0), so `prevLogIndex = 0` always has an entry to compare.
- [ ] Public API: `Start(command []byte) (index, term, isLeader)`, `State() (term, isLeader)`, `ApplyCh <-chan ApplyMsg`, `Stop()`.
- [ ] Empty handlers for `RequestVote` and `AppendEntries`, using Go structs that match Figure 2 field for field.

### The transport

- [ ] A `Transport` interface: `RequestVote(ctx, to, args)` and `AppendEntries(ctx, to, args)`.
- [ ] An in-memory `Network` that implements it: it looks up the target node and calls its handler directly.
- [ ] Controls on the network: `Disconnect(id)`, `Connect(id)`, `Partition([]ids, []ids)`, `Heal()`, `SetDropRate(0.2)`, `SetDelay(min, max)`.
- [ ] A disconnected or stopped node's calls return an error after the timeout, just like a real dead server.

### The test harness

- [ ] `tests/cluster.go`: `NewCluster(t, n)`, `Crash(i)`, `Restart(i)`, `Leader()`, `CheckOneLeader()`, `Submit(cmd)`, `WaitCommitted(index, count)`.
- [ ] A checker that reads every node's `ApplyCh` and fails the test if two nodes ever apply different commands at the same index.

**Done when:** `NewCluster(t, 5)` starts and stops 5 idle nodes with `-race` clean, and a test can disconnect and reconnect a node.

## Stage 3: Leader election (2–3 days)

**Goal:** a 5-node cluster always has at most one leader per term, picks a new one when the leader fails, and doesn't elect anyone without a majority.

### Tasks

- [ ] **Election timer:** one goroutine per node. Pick a random timeout between min and max; reset it whenever you get a valid AppendEntries from the current leader or **grant** a vote. Don't reset it when you refuse a vote.
- [ ] **Become candidate:** `currentTerm++`, vote for yourself, reset the timer, send RequestVote to all peers **in parallel**, one goroutine each.
- [ ] **Count votes:** become leader as soon as votes > n/2 (3 of 5). Ignore replies from an older term. If any reply has a higher term, become follower in that term.
- [ ] **RequestVote handler**, exactly as in Figure 2:
  1. If `args.Term < currentTerm`, refuse.
  2. If `args.Term > currentTerm`, adopt it, become follower, clear `votedFor`.
  3. Grant if `votedFor` is empty or equals the candidate, **and** the candidate's log is at least as up to date (higher last term, or same last term and an index at least as large).
- [ ] **Heartbeats:** the leader sends empty AppendEntries to every peer every 50 ms (tests) or about 100 ms (real). Followers reset their timer when one arrives.
- [ ] **Step down rule:** in every handler and every reply, a higher term means "become follower in that term".
- [ ] Follow the lock rule: copy what you need under the lock, unlock, make the call, lock again, and check the term and role haven't changed before using the reply.

### Tests

- [ ] `TestInitialElection`: 5 nodes elect exactly one leader within 1 s; the term stays stable with no failures.
- [ ] `TestReElection`: disconnect the leader; a new one is elected; reconnect the old leader; it steps down.
- [ ] `TestNoQuorumNoLeader`: disconnect 3 of 5; the remaining 2 never elect a leader.
- [ ] `TestManyElections`: randomly disconnect and reconnect up to 2 nodes for 10 s; never two leaders in one term.

**Done when:** all four pass 100 times in a row with `-race`.

**Makes true:** "leader election via RequestVote/AppendEntries RPCs, timeouts".

## Stage 4: Log replication and backtracking (3–4 days)

**Goal:** every follower, however far behind or however wrong its log is, ends up with exactly the leader's log. This is the hardest stage; take your time.

### Leader side

- [ ] `Start(cmd)` on the leader appends `{term: currentTerm, cmd}` to its log and returns the new index straight away.
- [ ] When it becomes leader: set `nextIndex[p] = lastLogIndex + 1` and `matchIndex[p] = 0` for every peer.
- [ ] Each heartbeat (and right after `Start`), send each follower AppendEntries with `prevLogIndex = nextIndex[p] - 1`, its term, **all entries from `nextIndex[p]` to the end** (cap at about 100), and `leaderCommit`.
- [ ] On success: `matchIndex[p] = prevLogIndex + len(entries)`, `nextIndex[p] = matchIndex[p] + 1`. Use the values you *sent*, not the current log length, because the log may have grown meanwhile.
- [ ] On a log mismatch: **back up `nextIndex[p]`** and retry. Version 1: subtract 1. Version 2 (fast backtracking): use the follower's `conflictTerm`/`conflictIndex` to skip a whole term at once.

### Follower side (Figure 2, AppendEntries receiver)

- [ ] Refuse if `args.Term < currentTerm`.
- [ ] Refuse if there's no entry at `prevLogIndex`, or its term isn't `prevLogTerm`. With fast backtracking, also reply with:
  - log too short: `conflictIndex = len(log)`, `conflictTerm = -1`
  - term mismatch: `conflictTerm = log[prevLogIndex].term`, `conflictIndex` = first index of that term
- [ ] For each new entry: if an existing entry at that index has a different term, **delete it and everything after it**; then append any entries not already present.
- [ ] Never truncate when the entries already match. A delayed, old AppendEntries must not remove newer entries.
- [ ] `commitIndex = min(leaderCommit, index of last new entry)` if `leaderCommit > commitIndex`.

### Tests

- [ ] `TestBasicAgree`: submit 10 commands; all 5 nodes store them in the same order.
- [ ] `TestFollowerCatchUp`: disconnect a follower, submit 50 commands, reconnect; it receives all 50.
- [ ] `TestConflictingLogs`: an old leader in a minority partition collects uncommitted entries; after healing they're replaced by the new leader's entries.
- [ ] `TestBacktrackingSpeed`: a follower 1,000 entries behind with conflicts catches up within a few seconds (this checks fast backtracking).

**Done when:** all four pass 100 times in a row with `-race`.

**Makes true:** "replicated logs", "log replication via AppendEntries", "backtracking".

## Stage 5: Majority commit and applying entries (1–2 days)

**Goal:** an entry counts as committed exactly when a majority stores it (with Raft's current-term rule), and every node applies committed entries once, in order.

### Majority commit

- [ ] Whenever a `matchIndex` changes, the leader recomputes its commit index:

  ```go
  idx := []int{lastLogIndex}                       // the leader counts itself
  for _, p := range peers { idx = append(idx, matchIndex[p]) }
  sort.Sort(sort.Reverse(sort.IntSlice(idx)))
  N := idx[len(idx)/2]                             // 5 nodes: 3rd highest
  if N > commitIndex && log[N].Term == currentTerm {
      commitIndex = N
  }
  ```
- [ ] The `log[N].Term == currentTerm` check is essential. Without it, the Figure 8 scenario in the paper can make a "committed" entry disappear.
- [ ] **No-op on election:** a new leader appends an empty entry in its own term straight away. Committing it also commits every earlier entry, which is what makes old entries usable after a leader change or restart.

### Applying

- [ ] One applier goroutine per node. It waits on a `sync.Cond`; whenever `commitIndex > lastApplied`, it sends entries `lastApplied+1 … commitIndex` on `ApplyCh` **in order**, then updates `lastApplied`.
- [ ] Don't hold the Raft lock while sending on `ApplyCh`, or a slow reader can freeze the whole node.

### Tests

- [ ] `TestCommitWithTwoDown`: disconnect 2 of 5 followers; new commands still commit and get applied.
- [ ] `TestNoCommitWithThreeDown`: disconnect 3 of 5; the leader's new entries never commit. Reconnect them; the entries commit.
- [ ] `TestFigure8`: build the scenario from the paper's Figure 8 using disconnections; no applied entry ever changes. Rerun it with real crashes and restarts after Stage 6.
- [ ] `TestApplyOrder`: every node's `ApplyCh` delivers indexes 1, 2, 3, … with no gaps or repeats.

**Done when:** all four pass 100 times in a row with `-race`.

**Makes true:** "majority-based commitment", and the first half of "tolerating 2 failures".

## Stage 6: Persistence and restarts (2–3 days)

**Goal:** a node that crashes and restarts comes back as itself, with its term, vote and log intact, and catches up. This is the crash-recovery model from lecture 2.3.

### What to save, and why

| State | Saved when | If it were lost |
| --- | --- | --- |
| `currentTerm` | Term changes | The node could go back to an old term |
| `votedFor` | A vote is granted | The node could vote twice in one term, so two leaders could be elected |
| Log entries | Entries appended or truncated | A committed entry could vanish from the majority that held it |

`commitIndex`, `lastApplied`, `nextIndex` and `matchIndex` are **not** saved; they're rebuilt after restart.

### Tasks

- [ ] A `storage.Storage` interface: `SaveState(term, votedFor)`, `AppendEntries(entries)`, `TruncateFrom(index)`, `Load()`. Write a **memory** version (for tests, surviving `Crash`/`Restart` inside the harness) and a **file** version.
- [ ] File version: a small `state` file written atomically (write temp file, `fsync`, rename) and an append-only `log` file where each record has a length and a CRC32 checksum.
- [ ] `fsync` **before** replying to RequestVote or AppendEntries, and before the leader counts its own new entry towards the majority.
- [ ] On start, `Load()` the term, vote and log. A broken last record (torn write) is detected by its checksum and dropped.
- [ ] Use a stable node ID from config and a data directory per node (`data/node1/`, …).
- [ ] Make `Crash(i)` in the harness keep that node's storage and `Restart(i)` build a new node from it.
- [ ] Optional stretch: **snapshots**, i.e. save the state machine every N entries, discard older log entries, and add the `InstallSnapshot` RPC.

### Tests

- [ ] `TestPersistVote`: a node votes, crashes and restarts in the same term; it refuses a second candidate.
- [ ] `TestRestartFollower`: crash a follower, commit 50 entries, restart it; it catches up.
- [ ] `TestRestartLeader`: crash the leader just after it sends entries; no committed entry is lost.
- [ ] `TestRestartAll`: commit 100 entries, crash and restart all 5; all 100 are applied again on every node.
- [ ] `TestTornWrite`: cut the last log record in half on disk; the node still starts and catches up.
- [ ] Rerun `TestFigure8` with real crashes and restarts.

**Done when:** all pass 100 times in a row with `-race`.

**Makes true:** "restarts" and "preserving committed state".

## Stage 7: The key-value store on top of Raft (2–3 days)

**Goal:** connect the Stage 1 store to Raft, so that a client's "OK" means the write is committed by a majority and will survive failures.

### Server side

- [ ] `Put`/`Delete` handler: encode a `Command`, call `raft.Start(cmd)`. If not leader, reply `NOT_LEADER` with `leader_hint`.
- [ ] Wait until that log index is applied, with a timeout (for example 2 s). Keep a map `index → waiting channel`.
- [ ] If a **different** command shows up at that index (because leadership changed and the entry was overwritten), reply with an error so the client retries.
- [ ] One applier loop reads `ApplyCh` and calls `store.Apply` for each entry, strictly in order. After a restart the map starts empty and is rebuilt by replaying the log.
- [ ] **Deduplication:** keep `lastSeq[client_id]`. If a command's `seq` isn't newer, skip it but still reply OK. A retried Put is then applied only once (exactly-once).
- [ ] **Reads:** send `Get` through the log too (simplest, and linearizable), or use ReadIndex: the leader records `commitIndex`, confirms it's still leader with one heartbeat round, waits until applied, then reads locally.

### Client library (`client/`)

- [ ] Picks a random node, follows `leader_hint` on `NOT_LEADER`, retries on timeout with the **same** `client_id` and `seq`.
- [ ] `kvctl` from Stage 1 now uses this library.

### Tests

- [ ] `TestKVBasic`: Put then Get returns the value, through any node.
- [ ] `TestKVLeaderChange`: Put, crash the leader, Get from the new leader returns the value.
- [ ] `TestKVDuplicateRetry`: drop the reply to a Put so the client retries; the command is applied only once.
- [ ] `TestKVConcurrentClients`: 5 clients write their own keys at the same time for 10 s; at the end every acknowledged write is readable.

**Done when:** all four pass 100 times in a row with `-race`.

**Makes true:** "fault-tolerant key-value store".

## Stage 8: A real 5-node cluster with gRPC and Docker (2 days)

**Goal:** the same code, unchanged, running as 5 separate processes that talk over real networks.

- [ ] Write `raft.proto` with `RequestVote` and `AppendEntries` messages matching your Go structs (including `conflictTerm` and `conflictIndex`).
- [ ] A **gRPC transport** that implements the same `Transport` interface as the in-memory one. Open **one connection per peer** at start-up and reuse it; set a per-call timeout (for example 100 ms).
- [ ] One gRPC server per node serves both the Raft service and the KV service on one port.
- [ ] `kvnode` reads its config from flags or environment: `--id node1 --peers node1=10.5.0.11:3000,node2=10.5.0.12:3000,… --data /data`.
- [ ] A multi-stage `Dockerfile` (build with `golang`, run on a small base image).
- [ ] `deploy/docker-compose.yml` with **five named services** `node1` … `node5`: fixed IPs on one bridge network, a volume each (`./data/node1:/data`, …) and client ports 8001–8005 published on the host.
- [ ] A `Status` RPC (ID, term, role, leader, commit index, last applied) and `kvctl status` to print all 5.
- [ ] Structured logs with node ID, term and role on every state change.

Membership is **fixed** at these 5 nodes. That keeps "majority" at 3 and "2 failures" well-defined. Dynamic membership changes are an optional extra from section 6 of the Raft paper; don't use gossip to add or remove voters.

**Done when:** `docker compose up` starts 5 nodes; `kvctl status` shows one leader that all 5 agree on; `kvctl put`/`get` work through any of the 5 ports; `docker compose stop node2 node3` still lets writes succeed.

**Makes true:** "5-node".

## Stage 9: Fault-tolerance validation (3–4 days)

**Goal:** automated tests that crash, restart and partition a 5-node cluster while clients keep writing, and prove that every acknowledged write survives. This is the "Validated fault recovery" sentence.

### Invariants checked after every test

1. **Election safety:** at most one leader per term.
2. **Log matching:** same index and term means identical logs up to that point.
3. **Leader completeness:** every committed entry is in every later leader's log.
4. **State machine safety:** no two nodes apply different commands at the same index.
5. **Durability:** every write that got "OK" can be read back at the end.

Add a history recorder to the client library: it logs every request, its reply, and when each happened. Checks 1–4 come from the Stage 2 checker; check 5 comes from the history.

### Scenarios (in-memory harness, 5 nodes, clients writing throughout)

| Test | What it does | Expected result |
| --- | --- | --- |
| `TestLeaderCrash` | Crash the leader | New leader within about 1 s; no acknowledged write lost |
| `TestRepeatedLeaderCrash` | Crash each new leader, 5 times | Elections keep succeeding; all acknowledged writes present |
| `TestTwoFollowersDown` | Crash 2 followers | Writes keep committing (3 of 5) |
| `TestLeaderAndFollowerDown` | Crash the leader and 1 follower | Remaining 3 elect a leader; writes continue |
| `TestThreeDown` | Crash 3 nodes | No commits and no wrong data; restart 1 and writes resume |
| `TestRestartFollower` | Restart a follower that missed writes | It reloads its log and catches up |
| `TestRestartAll` | Restart all 5 | Every acknowledged write still readable |
| `TestPartitionLeaderInMinority` | Split 2 (with leader) vs 3 | The 3 elect a new leader and commit; the old leader's writes never commit; after healing it steps down and its entries are replaced |
| `TestPartitionLeaderInMajority` | Split 3 (with leader) vs 2 | Leader keeps committing; the 2 catch up after healing |
| `TestFlakyNetwork` | Drop 20% of messages and delay the rest, 30 s | Progress continues; all invariants hold |
| `TestChaos` | Random crashes, restarts and partitions, never more than 2 nodes down, 60 s | All invariants hold; every acknowledged write readable |

### Real cluster (Docker)

- [ ] `scripts/chaos.sh`: against the 5-node compose cluster, use `docker kill`, `docker start` and `docker network disconnect/connect` to repeat the leader-crash, two-down, restart and partition scenarios.
- [ ] `cmd/loadgen`: writes numbered keys from several clients, records which got "OK", and checks them all at the end.

### Running

- [ ] Run the whole suite at least 100 times with `-race` (`go test ./tests -count=100 -race`). Raft bugs are often rare timing bugs; one pass proves little.
- [ ] Save a results table (scenario, runs, passes) for the README.

**Done when:** every scenario passes 100 of 100 runs, and the Docker chaos script passes.

**Makes true:** "Validated fault recovery across leader crashes, restarts, partitions, tolerating 2 failures preserving committed state".

## Stage 10: README, CI, demo and final check (1–2 days)

**Goal:** anyone who opens the repo can check every claim in a few minutes.

- [ ] **README:** what it is; an architecture diagram; how to run 5 nodes; how to run the tests; the Stage 9 results table; design choices (fixed membership, reads through the log or ReadIndex); known limits (for example no snapshots, if you skipped them).
- [ ] **Demo script** `scripts/demo.sh`: start 5 nodes, write 100 keys, kill the leader, kill one more node, write 100 more, restart both, read all 200 back. Record it as a short terminal recording (for example with asciinema) and link it from the README.
- [ ] **CI:** a GitHub Actions workflow running `go vet`, `go test -race ./...` and a short chaos run on every push, with a status badge.
- [ ] Tag `v1.0.0`.
- [ ] Go through the claim map below. Every row should point to a test that passes.

**Done when:** a fresh clone, `make test` and `docker compose up` reproduce everything in the README.

## Claim map and interview preparation

### Which stage makes each claim true

| Claim in the description | Built in | Proven by |
| --- | --- | --- |
| 5-node | Stage 8 | 5-node harness and compose cluster |
| Fault-tolerant key-value store | Stage 7 | `TestKVLeaderChange`, `TestKVConcurrentClients` |
| Raft consensus, replicated logs | Stages 3–4 | `TestBasicAgree`, log-matching invariant |
| Majority-based commitment | Stage 5 | `TestCommitWithTwoDown`, `TestNoCommitWithThreeDown`, `TestFigure8` |
| Leader election via RequestVote, timeouts | Stage 3 | `TestInitialElection`, `TestReElection`, `TestManyElections` |
| Log replication via AppendEntries | Stage 4 | `TestFollowerCatchUp` |
| Backtracking | Stage 4 | `TestConflictingLogs`, `TestBacktrackingSpeed` |
| Validated leader-crash recovery | Stage 9 | `TestLeaderCrash`, `TestRepeatedLeaderCrash` |
| Validated restarts | Stages 6, 9 | `TestRestartFollower`, `TestRestartAll` |
| Validated partitions | Stages 2, 9 | `TestPartitionLeaderInMinority`, `TestPartitionLeaderInMajority` |
| Tolerating 2 failures | Stages 5, 9 | `TestTwoFollowersDown`, `TestLeaderAndFollowerDown`, `TestChaos` |
| Preserving committed state | Stages 6, 7, 9 | Durability invariant across every scenario |

**If you stop early:** after Stage 5 you can honestly claim election, replication, backtracking and majority commit. Leave out "restarts" until Stage 6, and "validated …" until Stage 9.

### Questions to be ready for

| Question | Your answer comes from |
| --- | --- |
| Why can't there be two leaders in one term? | One saved vote per term (Stage 6) and overlapping majorities (lecture 5.2) |
| What if the leader crashes right after a client writes? | OK is sent only after commit (Stage 7); the client retries with the same seq, and duplicates are skipped |
| How does a lagging follower catch up? | `nextIndex` back-off, fast backtracking and conflict truncation (Stage 4) |
| Why can't a leader commit an old-term entry by counting copies? | Figure 8 and the current-term rule, plus the no-op entry (Stage 5) |
| What goes to disk, and when? | Term, vote and log, fsync before replying (Stage 6) |
| How did you test partitions? | In-memory network that blocks links, plus Docker network disconnect (Stages 2, 9) |
| Why 5 nodes, not 4? | 5 tolerates 2 failures; 4 still tolerates only 1, because its majority is 3 |
| Are reads linearizable? | Reads go through the log or ReadIndex (Stage 7) |
| What would you add next? | Snapshots and log compaction, membership changes, sharding into several Raft groups |

### Sources

- [Raft paper (Ongaro and Ousterhout, 2014)](https://raft.github.io/raft.pdf), Figure 2 and sections 5–8
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
- [gRPC Go basics tutorial](https://grpc.io/docs/tutorials/basic/go/)
- [Martin Kleppmann's Distributed Systems lectures](https://www.youtube.com/playlist?list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB)
- [Existing KVRaft repository](https://github.com/Meet-Modi/KVRaft) and its [project report](https://claude.ai/artifact/PoXbXGBqihYWLXAyT24Aba)
