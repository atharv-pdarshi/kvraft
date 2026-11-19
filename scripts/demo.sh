#!/usr/bin/env bash
# demo.sh — end-to-end demonstration with five local processes (no Docker).
#
# Starts 5 nodes, writes 100 keys, kills the leader and one more node, writes
# 100 more, restarts both, and reads all 200 back. Proves that writes survive
# leader crashes and that a restarted node catches up.
set -uo pipefail

cd "$(dirname "$0")/.."

PORTS=(9101 9102 9103 9104 9105)
PEERS="node1=localhost:9101,node2=localhost:9102,node3=localhost:9103,node4=localhost:9104,node5=localhost:9105"
DATA=$(mktemp -d)
PIDS=()

cleanup() {
  for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null; done
  rm -rf "$DATA"
}
trap cleanup EXIT

say() { echo -e "\n=== $* ==="; }

say "building binaries"
go build -o bin/kvnode ./cmd/kvnode
go build -o bin/kvctl  ./cmd/kvctl

say "starting 5 nodes (data in $DATA)"
for i in 1 2 3 4 5; do
  ./bin/kvnode -id "node$i" -peers "$PEERS" -data "$DATA/node$i" >"$DATA/node$i.log" 2>&1 &
  PIDS+=($!)
done
sleep 3

kv() { ./bin/kvctl -peers "$PEERS" "$@"; }
leader_idx() { kv status 2>/dev/null | awk '$2=="Leader"{sub(/node/,"",$1); print $1; exit}'; }

say "cluster status"
kv status

say "writing keys 1..100"
for n in $(seq 1 100); do kv put "key$n" "val$n" >/dev/null; done
echo "wrote 100 keys"

L=$(leader_idx); echo "current leader: node$L"
say "killing the leader (node$L) and one more node"
kill "${PIDS[$((L-1))]}" 2>/dev/null
OTHER=$(( L % 5 + 1 ))
kill "${PIDS[$((OTHER-1))]}" 2>/dev/null
sleep 4
kv status

say "writing keys 101..200 with 2 nodes down"
for n in $(seq 101 200); do kv put "key$n" "val$n" >/dev/null; done
echo "wrote 100 more keys"

say "restarting the two downed nodes"
for i in "$L" "$OTHER"; do
  ./bin/kvnode -id "node$i" -peers "$PEERS" -data "$DATA/node$i" >"$DATA/node$i.restart.log" 2>&1 &
  PIDS[$((i-1))]=$!
done
sleep 4
kv status

say "reading all 200 keys back"
missing=0
for n in $(seq 1 200); do
  got=$(kv get "key$n")
  if [[ "$got" != "val$n" ]]; then
    missing=$((missing+1))
    [[ $missing -le 10 ]] && echo "MISSING key$n: got '$got', want 'val$n'"
  fi
done
if [[ $missing -eq 0 ]]; then
  echo -e "\nOK: all 200 keys readable after leader crash + restart"
else
  echo -e "\nFAIL: $missing/200 keys missing"
  exit 1
fi
