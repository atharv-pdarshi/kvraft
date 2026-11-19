#!/usr/bin/env bash
# chaos.sh — fault injection against the Docker compose cluster.
#
# It starts loadgen writing in the background, then repeats the leader-crash,
# two-down, restart and partition scenarios using docker kill/start and docker
# network disconnect/connect. At the end loadgen checks that every acknowledged
# write is still readable.
#
#   cd deploy && docker compose up --build -d
#   ../scripts/chaos.sh
set -euo pipefail

cd "$(dirname "$0")/.."
PEERS="node1=localhost:8001,node2=localhost:8002,node3=localhost:8003,node4=localhost:8004,node5=localhost:8005"
NET="deploy_kvnet"
NODES=(node1 node2 node3 node4 node5)

say() { echo -e "\n=== $* ==="; }

leader() {
  # Ask any node for the current leader id.
  go run ./cmd/kvctl -peers "$PEERS" status 2>/dev/null | awk '$2=="Leader"{print $1; exit}'
}

say "building loadgen"
go build -o bin/loadgen ./cmd/loadgen

say "starting background load (90s)"
./bin/loadgen -peers "$PEERS" -clients 4 -duration 90s >/tmp/loadgen.out 2>&1 &
LOAD_PID=$!

sleep 5

say "scenario: crash the leader"
L=$(leader); echo "leader is $L"; docker kill "$L" || true
sleep 8
echo "new leader: $(leader)"
docker start "$L"; sleep 5

say "scenario: two followers down"
L=$(leader)
DOWN=(); for n in "${NODES[@]}"; do [[ "$n" != "$L" ]] && DOWN+=("$n"); done
docker kill "${DOWN[0]}" "${DOWN[1]}" || true
sleep 8
docker start "${DOWN[0]}" "${DOWN[1]}"; sleep 5

say "scenario: partition the leader into a minority"
L=$(leader)
# Disconnect the leader and one follower from the bridge network.
PARTNER=""; for n in "${NODES[@]}"; do [[ "$n" != "$L" ]] && PARTNER="$n" && break; done
docker network disconnect "$NET" "$L" || true
docker network disconnect "$NET" "$PARTNER" || true
sleep 8
echo "new leader among majority: $(leader)"
docker network connect "$NET" "$L" || true
docker network connect "$NET" "$PARTNER" || true
sleep 5

say "waiting for load to finish and verify"
wait "$LOAD_PID"
cat /tmp/loadgen.out
