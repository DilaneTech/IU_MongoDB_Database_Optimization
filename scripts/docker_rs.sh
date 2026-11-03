#!/usr/bin/env bash
# Two copies of the DB (replica set rs0: mongo1 primary, mongo2 secondary),
# each limited like the single-node breaking point (1g / 2 CPUs).
#
#   bash scripts/docker_rs.sh setup   # stop single mongo, start rs, load optimized_indexed
#   bash scripts/docker_rs.sh runs    # 3 open-loop runs: primary / secondary / secondary+majority
#   READ_RATE=500 WRITE_RATE=200 bash scripts/docker_rs.sh readheavy   # primary / nearest
set -euo pipefail
cd "$(dirname "$0")/.."

RS_URI="mongodb://mongo1:27017,mongo2:27017/?replicaSet=rs0"
SCHEMA=optimized_indexed
export RS_MEM=1g RS_CPUS=2
READ_RATE=${READ_RATE:-300} WRITE_RATE=${WRITE_RATE:-2000}

setup() {
  docker compose stop mongo  # frees the host; its data stays in its volume
  docker compose --profile rs up -d --wait mongo1 mongo2
  docker compose exec -T mongo1 mongosh --quiet --eval '
    try { rs.status() } catch (e) {
      rs.initiate({_id: "rs0", members: [
        {_id: 0, host: "mongo1:27017", priority: 2},
        {_id: 1, host: "mongo2:27017", priority: 1}]})
    }
    while (!db.hello().isWritablePrimary) sleep(1000)
    print("primary:", db.hello().primary)'
  # w=1 for the bulk load: a 2-member set otherwise defaults to w:majority
  docker compose exec -T -e MONGO_URI="$RS_URI&w=1" -e RESULT_PREFIX=docker_rs_ \
    app python src/import_data.py --schema "$SCHEMA"
  docker compose exec -T -e MONGO_URI="$RS_URI" -e RESULT_PREFIX=docker_rs_ \
    app python src/create_indexes.py --schema "$SCHEMA"
}

run() {  # run <label> <read_pref> <write_concern>
  local label="docker_rs_1g_2cpu_open${READ_RATE}r${WRITE_RATE}w_$1"
  local stats="results/dockerstats_${label}.csv"
  echo "time,container,cpu,mem" > "$stats"
  ( while true; do
      docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" \
        | sed "s/^/$(date +%T),/" >> "$stats"
      sleep 5
    done ) &
  local sampler=$!
  docker compose exec -T app python src/workload.py --schema "$SCHEMA" --label "$label" \
    --uri "$RS_URI" --read-pref "$2" --write-concern "$3" \
    --read-rate "$READ_RATE" --write-rate "$WRITE_RATE" || echo "RUN FAILED: $label"
  kill "$sampler"
}

case "${1:-}" in
  setup) setup ;;
  runs)
    run primary_w1 primary 1
    run secondary_w1 secondaryPreferred 1
    run secondary_wmajority secondaryPreferred majority
    ;;
  readheavy)
    docker compose stop mongo
    docker compose --profile rs up -d --wait mongo1 mongo2
    run primary_w1 primary 1
    run nearest_w1 nearest 1
    ;;
  *) echo "usage: $0 setup|runs|readheavy"; exit 1 ;;
esac
