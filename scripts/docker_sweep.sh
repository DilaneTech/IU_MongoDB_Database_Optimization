#!/usr/bin/env bash
# Constrained-environment benchmark: load the 4 configurations once, then for
# each MongoDB memory limit run the workload on each configuration while
# sampling `docker stats` (to show the app container is not the bottleneck).
#
#   bash scripts/docker_sweep.sh            # load + sweep
#   SKIP_LOAD=1 bash scripts/docker_sweep.sh  # sweep only
set -euo pipefail
cd "$(dirname "$0")/.."

MEMS=(6g 3g 1.5g)
CONFIGS=(baseline optimized baseline_indexed optimized_indexed)
run() { docker compose exec -T app python "$@"; }

mongo_with() {  # restart mongo with a new memory limit and wait until healthy
  MONGO_MEM=$1 docker compose up -d --wait mongo app
}

if [[ -z "${SKIP_LOAD:-}" ]]; then
  mongo_with 6g
  for s in "${CONFIGS[@]}"; do run src/import_data.py --schema "$s"; done
  for s in baseline_indexed optimized_indexed; do run src/create_indexes.py --schema "$s"; done
fi

for m in "${MEMS[@]}"; do
  mongo_with "$m"
  for s in "${CONFIGS[@]}"; do
    label="docker_${m}_${s}"
    stats="results/dockerstats_${label}.csv"
    echo "time,container,cpu,mem" > "$stats"
    ( while true; do
        docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" \
          | sed "s/^/$(date +%T),/" >> "$stats"
        sleep 5
      done ) &
    sampler=$!
    run src/workload.py --schema "$s" --label "$label"
    kill "$sampler"
  done
done
echo "SWEEP DONE"
