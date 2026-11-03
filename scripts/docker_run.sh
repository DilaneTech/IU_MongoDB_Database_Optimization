#!/usr/bin/env bash
# Run the workload on given configurations at one constraint level, sampling
# `docker stats`. Data must already be loaded (see docker_sweep.sh).
#
#   bash scripts/docker_run.sh <mongo_mem> <mongo_cpus> <config>...
#   bash scripts/docker_run.sh 512m 4 baseline_indexed optimized_indexed
#   READ_RATE=300 WRITE_RATE=2000 bash scripts/docker_run.sh 1g 2 optimized_indexed   # open loop
#   RUN_SUFFIX=_r2 ...   # repeat a run without overwriting its result
set -euo pipefail
cd "$(dirname "$0")/.."

mem=$1 cpus=$2; shift 2
MONGO_MEM=$mem MONGO_CPUS=$cpus docker compose up -d --wait mongo app

cpu_tag=$([[ $cpus == 4 ]] && echo "" || echo "${cpus}cpu_")
open_args=() open_tag=""
if [[ -n "${READ_RATE:-}" ]]; then
  open_args=(--read-rate "$READ_RATE" --write-rate "$WRITE_RATE")
  open_tag="open${READ_RATE}r${WRITE_RATE}w_"
fi
for s in "$@"; do
  label="docker_${mem}_${cpu_tag}${open_tag}${s}${RUN_SUFFIX:-}"
  stats="results/dockerstats_${label}.csv"
  echo "time,container,cpu,mem" > "$stats"
  ( while true; do
      docker stats --no-stream --format "{{.Name}},{{.CPUPerc}},{{.MemUsage}}" \
        | sed "s/^/$(date +%T),/" >> "$stats"
      sleep 5
    done ) &
  sampler=$!
  docker compose exec -T app python src/workload.py --schema "$s" --label "$label" "${open_args[@]}" \
    || echo "RUN FAILED: $label"
  kill "$sampler"
done
