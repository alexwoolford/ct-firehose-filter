#!/usr/bin/env bash
# Disk soak sampler: record novelty.db / alerts.jsonl / docker log sizes over time.
#
# Compressed (offline, ~minutes): replay dump cold+warm and write growth CSV.
# Live (hours): sample Compose novelty volume + container logs.
#
# Usage:
#   deploy/scripts/preflight-soak.sh --compressed
#   deploy/scripts/preflight-soak.sh --live [--hours 4] [--interval 300]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

MODE=""
HOURS=4
INTERVAL=300
JSONL="${PREFLIGHT_JSONL:-/tmp/ct-ma-eval.jsonl}"
WORKDIR="${PREFLIGHT_DIR:-/tmp/ct-preflight}"
OUT="$WORKDIR/soak.csv"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --compressed) MODE=compressed; shift ;;
    --live) MODE=live; shift ;;
    --hours) HOURS="$2"; shift 2 ;;
    --interval) INTERVAL="$2"; shift 2 ;;
    -h|--help)
      echo "usage: $0 --compressed | --live [--hours N] [--interval SECS]"
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

if [[ -z "$MODE" ]]; then
  echo "usage: $0 --compressed | --live [--hours N] [--interval SECS]" >&2
  exit 1
fi

mkdir -p "$WORKDIR"
bytes() { wc -c <"$1" 2>/dev/null | tr -d ' ' || echo 0; }

if [[ "$MODE" == "compressed" ]]; then
  DB="$WORKDIR/soak-novelty.db"
  ALERTS="$WORKDIR/soak-alerts.jsonl"
  rm -f "$DB" "$DB-wal" "$DB-shm" "$ALERTS" "$ALERTS".*
  echo "ts,phase,novelty_db_bytes,alerts_bytes,coalitions,hosts,alerts_a" >"$OUT"

  sample() {
    local phase="$1" a="${2:-}"
    local ts hosts coalitions
    ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    coalitions=$(sqlite3 "$DB" "SELECT COUNT(*) FROM coalitions;" 2>/dev/null || echo 0)
    hosts=$(sqlite3 "$DB" "SELECT COUNT(*) FROM hosts;" 2>/dev/null || echo 0)
    local db_b
    db_b=$(bytes "$DB")
    db_b=$((db_b + $(bytes "$DB-wal") + $(bytes "$DB-shm")))
    echo "$ts,$phase,$db_b,$(bytes "$ALERTS"),$coalitions,$hosts,$a" | tee -a "$OUT"
  }

  echo "== compressed soak: cold pass =="
  NOVELTY_REQUIRE_DB=0 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
    cargo run --release --example novelty_replay -- "$JSONL" "$DB" "$ALERTS" | tee "$WORKDIR/soak-cold.txt"
  A=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/soak-cold.txt")
  sample cold "$A"

  echo "== compressed soak: warm re-pass (expect flat) =="
  : >"$ALERTS"
  NOVELTY_REQUIRE_DB=1 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
    cargo run --release --example novelty_replay -- "$JSONL" "$DB" "$ALERTS" | tee "$WORKDIR/soak-warm.txt"
  A=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/soak-warm.txt")
  sample warm "$A"

  echo "== compressed soak: rotate stress (tiny max bytes) =="
  # Fresh DB so A′ fires again and exercises rotation under load.
  rm -f "$DB" "$DB-wal" "$DB-shm" "$ALERTS" "$ALERTS".*
  NOVELTY_ALERTS_MAX_BYTES=4096 NOVELTY_ALERTS_KEEP=2 \
  NOVELTY_REQUIRE_DB=0 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
    cargo run --release --example novelty_replay -- "$JSONL" "$DB" "$ALERTS" >/dev/null
  ROT=$(find "$WORKDIR" -name 'soak-alerts.jsonl.*' | wc -l | tr -d ' ')
  echo "rotated_siblings=$ROT"
  if [[ "$ROT" -lt 1 ]]; then
    echo "FAIL: expected alerts rotation with 4KiB cap" >&2
    exit 1
  fi
  # keep=2 ⇒ live + ≤2 siblings bound disk (older rotations pruned on purpose).
  TOTAL_ALERT_BYTES=0
  for f in "$ALERTS" "$ALERTS".*; do
    [[ -f "$f" ]] || continue
    TOTAL_ALERT_BYTES=$((TOTAL_ALERT_BYTES + $(bytes "$f")))
  done
  # 3 files × ~4–8KiB headroom
  if [[ "$TOTAL_ALERT_BYTES" -gt 65536 ]]; then
    echo "FAIL: rotated alerts should stay bounded (got $TOTAL_ALERT_BYTES bytes)" >&2
    exit 1
  fi
  echo "alerts_retained_bytes=$TOTAL_ALERT_BYTES (bounded by KEEP)"
  sample rotate_stress

  echo "PASS: compressed soak → $OUT"
  column -t -s, "$OUT" 2>/dev/null || cat "$OUT"
  exit 0
fi

echo "ts,novelty_db_bytes,alerts_bytes,docker_logs_bytes,df_used_pct" >"$OUT"
END=$(( $(date +%s) + HOURS * 3600 ))
if [[ "$HOURS" -eq 0 ]]; then
  END=$(( $(date +%s) + INTERVAL ))
fi
echo "live soak hours=$HOURS interval=${INTERVAL}s → $OUT"

bytes_in_novelty() {
  local path="$1"
  docker exec ct-novelty-consumer sh -c "wc -c <'$path' 2>/dev/null" 2>/dev/null | tr -d ' \r' || echo 0
}

while [[ $(date +%s) -lt $END ]]; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  db_b=0
  al_b=0
  if docker ps --format '{{.Names}}' | grep -qx ct-novelty-consumer; then
    db_b=$(bytes_in_novelty /var/lib/ct-firehose-filter/novelty.db)
    wal_b=$(bytes_in_novelty /var/lib/ct-firehose-filter/novelty.db-wal)
    shm_b=$(bytes_in_novelty /var/lib/ct-firehose-filter/novelty.db-shm)
    db_b=$((db_b + wal_b + shm_b))
    al_b=$(bytes_in_novelty /var/lib/ct-firehose-filter/alerts.jsonl)
    # Sum rotated siblings inside the container.
    extra=$(docker exec ct-novelty-consumer sh -c \
      "cat /var/lib/ct-firehose-filter/alerts.jsonl.* 2>/dev/null | wc -c" 2>/dev/null | tr -d ' \r' || echo 0)
    al_b=$((al_b + extra))
  fi
  log_b=0
  for c in certstream-sidecar ct-firehose-filter ct-novelty-consumer; do
    log=$(docker inspect --format='{{.LogPath}}' "$c" 2>/dev/null || true)
    if [[ -n "$log" ]]; then
      # Docker Desktop: LogPath is inside the VM; use docker logs size estimate.
      sz=$(docker logs "$c" 2>/dev/null | wc -c | tr -d ' ')
      log_b=$((log_b + sz))
    fi
  done
  df_pct=$(df -P "$WORKDIR" | awk 'NR==2{gsub(/%/,"",$5); print $5}')
  echo "$ts,$db_b,$al_b,$log_b,$df_pct" | tee -a "$OUT"
  sleep "$INTERVAL"
done

echo "PASS: live soak complete → $OUT"
