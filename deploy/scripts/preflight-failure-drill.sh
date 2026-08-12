#!/usr/bin/env bash
# Failure drill: stop consumer → SQS backlog → snapshot → wipe DB → restore → no cold flood.
#
# Offline mode (default): uses local file "snapshot" + novelty_replay on a JSONL dump.
# With --s3: uploads/restores via novelty-s3-*.sh (needs NOVELTY_S3_URI or arg).
#
# Usage:
#   deploy/scripts/preflight-failure-drill.sh
#   deploy/scripts/preflight-failure-drill.sh --s3 s3://bucket/ct-preflight/novelty.db
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

JSONL="${PREFLIGHT_JSONL:-/tmp/ct-ma-eval.jsonl}"
WORKDIR="${PREFLIGHT_DIR:-/tmp/ct-preflight}"
S3_URI=""
USE_S3=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --s3)
      USE_S3=1
      S3_URI="${2:-${NOVELTY_S3_URI:-}}"
      shift
      if [[ $# -gt 0 && "$1" != --* ]]; then shift; fi
      ;;
    -h|--help)
      echo "usage: $0 [--s3 [s3://bucket/key]]"
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 1 ;;
  esac
done

mkdir -p "$WORKDIR"
DB="$WORKDIR/drill-novelty.db"
ALERTS="$WORKDIR/drill-alerts.jsonl"
SNAP="$WORKDIR/drill-novelty.snapshot.db"
HALF1="${PREFLIGHT_HALF1:-/tmp/ct-ma-eval-half1.jsonl}"
HALF2="${PREFLIGHT_HALF2:-/tmp/ct-ma-eval-half2.jsonl}"

if [[ ! -f "$JSONL" ]]; then
  echo "missing $JSONL" >&2
  exit 1
fi

# Build halves if missing (simulates outage gap / catch-up).
if [[ ! -f "$HALF1" || ! -f "$HALF2" ]]; then
  echo "splitting $JSONL into halves..."
  total=$(wc -l <"$JSONL" | tr -d ' ')
  mid=$((total / 2))
  head -n "$mid" "$JSONL" >"$WORKDIR/half1.jsonl"
  tail -n +"$((mid + 1))" "$JSONL" >"$WORKDIR/half2.jsonl"
  HALF1="$WORKDIR/half1.jsonl"
  HALF2="$WORKDIR/half2.jsonl"
fi

rm -f "$DB" "$DB-wal" "$DB-shm" "$ALERTS" "$ALERTS".* "$SNAP"

echo "== 1) warm DB on first half (simulates running consumer) =="
NOVELTY_REQUIRE_DB=0 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
  cargo run --release --example novelty_replay -- "$HALF1" "$DB" "$ALERTS" | tee "$WORKDIR/drill-h1.txt"
A1=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/drill-h1.txt")
echo "half1 A′=$A1"

echo "== 2) snapshot (consumer 'stopped'; state durable) =="
if command -v sqlite3 >/dev/null; then
  sqlite3 "$DB" "PRAGMA wal_checkpoint(TRUNCATE);"
  # `.backup` is a shell meta-command — must be stdin, not -cmd SQL.
  sqlite3 "$DB" <<EOF
.backup '$SNAP'
EOF
else
  cp -f "$DB" "$SNAP"
fi
test -s "$SNAP"
SNAP_BYTES=$(wc -c <"$SNAP" | tr -d ' ')

if [[ $USE_S3 -eq 1 ]]; then
  if [[ -z "$S3_URI" ]]; then
    echo "set NOVELTY_S3_URI or pass --s3 s3://bucket/key" >&2
    exit 1
  fi
  echo "== 2b) upload snapshot to $S3_URI =="
  NOVELTY_DB="$SNAP" NOVELTY_S3_URI="$S3_URI" "$ROOT/deploy/scripts/novelty-s3-snapshot.sh" "$SNAP" "$S3_URI"
fi

echo "== 3) wipe DB (simulates disk wipe / new volume) =="
rm -f "$DB" "$DB-wal" "$DB-shm"
: >"$ALERTS"

echo "== 4) restore before start =="
if [[ $USE_S3 -eq 1 ]]; then
  NOVELTY_DB="$DB" NOVELTY_S3_URI="$S3_URI" "$ROOT/deploy/scripts/novelty-s3-restore.sh" "$S3_URI" "$DB"
else
  cp -f "$SNAP" "$DB"
  rm -f "$DB-wal" "$DB-shm"
  echo "restored local snapshot ($SNAP_BYTES bytes) → $DB"
fi
test -f "$DB"

echo "== 5) REQUIRE_DB=1 + second half (catch-up; should NOT re-flood half1 keys) =="
NOVELTY_REQUIRE_DB=1 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
  cargo run --release --example novelty_replay -- "$HALF2" "$DB" "$ALERTS" | tee "$WORKDIR/drill-h2.txt"
A2=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/drill-h2.txt")

echo "== 6) control: cold start on half2 alone (upper bound) =="
COLD_DB="$WORKDIR/drill-cold-h2.db"
COLD_ALERTS="$WORKDIR/drill-cold-h2-alerts.jsonl"
rm -f "$COLD_DB" "$COLD_DB-wal" "$COLD_DB-shm" "$COLD_ALERTS"
NOVELTY_REQUIRE_DB=0 NOVELTY_DB="$COLD_DB" NOVELTY_ALERTS="$COLD_ALERTS" NOVELTY_TIERS=A \
  cargo run --release --example novelty_replay -- "$HALF2" "$COLD_DB" "$COLD_ALERTS" | tee "$WORKDIR/drill-cold-h2.txt"
A2_COLD=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/drill-cold-h2.txt")

echo "restored catch-up A′=$A2  vs cold half2 A′=$A2_COLD"
# Restored path should emit ≤ cold (usually much less if halves share coalitions).
if [[ "$A2" -gt "$A2_COLD" ]]; then
  echo "FAIL: restored catch-up emitted more than cold half2 ($A2 > $A2_COLD)" >&2
  exit 1
fi

echo "== 7) REQUIRE_DB=1 refuses start when DB missing =="
rm -f "$WORKDIR/missing.db"
set +e
NOVELTY_REQUIRE_DB=1 NOVELTY_DB="$WORKDIR/missing.db" NOVELTY_ALERTS="$ALERTS" \
  cargo run --release --example novelty_replay -- "$HALF2" "$WORKDIR/missing.db" "$ALERTS" >/dev/null 2>"$WORKDIR/drill-missing.txt"
rc=$?
set -e
[[ $rc -ne 0 ]]
grep -q "NOVELTY_REQUIRE_DB=1" "$WORKDIR/drill-missing.txt"

echo "PASS: failure drill (restored A′=$A2 ≤ cold A′=$A2_COLD)"
