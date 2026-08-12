#!/usr/bin/env bash
# Quiet smoke: REQUIRE_DB 0→1, optional Compose+throwaway SQS.
# Usage:
#   deploy/scripts/preflight-smoke.sh              # offline novelty + config checks
#   deploy/scripts/preflight-smoke.sh --compose    # also bring up prod overlay briefly
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

COMPOSE=0
for arg in "$@"; do
  case "$arg" in
    --compose) COMPOSE=1 ;;
    -h|--help)
      echo "usage: $0 [--compose]"
      exit 0
      ;;
  esac
done

WATCHLIST="${WATCHLIST_HOST_PATH:-}"
JSONL="${PREFLIGHT_JSONL:-/tmp/ct-ma-eval.jsonl}"
WORKDIR="${PREFLIGHT_DIR:-/tmp/ct-preflight}"
mkdir -p "$WORKDIR"
DB="$WORKDIR/novelty.db"
ALERTS="$WORKDIR/alerts.jsonl"
rm -f "$DB" "$DB-wal" "$DB-shm" "$ALERTS" "$ALERTS".*

echo "== preflight smoke: offline REQUIRE_DB =="
if [[ ! -f "$JSONL" ]]; then
  echo "missing $JSONL — run a live_smoke dump first" >&2
  exit 1
fi

# Must refuse missing DB when REQUIRE_DB=1
set +e
NOVELTY_REQUIRE_DB=1 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
  cargo run --release --example novelty_replay -- "$JSONL" "$DB" "$ALERTS" >/dev/null 2>"$WORKDIR/require_fail.txt"
rc=$?
set -e
if [[ $rc -eq 0 ]]; then
  echo "FAIL: NOVELTY_REQUIRE_DB=1 should refuse missing DB" >&2
  exit 1
fi
grep -q "NOVELTY_REQUIRE_DB=1" "$WORKDIR/require_fail.txt"
echo "ok: REQUIRE_DB=1 refuses missing DB"

# Cold create
NOVELTY_REQUIRE_DB=0 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
  cargo run --release --example novelty_replay -- "$JSONL" "$DB" "$ALERTS" | tee "$WORKDIR/cold.txt"
test -f "$DB"
COLD_A=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/cold.txt")
COLD_HOSTS=$(awk '/db_hosts:/ {print $2}' "$WORKDIR/cold.txt")
echo "cold A′=$COLD_A hosts=$COLD_HOSTS"
if [[ "${COLD_HOSTS:-1}" != "0" ]]; then
  echo "FAIL: A′-only should not grow hosts table (got $COLD_HOSTS)" >&2
  exit 1
fi

# Warm re-run with REQUIRE_DB=1 — near-zero new A′
cp -f "$ALERTS" "$WORKDIR/alerts.cold.jsonl"
: > "$ALERTS"
NOVELTY_REQUIRE_DB=1 NOVELTY_DB="$DB" NOVELTY_ALERTS="$ALERTS" NOVELTY_TIERS=A \
  cargo run --release --example novelty_replay -- "$JSONL" "$DB" "$ALERTS" | tee "$WORKDIR/warm.txt"
WARM_A=$(awk '/alerts_A_prime:/ {print $2}' "$WORKDIR/warm.txt")
echo "warm re-pass A′=$WARM_A (expect 0)"
if [[ "${WARM_A:-1}" != "0" ]]; then
  echo "FAIL: warm re-pass should emit 0 A′" >&2
  exit 1
fi

# Prod overlay hard no-gos
grep -q 'EGRESS: sqs' docker-compose.prod.yml
grep -q 'max-size: "10m"' docker-compose.prod.yml
grep -q 'NOVELTY_TIERS: A' docker-compose.prod.yml
echo "ok: prod compose quiet defaults present"

if [[ $COMPOSE -eq 1 && ( -z "$WATCHLIST" || ! -f "$WATCHLIST" ) ]]; then
  echo "ERROR: set WATCHLIST_HOST_PATH to your full domains.txt for --compose" >&2
  exit 1
fi
if [[ -n "$WATCHLIST" && ! -f "$WATCHLIST" ]]; then
  echo "WARN: watchlist missing at $WATCHLIST" >&2
elif [[ $COMPOSE -eq 1 ]]; then
  echo "== preflight smoke: compose + throwaway SQS =="
  REGION="${AWS_REGION:-us-west-2}"
  SUFFIX="$(date +%s)"
  MATCH_Q="ct-preflight-matches-$SUFFIX"
  MATCH_URL=$(aws sqs create-queue --queue-name "$MATCH_Q" --region "$REGION" \
    --query QueueUrl --output text)
  echo "created $MATCH_URL"
  STATUS=0
  cleanup() {
    local ec=$?
    aws sqs delete-queue --queue-url "$MATCH_URL" --region "$REGION" >/dev/null 2>&1 || true
    docker compose -f docker-compose.yml -f docker-compose.prod.yml \
      --env-file "$WORKDIR/env.prod" down >/dev/null 2>&1 || true
    exit "${STATUS:-$ec}"
  }
  trap cleanup EXIT

  cat >"$WORKDIR/env.prod" <<EOF
SQS_QUEUE_URL=$MATCH_URL
AWS_REGION=$REGION
WATCHLIST_HOST_PATH=$WATCHLIST
NOVELTY_REQUIRE_DB=0
NOVELTY_MAX_COALITION=5
NOVELTY_ALERTS_MAX_BYTES=52428800
NOVELTY_ALERTS_KEEP=3
EOF
  # Pass host AWS creds into containers (instance-role path is Oracle-only).
  if [[ -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    {
      echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
      echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
      [[ -n "${AWS_SESSION_TOKEN:-}" ]] && echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN"
    } >>"$WORKDIR/env.prod"
  elif command -v aws >/dev/null 2>&1; then
    if creds=$(aws configure export-credentials --format env 2>/dev/null); then
      # shellcheck disable=SC2086
      eval "$creds"
      {
        echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
        echo "AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY"
        [[ -n "${AWS_SESSION_TOKEN:-}" ]] && echo "AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN"
      } >>"$WORKDIR/env.prod"
    fi
  fi

  docker compose -f docker-compose.yml -f docker-compose.prod.yml \
    --env-file "$WORKDIR/env.prod" up --build -d

  echo "waiting for filter+novelty healthy..."
  sleep 45
  docker compose -f docker-compose.yml -f docker-compose.prod.yml \
    --env-file "$WORKDIR/env.prod" ps
  # Filter at warn should not spam match JSON; novelty should start.
  FILTER_LOG=$(docker logs ct-firehose-filter 2>&1 | tail -n 80 || true)
  if echo "$FILTER_LOG" | grep -E '"matched_domains"|EGRESS=stdout' >/dev/null; then
    echo "FAIL: filter looks chatty / stdout-like" >&2
    echo "$FILTER_LOG" >&2
    STATUS=1
    exit 1
  fi
  NOV_LOG=$(docker logs ct-novelty-consumer 2>&1 | tail -n 80 || true)
  echo "$NOV_LOG" | grep -q "starting ct-novelty-consumer" || {
    echo "FAIL: novelty did not start"
    echo "$NOV_LOG" >&2
    STATUS=1
    exit 1
  }

  # Flip REQUIRE_DB=1 and recreate novelty (DB now exists on volume)
  sed -i.bak 's/NOVELTY_REQUIRE_DB=0/NOVELTY_REQUIRE_DB=1/' "$WORKDIR/env.prod"
  docker compose -f docker-compose.yml -f docker-compose.prod.yml \
    --env-file "$WORKDIR/env.prod" up -d novelty
  sleep 10
  if ! docker logs ct-novelty-consumer 2>&1 | tail -n 40 | grep -q "starting ct-novelty-consumer"; then
    echo "FAIL: novelty did not restart with REQUIRE_DB=1" >&2
    docker logs ct-novelty-consumer 2>&1 | tail -n 40 >&2
    STATUS=1
    exit 1
  fi
  echo "ok: compose smoke REQUIRE_DB 0->1"
fi

echo "PASS: preflight smoke"
STATUS=0
