#!/usr/bin/env bash
# Restore novelty SQLite from S3 before starting the consumer (avoid cold flood).
# Usage: novelty-s3-restore.sh [s3://bucket/key] [/path/to/novelty.db]
set -euo pipefail

URI="${1:-${NOVELTY_S3_URI:-}}"
DB="${2:-${NOVELTY_DB:-/var/lib/ct-firehose-filter/novelty.db}}"
REGION="${AWS_REGION:-us-west-2}"

if [[ -z "$URI" ]]; then
  echo "set NOVELTY_S3_URI or pass s3://bucket/key" >&2
  exit 1
fi

mkdir -p "$(dirname "$DB")"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

aws s3 cp "$URI" "$TMP" --region "$REGION"
# Atomic replace
mv -f "$TMP" "$DB"
# Drop stale WAL/SHM from a previous crashed process
rm -f "${DB}-wal" "${DB}-shm"
echo "restored $URI -> $DB"
echo "safe to start novelty with NOVELTY_REQUIRE_DB=1"
