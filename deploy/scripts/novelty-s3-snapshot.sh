#!/usr/bin/env bash
# Snapshot novelty SQLite to S3 (shoestring flood insurance).
# Usage: novelty-s3-snapshot.sh [/path/to/novelty.db] [s3://bucket/key]
set -euo pipefail

DB="${1:-${NOVELTY_DB:-/var/lib/ct-firehose-filter/novelty.db}}"
URI="${2:-${NOVELTY_S3_URI:-}}"
REGION="${AWS_REGION:-us-west-2}"

if [[ -z "$URI" ]]; then
  echo "set NOVELTY_S3_URI or pass s3://bucket/key" >&2
  exit 1
fi
if [[ ! -f "$DB" ]]; then
  echo "novelty DB missing: $DB" >&2
  exit 1
fi

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
SNAP="$TMP/novelty.db"

# Consistent copy even if WAL is present (sqlite3 recommended when available).
if command -v sqlite3 >/dev/null 2>&1; then
  sqlite3 "$DB" ".backup '$SNAP'"
else
  echo "sqlite3 not installed; copying DB file only (stop writer first if possible)" >&2
  cp -f "$DB" "$SNAP"
  [[ -f "${DB}-wal" ]] && cp -f "${DB}-wal" "${SNAP}-wal" || true
  [[ -f "${DB}-shm" ]] && cp -f "${DB}-shm" "${SNAP}-shm" || true
fi

aws s3 cp "$SNAP" "$URI" --region "$REGION"
echo "uploaded $DB -> $URI"
