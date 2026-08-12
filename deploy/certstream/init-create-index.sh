#!/bin/sh
# First-boot helper for compose: seed ct_index.json at current log heads so the
# sidecar does not replay historic CT at thousands of certs/s.
set -eu
CONFIG="${CERTSTREAM_CONFIG:-/etc/certstream/config.yml}"
INDEX="${CT_INDEX_FILE:-/data/ct_index.json}"

if [ -f "$INDEX" ]; then
  echo "certstream-init: $INDEX exists; skipping create-index"
  exit 0
fi

echo "certstream-init: seeding $INDEX at current log heads"
exec /app/certstream-server-go create-index -c "$CONFIG"
