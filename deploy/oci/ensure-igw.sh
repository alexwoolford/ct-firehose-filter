#!/usr/bin/env bash
# Ensure VCN has an Internet Gateway + default route (required for cloud-init/dnf).
# Usage:
#   export OCI_CLI_AUTH=security_token
#   export COMPARTMENT_ID=...
#   export VCN_ID=...   # or SUBNET_ID=... (VCN inferred)
#   ./deploy/oci/ensure-igw.sh
set -euo pipefail

export OCI_CLI_AUTH="${OCI_CLI_AUTH:-security_token}"
REGION="${REGION:-us-phoenix-1}"

die() { echo "ERROR: $*" >&2; exit 1; }
[[ -n "${COMPARTMENT_ID:-}" ]] || die "set COMPARTMENT_ID"

if [[ -z "${VCN_ID:-}" ]]; then
  [[ -n "${SUBNET_ID:-}" ]] || die "set VCN_ID or SUBNET_ID"
  VCN_ID=$(oci network subnet get --subnet-id "$SUBNET_ID" --query 'data."vcn-id"' --raw-output)
fi

RT_ID=$(oci network subnet list --compartment-id "$COMPARTMENT_ID" --vcn-id "$VCN_ID" \
  --query 'data[0]."route-table-id"' --raw-output)
# Prefer route table attached to named public subnet if SUBNET_ID set
if [[ -n "${SUBNET_ID:-}" ]]; then
  RT_ID=$(oci network subnet get --subnet-id "$SUBNET_ID" --query 'data."route-table-id"' --raw-output)
fi

IGW=$(oci network internet-gateway list --compartment-id "$COMPARTMENT_ID" --vcn-id "$VCN_ID" \
  --region "$REGION" --query 'data[0].id' --raw-output 2>/dev/null || true)
if [[ -z "$IGW" || "$IGW" == "null" || "$IGW" == "None" ]]; then
  echo "Creating Internet Gateway..."
  IGW=$(oci network internet-gateway create \
    --compartment-id "$COMPARTMENT_ID" \
    --vcn-id "$VCN_ID" \
    --is-enabled true \
    --display-name ct-firehose-igw \
    --query 'data.id' --raw-output)
fi
echo "IGW=$IGW"

# Merge default route (replace empty or ensure 0.0.0.0/0 present)
echo "Updating route table $RT_ID ..."
oci network route-table update --rt-id "$RT_ID" --force \
  --route-rules "[{\"cidrBlock\":\"0.0.0.0/0\",\"networkEntityId\":\"$IGW\",\"description\":\"default via IGW\"}]" \
  --query 'data."route-rules"' --output table

echo "OK: default route 0.0.0.0/0 -> IGW"
