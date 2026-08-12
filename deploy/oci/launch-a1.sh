#!/usr/bin/env bash
# Launch Always Free Ampere A1 via OCI CLI (bypasses flaky console Create UI).
#
# Prerequisites:
#   brew install oci-cli
#   oci session authenticate --region us-phoenix-1
#     (or: oci setup config)
#   VCN must already have an Internet Gateway + default route (see ensure-igw.sh).
#
# Required env:
#   COMPARTMENT_ID   ocid1.compartment... (or tenancy OCID for root)
#   SUBNET_ID        ocid1.subnet...      (ct-firehose-public)
#   IMAGE_ID         ocid1.image...       (Oracle Linux 9 aarch64, Phoenix)
#   SSH_PUBLIC_KEY_FILE  path to .pub
#
# Optional:
#   AVAILABILITY_DOMAIN  e.g. gHNn:US-PHOENIX-1-AD-2
#   SKIP_USER_DATA=1     launch without cloud-init
#   BOOT_VOLUME_GB=200
#   INSTANCE_NAME=ct-firehose-filter-vm
#
# Lookup helpers (after auth):
#   oci iam availability-domain list --compartment-id "$COMPARTMENT_ID" --region us-phoenix-1
#   oci compute image list --compartment-id "$COMPARTMENT_ID" --operating-system "Oracle Linux" \
#        --operating-system-version "9" --shape VM.Standard.A1.Flex --region us-phoenix-1 \
#        --query "data[0].id" --raw-output

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CLOUD_INIT="${CLOUD_INIT:-$ROOT/deploy/oci/cloud-init.yaml}"
REGION="${REGION:-us-phoenix-1}"
INSTANCE_NAME="${INSTANCE_NAME:-ct-firehose-filter-vm}"
BOOT_VOLUME_GB="${BOOT_VOLUME_GB:-200}"
SHAPE="${SHAPE:-VM.Standard.A1.Flex}"
OCPUS="${OCPUS:-2}"
MEMORY_GB="${MEMORY_GB:-12}"
SKIP_USER_DATA="${SKIP_USER_DATA:-0}"
# Session auth (oci session authenticate) requires this; API-key profiles can override:
#   OCI_CLI_AUTH=api_key ./deploy/oci/launch-a1.sh
export OCI_CLI_AUTH="${OCI_CLI_AUTH:-security_token}"

die() { echo "ERROR: $*" >&2; exit 1; }

command -v oci >/dev/null || die "oci CLI not found. Install: brew install oci-cli"
[[ -n "${COMPARTMENT_ID:-}" ]] || die "set COMPARTMENT_ID"
[[ -n "${SUBNET_ID:-}" ]] || die "set SUBNET_ID"
[[ -n "${IMAGE_ID:-}" ]] || die "set IMAGE_ID"
[[ -f "${SSH_PUBLIC_KEY_FILE:-}" ]] || die "set SSH_PUBLIC_KEY_FILE to an existing .pub path"
[[ -f "$CLOUD_INIT" ]] || die "missing cloud-init: $CLOUD_INIT"

if [[ -z "${AVAILABILITY_DOMAIN:-}" ]]; then
  AVAILABILITY_DOMAIN=$(oci iam availability-domain list \
    --compartment-id "$COMPARTMENT_ID" \
    --region "$REGION" \
    --query 'data[0].name' \
    --raw-output)
fi

ARGS=(
  compute instance launch
  --region "$REGION"
  --compartment-id "$COMPARTMENT_ID"
  --availability-domain "$AVAILABILITY_DOMAIN"
  --display-name "$INSTANCE_NAME"
  --shape "$SHAPE"
  --shape-config "{\"ocpus\": ${OCPUS}, \"memoryInGBs\": ${MEMORY_GB}}"
  --subnet-id "$SUBNET_ID"
  --assign-public-ip true
  --image-id "$IMAGE_ID"
  --ssh-authorized-keys-file "$SSH_PUBLIC_KEY_FILE"
  --boot-volume-size-in-gbs "$BOOT_VOLUME_GB"
  --instance-options '{"areLegacyImdsEndpointsDisabled": true}'
)

if [[ "$SKIP_USER_DATA" != "1" ]]; then
  ARGS+=(--user-data-file "$CLOUD_INIT")
fi

echo "Launching $INSTANCE_NAME ($SHAPE ${OCPUS}OCPU/${MEMORY_GB}GB, boot ${BOOT_VOLUME_GB}G, AD=$AVAILABILITY_DOMAIN)..."
oci "${ARGS[@]}"
