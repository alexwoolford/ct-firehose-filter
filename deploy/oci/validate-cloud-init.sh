#!/usr/bin/env bash
# Local checks for cloud-init before OCI launch (no oci CLI required).
# Usage: ./deploy/oci/validate-cloud-init.sh [path/to/cloud-init.yaml]
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
CLOUD_INIT="${1:-$ROOT/deploy/oci/cloud-init.yaml}"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

python3 - "$CLOUD_INIT" "$TMPDIR" <<'PY'
import json, re, sys, base64
from pathlib import Path

path = Path(sys.argv[1])
tmpdir = Path(sys.argv[2])
raw = path.read_bytes()
text = raw.decode("utf-8")
lines = text.splitlines()
first = lines[0] if lines else ""
b64 = base64.b64encode(raw)
issues = []

if not first.startswith("#cloud-config"):
    issues.append("missing_#cloud-config_first_line")
if b"\x00" in raw:
    issues.append("contains_null_bytes")
if len(b64) > 16_000:
    issues.append("base64_unusually_large")
if "\t" in text:
    issues.append("contains_tabs")

# Fail closed on secret-looking material in user-data.
secret_patterns = [
    (r"\bAKIA[0-9A-Z]{16}\b", "aws_access_key_id_literal"),
    (r"\bAWS_SECRET_ACCESS_KEY\s*=", "aws_secret_assignment"),
    (r"\bAWS_ACCESS_KEY_ID\s*=", "aws_access_key_assignment"),
    (r"\bAWS_SESSION_TOKEN\s*=", "aws_session_token_assignment"),
    (r"\bocid1\.[a-z0-9.]+\.[a-z0-9]+\.", "oci_ocid_literal"),
    (r"\bSQS_QUEUE_URL\s*=\s*https?://", "sqs_url_assignment"),
    (r"-----BEGIN (RSA |OPENSSH |EC )?PRIVATE KEY-----", "private_key_block"),
    (r"\b[A-Za-z0-9/+=]{40}\b.*aws_secret", "aws_secret_like"),
]
for pat, name in secret_patterns:
    if re.search(pat, text, re.IGNORECASE):
        issues.append(f"secret_pattern:{name}")

# Extract embedded bootstrap script (write_files content under ct-firehose-bootstrap.sh).
bootstrap = tmpdir / "ct-firehose-bootstrap.sh"
in_bootstrap = False
script_lines = []
indent = None
for line in lines:
    if "path: /usr/local/sbin/ct-firehose-bootstrap.sh" in line:
        in_bootstrap = True
        script_lines = []
        indent = None
        continue
    if in_bootstrap:
        if line.strip().startswith("content: |"):
            continue
        if indent is None:
            if not line.strip():
                continue
            # First content line: measure leading spaces of YAML block
            stripped = line.lstrip(" ")
            indent = len(line) - len(stripped)
            if indent < 2:
                issues.append("bootstrap_content_indent_unexpected")
                break
        leading = len(line) - len(line.lstrip(" "))
        if line.strip() and leading < indent and not line.strip().startswith("#"):
            # Dedented to next YAML key
            in_bootstrap = False
            break
        if leading >= indent:
            script_lines.append(line[indent:] if len(line) >= indent else "")
        elif not line.strip():
            script_lines.append("")

if not script_lines:
    issues.append("bootstrap_script_not_extracted")
else:
    bootstrap.write_text("\n".join(script_lines) + "\n", encoding="utf-8")

data = {
    "path": str(path),
    "bytes": len(raw),
    "base64_len": len(b64),
    "first_line": first,
    "line_count": len(lines),
    "bootstrap_extracted": bootstrap.is_file() and bootstrap.stat().st_size > 0,
    "bootstrap_path": str(bootstrap) if bootstrap.is_file() else None,
    "issues": issues,
    "ok": len(issues) == 0,
}
print(json.dumps(data, indent=2))
sys.exit(0 if not issues else 1)
PY

BOOTSTRAP="$TMPDIR/ct-firehose-bootstrap.sh"
if [[ -f "$BOOTSTRAP" && -s "$BOOTSTRAP" ]]; then
  bash -n "$BOOTSTRAP"
  if command -v shellcheck >/dev/null 2>&1; then
    shellcheck -e SC2086,SC2164 "$BOOTSTRAP" || {
      echo "shellcheck reported issues in embedded bootstrap" >&2
      exit 1
    }
  else
    echo "note: shellcheck not installed; bash -n only" >&2
  fi
else
  echo "ERROR: failed to extract bootstrap script" >&2
  exit 1
fi

echo "cloud-init validation OK: $CLOUD_INIT"
