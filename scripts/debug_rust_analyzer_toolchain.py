#!/usr/bin/env python3
"""Reproduce helper: capture rust-analyzer / toolchain state for debug session b047c4."""
from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

LOG = Path(__file__).resolve().parents[1] / ".cursor" / "debug-b047c4.log"
ROOT = Path(__file__).resolve().parents[1]


def sh(cmd: list[str]) -> str:
    try:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, cwd=ROOT).strip()
    except subprocess.CalledProcessError as e:
        return f"EXIT {e.returncode}: {e.output.strip()}"


def main() -> None:
    toml = (ROOT / "rust-toolchain.toml").read_text()
    comps = sh(["rustup", "component", "list", "--installed", "--toolchain", "1.91.1"])
    active = sh(["rustup", "show", "active-toolchain"])
    ra_tool = sh(["rustup", "run", "1.91.1", "rust-analyzer", "--version"])
    ra_path = sh(["rustup", "which", "rust-analyzer"])
    path_ra = sh(["which", "rust-analyzer"])
    path_ver = sh(["rust-analyzer", "--version"])
    vscode = ROOT / ".vscode" / "settings.json"
    run_id = "post-fix" if vscode.exists() else "ra-repro-1"

    def emit(hypothesis_id: str, message: str, data: dict) -> None:
        LOG.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "sessionId": "b047c4",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": "scripts/debug_rust_analyzer_toolchain.py",
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with LOG.open("a") as f:
            f.write(json.dumps(entry) + "\n")

    emit(
        "A",
        "component install check",
        {"has_ra_component": "rust-analyzer" in comps, "installed_lines": [l for l in comps.splitlines() if "analyzer" in l]},
    )
    emit(
        "B",
        "which binary would IDE PATH pick vs rustup",
        {"path_which": path_ra, "rustup_which": ra_path, "path_version": path_ver, "toolchain_version": ra_tool},
    )
    emit(
        "C",
        "rust-toolchain.toml contents",
        {"toml": toml, "lists_rust_analyzer": "rust-analyzer" in toml},
    )
    emit("D", "active toolchain", {"active": active})
    emit(
        "E",
        "workspace forces rust-analyzer.server.path?",
        {"settings_exists": vscode.exists(), "settings": vscode.read_text() if vscode.exists() else None},
    )
    emit(
        "B",
        "post-fix PATH rust-analyzer resolves to toolchain pin",
        {
            "path_version": path_ver,
            "matches_pin": "1.91.1" in path_ver,
        },
    )
    print(f"Wrote diagnostics to {LOG} runId={run_id}")


if __name__ == "__main__":
    main()
