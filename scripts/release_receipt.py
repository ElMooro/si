#!/usr/bin/env python3
"""Publish a release receipt to S3 after a verified Lambda deploy (deploy lane v2).

A green workflow is not proof that AWS runs main's bytes. The deploy transaction
now (1) compares the live CodeSha256 to the zip it built and (2) calls this
script to publish what was proven, so every lane can verify over plain HTTPS:

  data/ops/releases/<function>.json                 latest receipt
  data/ops/releases/history/<function>/<utc>.json   append-only history

Usage (from scripts/deploy_lambdas.sh):
  python3 scripts/release_receipt.py <function> <zip> <source_dir> <code_sha256>

Env: DEPLOY_COMMIT, DEPLOY_RUN_ID, DEPLOY_WORKFLOW, DEPLOY_ACTOR, RELEASE_BUCKET
(default justhodl-dashboard-live). Prints the receipt JSON; exits non-zero only
when the receipt cannot be built -- the S3 write reports its own failure so the
caller can decide (the deploy itself has already succeeded at this point).
"""
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path

BUCKET = os.environ.get("RELEASE_BUCKET", "justhodl-dashboard-live")
PREFIX = "data/ops/releases"


def source_inventory(source_dir: Path) -> dict:
    inv = {}
    for p in sorted(source_dir.rglob("*")):
        if p.is_file() and "__pycache__" not in p.parts:
            data = p.read_bytes()
            inv[str(p.relative_to(source_dir))] = {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}
    return inv


def build(function: str, zip_path: Path, source_dir: Path, code_sha256: str) -> dict:
    commit = os.environ.get("DEPLOY_COMMIT") or subprocess.run(
        ["git", "rev-parse", "HEAD"], text=True, capture_output=True).stdout.strip()
    run_id = os.environ.get("DEPLOY_RUN_ID", "")
    repo = os.environ.get("GITHUB_REPOSITORY", "ElMooro/si")
    zip_bytes = zip_path.read_bytes()
    return {
        "schema": "release-receipt.v1",
        "function": function,
        "commit": commit,
        "workflow": os.environ.get("DEPLOY_WORKFLOW", "deploy-lambdas.yml"),
        "run_id": run_id,
        "run_url": f"https://github.com/{repo}/actions/runs/{run_id}" if run_id else None,
        "actor": os.environ.get("DEPLOY_ACTOR", ""),
        "code_sha256": code_sha256,
        "zip_bytes": len(zip_bytes),
        "zip_sha256_hex": hashlib.sha256(zip_bytes).hexdigest(),
        "source": source_inventory(source_dir),
        "deployed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "verified": True,
    }


def publish(receipt: dict) -> list[str]:
    import boto3  # runner has boto3 from the preflight step
    s3 = boto3.client("s3")
    body = (json.dumps(receipt, indent=1, sort_keys=True) + "\n").encode()
    stamp = receipt["deployed_at"].replace(":", "").replace("-", "")
    keys = [f"{PREFIX}/{receipt['function']}.json",
            f"{PREFIX}/history/{receipt['function']}/{stamp}.json"]
    for key in keys:
        s3.put_object(Bucket=BUCKET, Key=key, Body=body, ContentType="application/json",
                      CacheControl="no-cache, max-age=60")
    return keys


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        print("usage: release_receipt.py <function> <zip> <source_dir> <code_sha256>", file=sys.stderr)
        return 2
    function, zip_path, source_dir, code_sha256 = argv
    receipt = build(function, Path(zip_path), Path(source_dir), code_sha256)
    print(json.dumps({k: receipt[k] for k in ("function", "commit", "code_sha256", "zip_bytes", "deployed_at")}))
    try:
        keys = publish(receipt)
        print("release_receipt: published " + ", ".join(f"s3://{BUCKET}/{k}" for k in keys))
    except Exception as exc:  # noqa: BLE001 - surface, never mask a finished deploy
        print(f"::warning::release_receipt: S3 publish failed for {function}: {type(exc).__name__}: {exc}")
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
