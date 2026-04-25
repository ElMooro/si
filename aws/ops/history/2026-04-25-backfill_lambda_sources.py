#!/usr/bin/env python3
"""
Step 88 — Backfill all missing Lambda sources into version control.

For every Lambda in the account NOT already in aws/lambdas/<n>/source/:
  1. Get function code URL via lambda.get_function
  2. Download the zip via urllib (the URL is a presigned S3 link)
  3. Extract into aws/lambdas/<n>/source/
  4. Save a config.json next to it with metadata (runtime, handler,
     memory, timeout, env keys, last_modified, role) — DOES NOT save
     env values themselves to keep secrets out of git
  5. CI auto-commit will pick everything up

Skip:
  - Lambdas already in repo (don't overwrite — those are sources of truth)
  - Lambdas with code packages > 50MB (likely contain large deps; pull
    just the metadata, log a warning)

Outputs a summary report listing pulled / skipped / failed.
"""
import io
import json
import os
import shutil
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
LAMBDAS_DIR = REPO_ROOT / "aws/lambdas"
LAMBDAS_DIR.mkdir(parents=True, exist_ok=True)

MAX_PACKAGE_BYTES = 50 * 1024 * 1024  # 50MB cap

lam = boto3.client("lambda", region_name=REGION)


def list_all_lambdas():
    out = []
    paginator = lam.get_paginator("list_functions")
    for page in paginator.paginate():
        out.extend(page.get("Functions", []))
    return out


def safe_extract_zip(zip_bytes, target_dir):
    """Extract zip safely (no path traversal). Returns list of files written."""
    files_written = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for member in z.namelist():
            # Reject anything that would escape target_dir
            p = Path(member)
            if p.is_absolute() or ".." in p.parts:
                print(f"[SKIP-MEMBER] suspicious path: {member}")
                continue
            # Reject __pycache__ + .pyc + .git
            if "__pycache__" in p.parts or member.endswith(".pyc") or ".git" in p.parts:
                continue
            dest = target_dir / member
            if member.endswith("/"):
                dest.mkdir(parents=True, exist_ok=True)
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            with z.open(member) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst)
            files_written.append(member)
    return files_written


with report("backfill_lambda_sources") as r:
    r.heading("Backfill missing Lambda sources from AWS")

    all_fns = list_all_lambdas()
    r.log(f"  Total Lambdas in account: {len(all_fns)}")

    existing = set()
    for d in LAMBDAS_DIR.iterdir():
        if d.is_dir() and (d / "source").exists():
            existing.add(d.name)
    r.log(f"  Already in repo: {len(existing)}")

    missing = [f for f in all_fns if f["FunctionName"] not in existing]
    r.log(f"  Missing (to backfill): {len(missing)}\n")

    pulled = []
    skipped = []
    failed = []
    too_large = []
    config_only = []  # Pulled metadata but not source

    for i, fn in enumerate(missing, 1):
        name = fn["FunctionName"]
        r.log(f"  [{i:>2}/{len(missing)}] {name}")
        try:
            # 1. Get code URL
            detail = lam.get_function(FunctionName=name)
            code = detail.get("Code", {})
            location = code.get("Location")
            repo_type = code.get("RepositoryType", "S3")  # Could be "ECR" for image-based
            cfg = detail.get("Configuration", {})

            # Save config.json regardless (always useful)
            target_dir = LAMBDAS_DIR / name
            target_dir.mkdir(exist_ok=True)
            config = {
                "name": name,
                "runtime": cfg.get("Runtime"),
                "handler": cfg.get("Handler"),
                "memory_mb": cfg.get("MemorySize"),
                "timeout_s": cfg.get("Timeout"),
                "code_size_bytes": cfg.get("CodeSize"),
                "package_type": cfg.get("PackageType", "Zip"),
                "last_modified": cfg.get("LastModified"),
                "role_arn_tail": (cfg.get("Role") or "")[-60:],
                "description": (cfg.get("Description") or "")[:500],
                "env_keys": sorted(list((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())),
                "layers": [l.get("Arn", "")[-80:] for l in (cfg.get("Layers", []) or [])],
                "architectures": cfg.get("Architectures", []),
                "backfilled_at": datetime.now(timezone.utc).isoformat(),
            }
            (target_dir / "config.json").write_text(json.dumps(config, indent=2))

            # If it's a container image (ECR), can't download source
            if cfg.get("PackageType") == "Image":
                config_only.append(name)
                r.log(f"      → image-based, config-only")
                continue

            if not location:
                config_only.append(name)
                r.log(f"      → no code URL, config-only")
                continue

            code_size = cfg.get("CodeSize", 0)
            if code_size > MAX_PACKAGE_BYTES:
                too_large.append((name, code_size))
                r.log(f"      → package {code_size/1024/1024:.1f}MB > 50MB cap, config-only")
                continue

            # 2. Download zip
            with urllib.request.urlopen(location, timeout=60) as resp:
                zbytes = resp.read()
            r.log(f"      downloaded {len(zbytes)/1024:.0f}KB")

            # 3. Extract
            source_dir = target_dir / "source"
            source_dir.mkdir(exist_ok=True)
            files = safe_extract_zip(zbytes, source_dir)
            pulled.append((name, len(files), len(zbytes)))
            r.log(f"      → {len(files)} files extracted")

        except Exception as e:
            failed.append((name, str(e)[:200]))
            r.log(f"      → FAILED: {str(e)[:200]}")

    # Summary
    r.section("Summary")
    r.log(f"  Pulled (full source):  {len(pulled)}")
    r.log(f"  Config-only:           {len(config_only)} (image-based or no URL)")
    r.log(f"  Too large:             {len(too_large)}")
    r.log(f"  Failed:                {len(failed)}")

    if pulled:
        r.log(f"\n  Top 10 pulled by file count:")
        for name, fcount, zsize in sorted(pulled, key=lambda x: -x[1])[:10]:
            r.log(f"    {name:50} {fcount:>4} files  {zsize/1024:>8.0f}KB")

    if too_large:
        r.log(f"\n  Too large (review separately):")
        for name, size in too_large:
            r.log(f"    {name:50} {size/1024/1024:>6.1f}MB")

    if config_only:
        r.log(f"\n  Config-only:")
        for name in config_only[:20]:
            r.log(f"    {name}")

    if failed:
        r.log(f"\n  Failed:")
        for name, err in failed[:20]:
            r.log(f"    {name}: {err}")

    r.kv(
        total_lambdas=len(all_fns),
        already_in_repo=len(existing),
        pulled_full_source=len(pulled),
        pulled_config_only=len(config_only),
        too_large=len(too_large),
        failed=len(failed),
    )
    r.log("Done")
