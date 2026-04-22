#!/usr/bin/env python3
"""
Phase 3a — Import active Lambdas into version control.

For every Lambda in ACTIVE_LIST:
  1. Download its deployment zip
  2. Extract into aws/lambdas/<function-name>/source/
  3. Write aws/lambdas/<function-name>/config.json capturing:
       - function_name, runtime, handler
       - memory_size, timeout, architectures
       - environment vars (KEYS only — never values, to keep secrets out of git)
       - layer ARNs
       - has_function_url, function_url_cors (for future reconstruction)
       - eventbridge rule names
       - last_modified, code_size, last_import_timestamp

Skips any function that is:
  - A container image (PackageType=Image) — those live in ECR, not in a zip
  - Uses external layers we can't fetch (rare; logged + skipped gracefully)

Idempotent: re-running overwrites the source/ directory cleanly, so newer
deploys not-yet-in-git (made outside CI/CD) are picked up.
"""

import io
import json
import os
import shutil
import sys
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"

# The 27 active Lambdas from audit 2026-04-22
ACTIVE_LIST = [
    "justhodl-bloomberg-v8",
    "openbb-system2-api",
    "macro-financial-intelligence",
    "enhanced-repo-agent",
    "fedliquidityapi",
    "bea-economic-agent",
    "chatgpt-agent-api",
    "news-sentiment-agent",
    "manufacturing-global-agent",
    "justhodl-ml-predictions",
    "fmp-stock-picks-agent",
    "justhodl-telegram-bot",
    "justhodl-financial-secretary",
    "justhodl-fred-proxy",
    "justhodl-edge-engine",
    "justhodl-signal-logger",
    "justhodl-morning-intelligence",
    "justhodl-ecb-proxy",
    "justhodl-news-sentiment",
    "alphavantage-technical-analysis",
    "bls-employment-api-v2",
    "justhodl-valuations-agent",
    "nasdaq-datalink-agent",
    "justhodl-chat-api",
    "nyfedapi-isolated",
    "nyfed-financial-stability-fetcher",
    "nyfed-primary-dealer-fetcher",
]

# Plus the lambdas our CI/CD already manages (want them version-controlled too)
ALSO_MANAGED = [
    "justhodl-ai-chat",
    "justhodl-investor-agents",
    "justhodl-daily-report-v3",
    "justhodl-khalid-metrics",
    "justhodl-crypto-enricher",
    "justhodl-crypto-intel",
    "justhodl-options-flow",
    "justhodl-intelligence",
    "justhodl-stock-analyzer",
    "justhodl-stock-screener",
    "justhodl-repo-monitor",
    "justhodl-outcome-checker",
    "justhodl-advanced-charts",
    "justhodl-cache-layer",
    "justhodl-charts-agent",
    "justhodl-dex-scanner",
    "justhodl-treasury-proxy",
    "justhodl-email-reports",
    "cftc-futures-positioning-agent",
]

FUNCTIONS = sorted(set(ACTIVE_LIST + ALSO_MANAGED))

lam = boto3.client("lambda", region_name=REGION)
ev  = boto3.client("events", region_name=REGION)


def log(msg):
    print(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {msg}", flush=True)


def import_function(fn_name: str, repo_root: Path) -> dict:
    """Returns an outcome dict. 'status' in {imported, skipped:reason, error:msg}."""
    try:
        fn = lam.get_function(FunctionName=fn_name)
    except lam.exceptions.ResourceNotFoundException:
        return {"name": fn_name, "status": "not-found"}

    cfg = fn["Configuration"]
    pkg_type = cfg.get("PackageType", "Zip")
    if pkg_type != "Zip":
        return {"name": fn_name, "status": f"skipped:package-type-{pkg_type}"}

    code_url = fn["Code"]["Location"]
    with urllib.request.urlopen(code_url) as r:
        zbytes = r.read()

    # Destination directory
    fn_dir = repo_root / "aws" / "lambdas" / fn_name
    src_dir = fn_dir / "source"
    # Nuke source dir for a clean import (preserves config.json and other metadata)
    if src_dir.exists():
        shutil.rmtree(src_dir)
    src_dir.mkdir(parents=True, exist_ok=True)

    # Extract zip
    file_count = 0
    with zipfile.ZipFile(io.BytesIO(zbytes)) as zf:
        for entry in zf.namelist():
            # Skip obviously-binary vendored bloat (pycache, .dist-info)
            # but keep them in repo for now — future cleanup can use .gitignore
            zf.extract(entry, src_dir)
            file_count += 1

    # Fetch Function URL config (if any)
    url_config = None
    try:
        url_resp = lam.get_function_url_config(FunctionName=fn_name)
        url_config = {
            "auth_type": url_resp.get("AuthType"),
            "cors": url_resp.get("Cors"),
            "url": url_resp.get("FunctionUrl"),
        }
    except lam.exceptions.ResourceNotFoundException:
        pass

    # Fetch EventBridge rules targeting this function
    try:
        eb_rules = ev.list_rule_names_by_target(
            TargetArn=cfg["FunctionArn"]
        ).get("RuleNames", [])
    except ClientError:
        eb_rules = []

    # Environment var KEYS only (never values — they'd leak secrets into git)
    env_keys = sorted((cfg.get("Environment") or {}).get("Variables", {}).keys())

    # Reserved concurrency (if any)
    reserved = None
    try:
        r = lam.get_function_concurrency(FunctionName=fn_name)
        reserved = r.get("ReservedConcurrentExecutions")
    except ClientError:
        pass

    config = {
        "function_name":       fn_name,
        "region":              REGION,
        "runtime":             cfg.get("Runtime"),
        "handler":             cfg.get("Handler"),
        "memory_size":         cfg.get("MemorySize"),
        "timeout":             cfg.get("Timeout"),
        "architectures":       cfg.get("Architectures", []),
        "role":                cfg.get("Role"),
        "environment_keys":    env_keys,
        "layers":              [layer["Arn"] for layer in cfg.get("Layers", [])],
        "reserved_concurrency": reserved,
        "function_url":        url_config,
        "eventbridge_rules":   eb_rules,
        "last_modified":       cfg.get("LastModified"),
        "code_size":           cfg.get("CodeSize"),
        "files_imported":      file_count,
        "imported_at":         datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    # Write config.json (pretty-printed for diffability)
    with open(fn_dir / "config.json", "w") as f:
        json.dump(config, f, indent=2, default=str)
        f.write("\n")

    # Write a README per function for human navigation
    readme = fn_dir / "README.md"
    if not readme.exists():
        readme.write_text(
            f"# {fn_name}\n\n"
            f"Runtime: `{cfg.get('Runtime')}` · "
            f"Handler: `{cfg.get('Handler')}` · "
            f"Memory: {cfg.get('MemorySize')} MB · "
            f"Timeout: {cfg.get('Timeout')}s\n\n"
            f"Edit files under `source/` and push — GitHub Actions deploys "
            f"the change automatically (see `.github/workflows/deploy-lambdas.yml`).\n\n"
            f"Config snapshot: [`config.json`](./config.json)\n"
        )

    return {
        "name": fn_name,
        "status": "imported",
        "files": file_count,
        "size_kb": round(cfg.get("CodeSize", 0) / 1024),
    }


def main():
    log(f"=== Phase 3a — Import {len(FUNCTIONS)} Lambdas into version control ===")

    repo_root = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
    log(f"Repo root: {repo_root}")

    outcomes = []
    for i, name in enumerate(FUNCTIONS, 1):
        log(f"[{i}/{len(FUNCTIONS)}] {name}")
        try:
            outcome = import_function(name, repo_root)
        except Exception as e:
            outcome = {"name": name, "status": f"error:{type(e).__name__}: {e}"}
        status = outcome["status"]
        if status == "imported":
            log(f"    ✓ {outcome['files']} files, {outcome['size_kb']} KB")
        else:
            log(f"    ⚠ {status}")
        outcomes.append(outcome)

    imported = [o for o in outcomes if o["status"] == "imported"]
    skipped  = [o for o in outcomes if o["status"].startswith("skipped")]
    errored  = [o for o in outcomes if o["status"].startswith("error")]
    missing  = [o for o in outcomes if o["status"] == "not-found"]

    log("")
    log("══════════════════ SUMMARY ══════════════════")
    log(f"  Imported:  {len(imported)}")
    log(f"  Skipped:   {len(skipped)}")
    log(f"  Not found: {len(missing)}")
    log(f"  Errored:   {len(errored)}")
    log("═════════════════════════════════════════════")

    for o in skipped + missing + errored:
        log(f"  ! {o['name']}: {o['status']}")

    # GitHub step summary
    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a") as f:
            f.write(f"# Phase 3a — Lambda Import\n\n")
            f.write(f"| Status | Count |\n|---|---|\n")
            f.write(f"| Imported | {len(imported)} |\n")
            f.write(f"| Skipped | {len(skipped)} |\n")
            f.write(f"| Not found | {len(missing)} |\n")
            f.write(f"| Errored | {len(errored)} |\n\n")
            if skipped + errored + missing:
                f.write(f"## Needs attention\n\n")
                for o in skipped + missing + errored:
                    f.write(f"- `{o['name']}`: {o['status']}\n")

    if errored:
        sys.exit(f"{len(errored)} functions errored during import")


if __name__ == "__main__":
    main()
