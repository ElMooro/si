#!/usr/bin/env python3
"""
Pull source of 4 un-versioned Lambdas into version control.

Lambdas in AWS but not in repo:
  - justhodl-calibrator (the heart of the learning loop)
  - MLPredictor
  - FinancialIntelligence-Backend
  - permanent-market-intelligence

For each, download the zip, extract, save to aws/lambdas/<name>/source/.
Also save the live config (env keys, schedule, etc) to config.json.

Read-only with respect to AWS — only writes to local repo.
"""
import io
import json
import os
import urllib.request
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)

TARGETS = [
    "justhodl-calibrator",
    "MLPredictor",
    "FinancialIntelligence-Backend",
    "permanent-market-intelligence",
]


with report("pull_unversioned_lambdas") as r:
    r.heading("Pull source of 4 un-versioned Lambdas into repo")

    for fn_name in TARGETS:
        r.section(f"Pulling {fn_name}")
        try:
            # Get the zip URL
            resp = lam.get_function(FunctionName=fn_name)
            zip_url = resp["Code"]["Location"]
            cfg = resp["Configuration"]

            # Download zip
            with urllib.request.urlopen(zip_url, timeout=30) as zr:
                zip_bytes = zr.read()
            r.log(f"  Downloaded zip: {len(zip_bytes):,} bytes")

            # Extract
            target_dir = REPO_ROOT / f"aws/lambdas/{fn_name}/source"
            target_dir.mkdir(parents=True, exist_ok=True)

            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                file_count = 0
                py_count = 0
                for name in zf.namelist():
                    # Strip dependency dirs to avoid bloat
                    if any(x in name for x in [".dist-info", "__pycache__", "boto3/", "botocore/", "urllib3/", "dateutil/", "jmespath/", "s3transfer/"]):
                        continue
                    out_path = target_dir / name
                    out_path.parent.mkdir(parents=True, exist_ok=True)
                    if name.endswith("/"):
                        continue
                    with zf.open(name) as src, open(out_path, "wb") as dst:
                        dst.write(src.read())
                    file_count += 1
                    if name.endswith(".py"):
                        py_count += 1
            r.log(f"  Extracted {file_count} files ({py_count} .py)")

            # Save config
            config = {
                "function_name": cfg["FunctionName"],
                "runtime": cfg.get("Runtime"),
                "handler": cfg.get("Handler"),
                "memory_mb": cfg.get("MemorySize"),
                "timeout_s": cfg.get("Timeout"),
                "last_modified": cfg.get("LastModified"),
                "role": cfg.get("Role"),
                "environment_keys": sorted(list((cfg.get("Environment", {}) or {}).get("Variables", {}).keys())),
                "code_size": cfg.get("CodeSize"),
                "description": cfg.get("Description", ""),
            }
            (target_dir.parent / "config.json").write_text(
                json.dumps(config, indent=2, default=str)
            )

            r.ok(f"  Saved to aws/lambdas/{fn_name}/")

            # Show the main source file for reading
            main_file = target_dir / "lambda_function.py"
            if main_file.exists():
                r.log(f"  Main source: {main_file.stat().st_size:,} bytes")

        except Exception as e:
            r.fail(f"  Failed: {e}")

    r.log("Done — next step: read what's in calibrator + ml-predictor")
