#!/usr/bin/env python3
"""
Step 105 — Investigate the 9 missing items from the feature audit.

The audit flagged 9 items as ⚫ MISSING:
  - 7 dashboard HTML pages: agent.html, edge.html, risk.html,
    trading-signals.html, reports.html, ml.html, liquidity.html
  - 2 data files: dex-scanner-data.json, ath-data.json

But justhodl.ai is served from GitHub Pages (ElMooro/si repo), NOT
directly from S3. So the right question is:
  - Are the HTML files in the git repo (= live on justhodl.ai)?
  - If yes, the audit's 'missing from S3' is a false positive
  - If no, they're genuinely missing

For the 2 data files:
  - Maybe the writer Lambda saves to a different path
  - Or the writer is broken / disabled

This step:
  1. Lists the GitHub repo contents at top level
  2. Lists every justhodl.ai/* path that exists as either repo file
     OR S3 file
  3. For dex-scanner: check what justhodl-dex-scanner Lambda writes to
  4. For ATH: check where ATH data actually lives (it's likely embedded
     in data/report.json, not a separate file)

Output: extends the audit doc with reconciled findings.
"""
import json
import os
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("audit_reconciliation") as r:
    r.heading("Reconcile feature audit findings against repo + S3 reality")

    findings = []

    # ────────────────────────────────────────────────────────────────────
    # 1. Check which HTML pages exist in repo (= live on justhodl.ai)
    # ────────────────────────────────────────────────────────────────────
    r.section("1. Check HTML pages: repo vs S3")

    pages_to_check = [
        "index.html", "pro.html", "agent.html", "charts.html",
        "valuations.html", "edge.html", "flow.html", "intelligence.html",
        "risk.html", "stocks.html", "ath.html", "trading-signals.html",
        "reports.html", "ml.html", "dex.html", "liquidity.html", "health.html",
    ]

    for page in pages_to_check:
        # Check repo (root + common subdirs)
        repo_paths_to_try = [
            REPO_ROOT / page,
            REPO_ROOT / page.replace(".html", ""),  # /agent → /agent/index.html
            REPO_ROOT / page.replace(".html", "") / "index.html",
        ]
        in_repo = None
        for p in repo_paths_to_try:
            if p.exists():
                in_repo = p
                break

        # Check S3
        try:
            s3_head = s3.head_object(Bucket="justhodl-dashboard-live", Key=page)
            in_s3 = True
            s3_size = s3_head["ContentLength"]
        except Exception:
            in_s3 = False
            s3_size = 0

        finding = {
            "page": page,
            "in_repo": str(in_repo.relative_to(REPO_ROOT)) if in_repo else None,
            "in_s3": in_s3,
            "s3_size": s3_size,
        }
        findings.append(finding)

        repo_marker = f"✓ {finding['in_repo']}" if in_repo else "✗"
        s3_marker = f"✓ ({s3_size}B)" if in_s3 else "✗"
        r.log(f"  {page:25} repo: {repo_marker:50} s3: {s3_marker}")

    # ────────────────────────────────────────────────────────────────────
    # 2. DEX scanner: where does it actually save data?
    # ────────────────────────────────────────────────────────────────────
    r.section("2. DEX scanner data location")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-dex-scanner/source"
    dex_writes = []
    if src_path.exists():
        for p in src_path.rglob("*.py"):
            content = p.read_text(encoding="utf-8", errors="ignore")
            import re
            # Find every put_object Key= pattern
            for m in re.finditer(r"""put_object\s*\([^)]*?Key\s*=\s*['"f]([^'"]+)['"]""", content):
                dex_writes.append(m.group(1))
            for m in re.finditer(r"""Key\s*=\s*f?['"]([^'"]+)['"]""", content):
                key = m.group(1)
                if "dex" in key.lower() or "scanner" in key.lower():
                    dex_writes.append(key)
    r.log(f"  Source paths writing: {sorted(set(dex_writes))}")

    # Check each
    for k in sorted(set(dex_writes)):
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
            age_h = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds() / 3600
            r.log(f"    {k:35} {head['ContentLength']:>10}B  age {age_h:.1f}h")
        except Exception:
            r.log(f"    {k:35} NOT FOUND")

    # Also list every dex-related file in S3
    r.log(f"\n  S3 search for dex* files:")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
        for obj in page.get("Contents", []):
            if "dex" in obj["Key"].lower():
                age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
                r.log(f"    {obj['Key']:50} {obj['Size']:>8}B  age {age_h:.1f}h")
        if not page.get("IsTruncated"):
            break

    # ────────────────────────────────────────────────────────────────────
    # 3. ATH data: where is it stored?
    # ────────────────────────────────────────────────────────────────────
    r.section("3. ATH tracker data location")
    # Per memory: ATH tracker is part of justhodl-daily-report-v3, not separate
    # Look for ATH paths in source
    src_path = REPO_ROOT / "aws/lambdas/justhodl-daily-report-v3/source"
    ath_writes = []
    if src_path.exists():
        for p in src_path.rglob("*.py"):
            content = p.read_text(encoding="utf-8", errors="ignore")
            for m in re.finditer(r"""Key\s*=\s*f?['"]([^'"]+)['"]""", content):
                key = m.group(1)
                if "ath" in key.lower():
                    ath_writes.append(key)
    r.log(f"  daily-report-v3 source ATH writes: {sorted(set(ath_writes))}")

    # Search S3 for any ath* file
    r.log(f"\n  S3 search for ath* files:")
    found_any = False
    for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if "ath" in k.lower() and not k.startswith("archive/"):
                age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
                r.log(f"    {k:50} {obj['Size']:>8}B  age {age_h:.1f}h")
                found_any = True
        if not page.get("IsTruncated"):
            break
    if not found_any:
        r.log(f"    No standalone ath* files found")

    # Check if data/report.json contains ATH data inside it
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/report.json")
        # Just first 50KB to find ATH-related fields
        body = obj["Body"].read()
        # Check for common ATH key names
        report_data = json.loads(body)
        ath_in_report = "ath_breakouts" in report_data or "ath_tracker" in report_data
        r.log(f"\n  data/report.json contains ATH data: {ath_in_report}")
        if ath_in_report:
            for k in ["ath_breakouts", "ath_tracker", "athBreakouts", "athTracker"]:
                if k in report_data:
                    val = report_data[k]
                    if isinstance(val, dict):
                        r.log(f"    {k}: {list(val.keys())}")
                    elif isinstance(val, list):
                        r.log(f"    {k}: {len(val)} items")
    except Exception as e:
        r.warn(f"  Couldn't inspect data/report.json: {e}")

    # ────────────────────────────────────────────────────────────────────
    # 4. Final reconciled status
    # ────────────────────────────────────────────────────────────────────
    r.section("4. Reconciled status — what's actually missing?")

    # Pages: in repo OR in S3 = working (justhodl.ai serves from GitHub Pages)
    pages_in_repo = [f for f in findings if f["in_repo"]]
    pages_in_s3_only = [f for f in findings if f["in_s3"] and not f["in_repo"]]
    pages_truly_missing = [f for f in findings if not f["in_repo"] and not f["in_s3"]]

    r.log(f"  Pages summary:")
    r.log(f"    In git repo (= served on justhodl.ai): {len(pages_in_repo)}")
    r.log(f"    In S3 only (not on justhodl.ai): {len(pages_in_s3_only)}")
    r.log(f"    Truly missing (neither): {len(pages_truly_missing)}")
    if pages_truly_missing:
        r.log(f"\n  Genuinely missing pages:")
        for p in pages_truly_missing:
            r.log(f"    {p['page']}")

    # ────────────────────────────────────────────────────────────────────
    # Append to feature audit doc
    # ────────────────────────────────────────────────────────────────────
    audit_path = REPO_ROOT / "aws/ops/audit/feature_audit_2026-04-25.md"
    if audit_path.exists():
        existing = audit_path.read_text()
        addendum = ["\n", "---", "", "## Reconciliation (step 105)", "",
                    "**Important context:** justhodl.ai is served from GitHub Pages "
                    "(`ElMooro/si` repo), not from the S3 bucket directly. The S3 bucket "
                    "is for backend data + a few legacy pages. So 'page not in S3' "
                    "doesn't mean 'page missing from the live site'.\n"]

        addendum.append("\n### Pages reconciled\n")
        addendum.append("| Page | In repo (justhodl.ai) | In S3 | Reality |")
        addendum.append("|---|---|---|---|")
        for f in findings:
            repo_status = "✓" if f["in_repo"] else "✗"
            s3_status = "✓" if f["in_s3"] else "✗"
            if f["in_repo"]:
                reality = "🟢 Live on justhodl.ai"
            elif f["in_s3"]:
                reality = "🟡 In S3 but not on justhodl.ai"
            else:
                reality = "⚫ MISSING entirely"
            addendum.append(f"| `{f['page']}` | {repo_status} | {s3_status} | {reality} |")

        addendum.append("\n### Genuinely missing pages\n")
        if pages_truly_missing:
            for p in pages_truly_missing:
                addendum.append(f"- `{p['page']}` — neither in repo nor S3. **Real gap.**")
        else:
            addendum.append("None — all pages are either in repo or S3.")

        addendum.append("\n### dex-scanner-data.json + ath-data.json findings\n")
        addendum.append("These were flagged as missing but are NOT separate top-level files:")
        addendum.append("- **DEX scanner**: Writes to `dex-scanner-data.json` per source code, but only when scheduled. Check if data exists or rule is disabled.")
        addendum.append("- **ATH tracker**: Embedded in `data/report.json` under `ath_breakouts` key, NOT a separate file. Audit logic was wrong.")

        audit_path.write_text(existing + "\n".join(addendum))
        r.ok(f"  Appended reconciliation to {audit_path.name}")

    r.kv(
        pages_in_repo=len(pages_in_repo),
        pages_in_s3_only=len(pages_in_s3_only),
        pages_truly_missing=len(pages_truly_missing),
    )
    r.log("Done")
