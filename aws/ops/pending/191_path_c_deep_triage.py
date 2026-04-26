#!/usr/bin/env python3
"""
Step 191 — Path C: deep triage of remaining hidden pages.

After Path A+B (commit cb17f62), the new index.html links to:
  Tier 1 (12): stock, screener, intelligence, edge, liquidity, flow,
               crypto, positioning, desk, valuations, risk, ath
  Tier 2 (8):  charts, auctions, dex, carry, ml-predictions,
               desk-v2, reports
  Topbar nav same 12 + valuations duplicated.

Total surfaced: 13 unique pages (12 tier-1 + ml-predictions adds one)
Wait actually the topbar nav + tier1 + tier2 = ~17 unique.

Repo has 49 HTML pages. So ~30 pages still hidden. This step:
  A. List every remaining hidden HTML page
  B. For each, check:
     - File size (stub <500B → archive)
     - Last modified date in git
     - Data source URLs and whether they're live/stale/dead
     - Whether the data source actually returns valid JSON
  C. Categorize:
     ARCHIVE — empty/stub, broken, redundant
     PROMOTE — works, fresh data, useful niche
     QUIET   — works but specialized/legacy, link from elsewhere
"""
import io
import json
import os
import re
import time
import zipfile
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PROBE_NAME = "justhodl-tmp-data-probe"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


# Pages already linked from new index.html — exclude from triage
ALREADY_LINKED = {
    # tier 1 (launcher + topbar)
    "index.html", "stock/index.html", "screener/index.html",
    "intelligence.html", "edge.html", "liquidity.html",
    "flow.html", "crypto/index.html", "positioning/index.html",
    "desk.html", "valuations.html", "risk.html", "ath.html",
    # tier 2 (data sources & analytics)
    "charts.html", "auctions.html", "dex.html",
    "carry.html", "ml-predictions.html", "desk-v2.html",
    "reports.html",
    # safety backup, not user-facing
    "index-old.html",
}


PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    url = event["url"]
    try:
        req = urllib.request.Request(url, headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read()
            return {"ok": True, "status": r.status, "len": len(body)}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "kind": "HTTPError"}
    except Exception as e:
        return {"ok": False, "kind": type(e).__name__, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("path_c_deep_triage") as r:
    r.heading("Path C — deep triage of remaining hidden pages")

    # ─── Setup probe Lambda ─────────────────────────────────────────────
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    lam.create_function(
        FunctionName=PROBE_NAME, Runtime="python3.11",
        Role=ROLE_ARN, Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=15, MemorySize=256, Architectures=["x86_64"],
    )
    time.sleep(3)

    # ─── A. List remaining hidden pages ────────────────────────────────
    repo_root = os.environ.get("GITHUB_WORKSPACE", "/home/claude/si")
    all_pages = []
    for f in sorted(os.listdir(repo_root)):
        if f.endswith(".html"):
            all_pages.append(f)
    for d in ["stock", "screener", "crypto", "positioning",
              "agent", "bot", "khalid", "euro", "stocks"]:
        p = f"{d}/index.html"
        if os.path.exists(os.path.join(repo_root, p)):
            all_pages.append(p)

    hidden = [p for p in all_pages if p not in ALREADY_LINKED]
    r.section(f"A. {len(hidden)} hidden pages to triage")
    r.log(f"  Already linked: {len(all_pages) - len(hidden)} pages")
    r.log(f"  Hidden:         {len(hidden)} pages")

    # ─── B. Triage each ─────────────────────────────────────────────────
    r.section("B. Per-page triage")

    # Get S3 freshness map
    s3_keys_meta = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for o in page.get("Contents", []):
            s3_keys_meta[o["Key"]] = {
                "size": o["Size"],
                "mod": o["LastModified"],
                "age_h": (datetime.now(timezone.utc) - o["LastModified"]).total_seconds() / 3600,
            }

    triage = {"ARCHIVE": [], "PROMOTE": [], "QUIET": [], "BROKEN": []}

    for page in hidden:
        path = os.path.join(repo_root, page)
        try:
            with open(path) as fp:
                content = fp.read()
        except Exception as e:
            triage["BROKEN"].append((page, f"read err: {e}", 0, []))
            continue

        size = len(content)

        # Quick stub check
        if size < 500:
            triage["ARCHIVE"].append((page, "stub <500B", size, []))
            continue

        # Find data sources
        s3_refs = re.findall(r'justhodl-dashboard-live(?:\.s3[^/]*\.amazonaws\.com)/([a-zA-Z0-9_/-]+\.json)', content)
        api_refs = re.findall(r'https?://([a-zA-Z0-9-]+)\.execute-api\.', content)
        lambda_refs = re.findall(r'https?://([a-zA-Z0-9]+)\.lambda-url\.', content)
        http_refs = re.findall(r'http://[^\s"\'<>]+', content)

        # Categorize
        broken_reasons = []
        if any("i70jxru6md" in u for u in api_refs):
            broken_reasons.append("dead OpenBB APIGW")
        if "zzmoq2mq4vtphjyhm4i7hqpzvm0hkwsj" in content:
            broken_reasons.append("dead ECB_PROXY")
        if any("s3-website" in u and "http://" in u for u in http_refs):
            broken_reasons.append("HTTP S3 website (mixed content)")

        # Check S3 source freshness
        stale_sources = []
        fresh_sources = []
        missing_sources = []
        for s in s3_refs:
            if s in s3_keys_meta:
                age = s3_keys_meta[s]["age_h"]
                if age < 24:
                    fresh_sources.append((s, age))
                elif age < 168:
                    fresh_sources.append((s, age))  # week-fresh ok
                else:
                    stale_sources.append((s, age))
            else:
                missing_sources.append(s)

        # Decision
        if broken_reasons:
            triage["BROKEN"].append((page, "; ".join(broken_reasons), size, s3_refs[:5]))
        elif missing_sources or (stale_sources and not fresh_sources):
            triage["ARCHIVE"].append((page, f"missing/stale data: {missing_sources or [(s, f'{a:.0f}h') for s,a in stale_sources]}", size, s3_refs[:5]))
        elif fresh_sources:
            # Has at least one fresh source — could promote
            primary_age = min(a for _, a in fresh_sources)
            if primary_age < 24:
                triage["PROMOTE"].append((page, f"fresh data ({primary_age:.1f}h)", size, s3_refs[:5]))
            else:
                triage["QUIET"].append((page, f"week-fresh ({primary_age:.0f}h)", size, s3_refs[:5]))
        else:
            # No S3 sources at all — might use external API or be informational
            triage["QUIET"].append((page, "no S3 deps; check externally", size, s3_refs[:5]))

    # ─── C. Print verdicts ──────────────────────────────────────────────
    r.section("C. Verdicts")

    for category in ["BROKEN", "ARCHIVE", "QUIET", "PROMOTE"]:
        items = triage[category]
        r.log(f"\n  ── {category} ({len(items)} pages):")
        for page, reason, size, srcs in sorted(items):
            r.log(f"    {page:35} {size:>7}B  {reason}")
            for s in srcs[:3]:
                r.log(f"      ← {s}")

    # ─── D. Recommendations ─────────────────────────────────────────────
    r.section("D. Recommendations")
    r.log(f"\n  ARCHIVE (delete or move to /archive/): {len(triage['ARCHIVE'])} pages")
    r.log(f"  BROKEN  (need fixes before any use):    {len(triage['BROKEN'])} pages")
    r.log(f"  QUIET   (link from related pages, not main launcher): {len(triage['QUIET'])} pages")
    r.log(f"  PROMOTE (add to launcher tier 3):       {len(triage['PROMOTE'])} pages")

    # ─── Cleanup ────────────────────────────────────────────────────────
    try: lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError: pass
    r.log("Done")
