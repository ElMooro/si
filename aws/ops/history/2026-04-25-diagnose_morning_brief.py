#!/usr/bin/env python3
"""
Step 114 — Diagnose what's available for Section 1 (Morning Brief Archive).

Per memory: justhodl-morning-intelligence runs 8AM ET daily, sends a
Telegram brief, and writes... where? Need to find out:

  1. List S3 keys under archive/ that look like morning briefs
     (keys like archive/morning/, archive/intelligence/, etc.)
  2. Check learning/morning_run_log.json (overwritten daily but worth
     a look at structure)
  3. Look at the morning-intelligence Lambda source to see where it
     writes outputs
  4. See if there's a permanent log somewhere that retains history

If history doesn't exist anywhere, Section 1 needs a NEW S3 path
that morning-intelligence will append to going forward, and we can't
backfill the past (it's gone — Telegram messages aren't archived).

Output: a clear yes/no on whether Section 1 is buildable right now.
"""
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


with report("diagnose_morning_brief_archive") as r:
    r.heading("Diagnose: where does Section 1 (morning brief archive) data live?")

    # ─── 1. Check learning/ keys ────────────────────────────────────────
    r.section("1. Keys under learning/")
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket="justhodl-dashboard-live", Prefix="learning/"
    ):
        for obj in page.get("Contents", []):
            age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
            r.log(f"  {obj['Key']:50} {obj['Size']:>8}B  age {age_h:.1f}h")

    # ─── 2. Look at archive/ for morning-brief-shaped keys ──────────────
    r.section("2. archive/ keys matching 'morning' or 'brief' or 'intelligence'")
    matches = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket="justhodl-dashboard-live", Prefix="archive/"
    ):
        for obj in page.get("Contents", []):
            k = obj["Key"].lower()
            if "morning" in k or "brief" in k or "intel" in k:
                matches.append({"key": obj["Key"], "size": obj["Size"], "modified": obj["LastModified"]})
    r.log(f"  Found {len(matches)} matching keys")
    matches.sort(key=lambda x: -x["modified"].timestamp())
    for m in matches[:15]:
        age_h = (datetime.now(timezone.utc) - m["modified"]).total_seconds() / 3600
        r.log(f"    {m['key']:60} {m['size']:>8}B  age {age_h:.1f}h")

    # ─── 3. Read morning-intelligence Lambda source — find where it writes
    r.section("3. morning-intelligence Lambda source — find write paths")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source"
    if src_dir.exists():
        all_keys_written = set()
        for p in src_dir.rglob("*.py"):
            content = p.read_text(encoding="utf-8", errors="ignore")
            r.log(f"  Source file: {p.relative_to(REPO_ROOT)} ({content.count(chr(10))} LOC)")
            # Find every put_object Key= pattern
            for m_ in re.finditer(r"""(?:put_object|Key)\s*=?\s*[(]?\s*Key\s*=\s*f?['"]([^'"]+)['"]""", content):
                all_keys_written.add(m_.group(1))
            # Simpler: just grep for Bucket/Key combos
            for m_ in re.finditer(r"""Key\s*=\s*f?['"]([^'"]+)['"]""", content):
                k = m_.group(1)
                if "{" in k or "{" in k:
                    # f-string — just note the template
                    r.log(f"    write template: Key={k}")
                else:
                    r.log(f"    writes: {k}")
                all_keys_written.add(k)
        r.log(f"\n  Distinct writes found: {len(all_keys_written)}")
    else:
        r.warn(f"  morning-intelligence source not in repo at {src_dir}")

    # ─── 4. Look at learning/morning_run_log.json structure ─────────────
    r.section("4. Read learning/morning_run_log.json structure")
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="learning/morning_run_log.json")
        data = json.loads(obj["Body"].read().decode("utf-8"))
        r.log(f"  Last modified: {obj['LastModified']}")
        r.log(f"  Type: {type(data).__name__}")
        if isinstance(data, dict):
            r.log(f"  Top keys: {sorted(data.keys())}")
            r.log(f"  Sample (first 800 chars):")
            r.log(json.dumps(data, default=str)[:800])
        elif isinstance(data, list):
            r.log(f"  Length: {len(data)}")
            if data:
                r.log(f"  Last entry keys: {sorted(data[-1].keys()) if isinstance(data[-1], dict) else 'not dict'}")
    except Exception as e:
        r.warn(f"  Couldn't read: {e}")

    # ─── 5. Search S3 root for morning-related top-level files ─────────
    r.section("5. S3 root: any morning-* / intelligence-* / brief-* files?")
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket="justhodl-dashboard-live", Delimiter="/"
    ):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if any(w in k.lower() for w in ["morning", "brief", "intel"]):
                age_h = (datetime.now(timezone.utc) - obj["LastModified"]).total_seconds() / 3600
                r.log(f"  {k:55} {obj['Size']:>10}B  age {age_h:.1f}h")
        for prefix in page.get("CommonPrefixes", []):
            r.log(f"  prefix/ {prefix['Prefix']}")

    # ─── 6. Sample one archive/intelligence/.../HHMM.json to see content
    r.section("6. Sample one archive/intelligence file to see if it's a morning brief")
    if matches:
        # Sort by name (chronological), pick the earliest one we have
        matches_with_morning = [m for m in matches if "8" in m["key"] or "morning" in m["key"].lower()]
        sample_key = matches_with_morning[0]["key"] if matches_with_morning else matches[0]["key"]
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=sample_key)
            content = obj["Body"].read()
            r.log(f"  Sampling: {sample_key} ({len(content):,}B)")
            try:
                data = json.loads(content.decode("utf-8"))
                if isinstance(data, dict):
                    r.log(f"  Top keys: {sorted(data.keys())[:20]}")
                    # Look for narrative or text fields
                    for k in ["narrative", "ai_outlook", "macro_outlook", "summary", "brief", "ai_response"]:
                        if k in data:
                            r.log(f"  {k}: {str(data[k])[:200]}")
            except json.JSONDecodeError:
                r.log(f"  Not JSON — first 200 bytes: {content[:200]}")
        except Exception as e:
            r.warn(f"  Couldn't fetch: {e}")

    r.log("Done")
