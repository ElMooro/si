#!/usr/bin/env python3
"""
Fix two bugs in v3.2 rollout:

BUG 1 — NameError 'timezone' in daily-report-v3:
  The smart TTL helper uses datetime.now(timezone.utc) and
  datetime.fromisoformat, which need 'timezone' from the datetime
  module. But daily-report-v3's import line is only:
      from datetime import datetime, timedelta
  Missing 'timezone'. Fix: update the import.

BUG 2 — Cache shape conflict:
  Both Lambdas write to s3://.../data/fred-cache.json, but:
    - daily-report-v3 writes {sid: [{date, value}, {date, value}, ...]}
    - secretary v3.2 also tried to read this but its _save_fred_cache
      writes {sid: {name, value, prev, chg_1d, history, ...}} shape
  When secretary wrote, it clobbered daily-report's rich cache with its
  summary-only shape. Need to standardize on the list-of-observations
  shape since that's what the smart TTL helper needs.

Fix for BUG 2: update secretary's cache-write so it either:
  a) Writes list-of-observations shape alongside its dict shape, OR
  b) Uses a separate cache key like data/fred-cache-secretary.json

Going with (b) because (a) could accidentally mix shapes. Secretary
is already the lighter-load Lambda; it barely needs caching. Let it
use its own cache key.

Also restoring daily-report cache from scratch — the secretary's
bad-shape write corrupted it. Daily-report's next run will rebuild
in ~80-240s.
"""

import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
DR = REPO_ROOT / "aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py"
SEC = REPO_ROOT / "aws/lambdas/justhodl-financial-secretary/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


with report("fix_v32_bugs") as r:
    r.heading("Fix v3.2 — NameError(timezone) + cache shape conflict")

    # ─────────────────────────────────────────
    # FIX 1: add 'timezone' to daily-report-v3 imports
    # ─────────────────────────────────────────
    r.section("Fix 1: add 'timezone' import to daily-report-v3")
    dr_src = DR.read_text(encoding="utf-8")
    old_import = "from datetime import datetime, timedelta"
    new_import = "from datetime import datetime, timedelta, timezone"
    if new_import in dr_src:
        r.log("  'timezone' already imported")
    elif old_import in dr_src:
        dr_src = dr_src.replace(old_import, new_import, 1)
        r.ok("  Added 'timezone' to import line")
    else:
        r.fail("  Import line not found as expected")
        raise SystemExit(1)

    import ast
    try:
        ast.parse(dr_src)
        r.ok(f"  daily-report syntax valid ({len(dr_src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR: {e}")
        raise SystemExit(1)
    DR.write_text(dr_src, encoding="utf-8")

    # ─────────────────────────────────────────
    # FIX 2a: change secretary's cache key so it doesn't clobber daily-report
    # ─────────────────────────────────────────
    r.section("Fix 2a: secretary writes to data/fred-cache-secretary.json")
    sec_src = SEC.read_text(encoding="utf-8")

    # Replace every _FRED_CACHE_KEY or "data/fred-cache.json" reference in
    # secretary's cache paths. There are TWO: one read and one write.
    # Both are in fetch_fred().
    old_read = 's3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")'
    new_read = 's3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")'
    old_write = 'Bucket=BUCKET, Key="data/fred-cache.json",'
    new_write = 'Bucket=BUCKET, Key="data/fred-cache-secretary.json",'

    read_count = sec_src.count(old_read)
    write_count = sec_src.count(old_write)
    sec_src = sec_src.replace(old_read, new_read)
    sec_src = sec_src.replace(old_write, new_write)
    r.log(f"  Patched {read_count} read + {write_count} write call(s)")

    # v3.2 added a NEW smart-TTL cache read for secretary that reads from
    # data/fred-cache.json (daily-report's cache). Keep that one as-is
    # since secretary BENEFITS from reading daily-report's richer cache.
    # Only the WRITE is redirected.
    # Let's verify the smart-TTL read still references the main cache:
    if '_cache_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")' in sec_src:
        r.log("  Secretary smart-TTL read still uses main cache (correct — richer data)")
    elif '_cache_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")' in sec_src:
        r.warn("  Smart-TTL read was redirected to secretary cache (lost richer data)")
        sec_src = sec_src.replace(
            '_cache_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache-secretary.json")',
            '_cache_obj = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")',
            1,
        )
        r.ok("  Redirected smart-TTL read back to main cache")

    try:
        ast.parse(sec_src)
        r.ok(f"  secretary syntax valid ({len(sec_src)} bytes)")
    except SyntaxError as e:
        r.fail(f"  SYNTAX ERROR: {e}")
        raise SystemExit(1)
    SEC.write_text(sec_src, encoding="utf-8")

    # ─────────────────────────────────────────
    # FIX 2b: wipe the corrupted fred-cache.json so next daily-report
    # run rebuilds it from scratch
    # ─────────────────────────────────────────
    r.section("Fix 2b: delete corrupted fred-cache.json")
    try:
        s3.delete_object(Bucket="justhodl-dashboard-live", Key="data/fred-cache.json")
        r.ok("  Deleted data/fred-cache.json (next daily-report rebuilds it)")
    except Exception as e:
        r.warn(f"  Delete failed (may not exist): {e}")

    # ─────────────────────────────────────────
    # Deploy
    # ─────────────────────────────────────────
    r.section("Deploy daily-report-v3")
    z1 = build_zip(DR.parent)
    lam.update_function_code(FunctionName="justhodl-daily-report-v3", ZipFile=z1)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-daily-report-v3",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  daily-report-v3 deployed ({len(z1)} bytes)")

    r.section("Deploy secretary")
    z2 = build_zip(SEC.parent)
    lam.update_function_code(FunctionName="justhodl-financial-secretary", ZipFile=z2)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-financial-secretary",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  secretary deployed ({len(z2)} bytes)")

    # Async trigger daily-report to rebuild cache
    r.section("Trigger daily-report-v3 async (rebuilds fresh cache)")
    import json as _json
    resp = lam.invoke(
        FunctionName="justhodl-daily-report-v3", InvocationType="Event",
        Payload=_json.dumps({"source": "aws.events"}).encode(),
    )
    r.ok(f"  Async triggered (status {resp['StatusCode']})")
    r.log("  Cache will be rebuilt in ~3-5 min. Next run after that should skip ~90%.")

    r.log("Done")
