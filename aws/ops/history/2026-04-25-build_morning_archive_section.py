#!/usr/bin/env python3
"""
Step 115 — Add Section 1 (Morning Brief Archive) to reports.html.

Data source confirmed by step 114:
  - archive/intelligence/YYYY/MM/DD/HHMM.json — 605 snapshots
  - Rich shape: headline, headline_detail, regime, phase, forecast,
    signals, stock_signals, metrics_table, risks, portfolio, scores
  - Hourly cadence

Strategy:
  1. Extend justhodl-reports-builder Lambda with a new function
     compute_morning_archive() that:
       - Lists archive/intelligence/* keys
       - For each day, picks the snapshot closest to 12:05 UTC
         (= 8:05 ET, the morning-intelligence run time)
       - Extracts the canonical fields (headline, regime, scores,
         action_required, etc.)
       - Returns last 30 days, newest first
  2. Adds 'morning_archive' key to scorecard.json output
  3. Updates reports.html with Section 1 — a card-grid of past
     morning briefs, each showing date / regime badge / headline /
     forecast / one-line action
  4. Keeps load reasonable: don't fetch all 605 snapshots, just
     the chosen one per day (= ~30 fetches max)
"""
import io
import json
import os
import re
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


with report("build_morning_archive_section") as r:
    r.heading("Add Section 1 (Morning Brief Archive) — Lambda + reports.html")

    # ─── 1. Patch reports-builder Lambda ────────────────────────────────
    r.section("1. Patch Lambda — add compute_morning_archive()")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    src = src_path.read_text()

    # The function to inject — placed right before lambda_handler
    new_func = '''def compute_morning_archive(s3_client, days=30):
    """Build a daily morning brief archive from archive/intelligence/.

    For each day in the past `days` days, picks the snapshot closest
    to 12:05 UTC (= 8:05 ET, when morning-intelligence runs). Extracts
    canonical fields suitable for display in reports.html Section 1.
    """
    from datetime import datetime, timezone, timedelta, date
    bucket = "justhodl-dashboard-live"

    # Collect intelligence/ keys for the last `days` days
    today = datetime.now(timezone.utc).date()
    cutoff_date = today - timedelta(days=days)
    keys_by_date = {}  # date -> list of (key, hhmm_distance_from_1205)

    paginator = s3_client.get_paginator("list_objects_v2")
    # Iterate the days backwards (today back to cutoff)
    for offset in range(days):
        d = today - timedelta(days=offset)
        prefix = f"archive/intelligence/{d.year}/{d.month:02d}/{d.day:02d}/"
        try:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    k = obj["Key"]
                    # Filename like 1205.json — strip the .json
                    fn = k.rsplit("/", 1)[-1]
                    m = re.match(r"(\\d{4})\\.json", fn)
                    if not m:
                        continue
                    hhmm = int(m.group(1))
                    # Distance from 12:05 UTC (= 1205) in minutes
                    h = hhmm // 100
                    minute = hhmm % 100
                    minutes = h * 60 + minute
                    target = 12 * 60 + 5  # 12:05 UTC
                    distance = abs(minutes - target)
                    keys_by_date.setdefault(d, []).append((k, distance))
        except Exception as e:
            print(f"morning_archive: list error for {d}: {e}")

    # For each day, pick the closest-to-1205 key
    chosen = {}
    for d, items in keys_by_date.items():
        items.sort(key=lambda x: x[1])
        chosen[d] = items[0][0]

    # Fetch each chosen snapshot and build the archive entry
    archive = []
    for d in sorted(chosen.keys(), reverse=True):  # newest first
        k = chosen[d]
        try:
            obj = s3_client.get_object(Bucket=bucket, Key=k)
            data = json.loads(obj["Body"].read().decode("utf-8"))
        except Exception as e:
            print(f"morning_archive: fetch error for {k}: {e}")
            continue

        # Extract canonical fields. Use .get with defaults so missing
        # fields don't break.
        scores = data.get("scores") or {}
        forecast = data.get("forecast") or {}
        archive.append({
            "date": d.isoformat(),
            "key": k,
            "generated_at": data.get("generated_at") or data.get("timestamp"),
            "regime": data.get("regime"),
            "phase": data.get("phase"),
            "phase_color": data.get("phase_color"),
            "headline": data.get("headline"),
            "headline_detail": data.get("headline_detail"),
            "action_required": data.get("action_required"),
            "khalid_score": scores.get("khalid_index") or scores.get("khalid"),
            "carry_risk": scores.get("carry_risk"),
            "ml_risk": scores.get("ml_risk") or scores.get("ml_intelligence"),
            "plumbing": scores.get("plumbing_stress") or scores.get("plumbing"),
            "vix": data.get("vix") or scores.get("vix"),
            "forecast_summary": forecast.get("summary") if isinstance(forecast, dict) else None,
            "risks_count": len(data.get("risks") or []),
            "signal_count": len(data.get("signals") or []),
        })

    return archive


'''

    # Inject right before lambda_handler
    if "def compute_morning_archive" in src:
        # Already present — replace
        pattern = re.compile(
            r"def compute_morning_archive\(.*?(?=\ndef lambda_handler)",
            re.DOTALL,
        )
        src_new = pattern.sub(new_func, src)
        r.log(f"  Replaced existing compute_morning_archive")
    else:
        # Insert before lambda_handler
        src_new = src.replace(
            "def lambda_handler(event, context):",
            new_func + "def lambda_handler(event, context):",
        )
        r.log(f"  Inserted compute_morning_archive before lambda_handler")

    # Now update lambda_handler to call it and add to output
    if '"morning_archive"' not in src_new:
        # Find the dict construction in lambda_handler — add morning_archive
        old_out = '''    # 5. Build output
    out = {
        "meta": {'''
        new_out = '''    # 5. Build output
    morning_archive = []
    try:
        morning_archive = compute_morning_archive(s3, days=30)
    except Exception as e:
        print(f"morning_archive failed: {e}")

    out = {
        "meta": {'''
        if old_out in src_new:
            src_new = src_new.replace(old_out, new_out)
            r.log(f"  Hooked compute_morning_archive into lambda_handler")
        else:
            r.warn(f"  Couldn't find lambda_handler insertion point — manual review needed")

        # Add to the output dict
        old_keys = '''        "calibration_weights": weights if isinstance(weights, dict) else {},
        "calibration_accuracy": accuracy if isinstance(accuracy, dict) else {},
    }'''
        new_keys = '''        "calibration_weights": weights if isinstance(weights, dict) else {},
        "calibration_accuracy": accuracy if isinstance(accuracy, dict) else {},
        "morning_archive": morning_archive,
    }'''
        if old_keys in src_new:
            src_new = src_new.replace(old_keys, new_keys)
            r.log(f"  Added morning_archive to output dict")

        # Update the body's response so we can verify
        old_body = '''            "scorecard_rows": len(scorecard),
            "timeline_points": len(timeline),
            "signals_seen": len(signals),
            "outcomes_seen": len(outcomes),'''
        new_body = '''            "scorecard_rows": len(scorecard),
            "timeline_points": len(timeline),
            "morning_archive_days": len(morning_archive),
            "signals_seen": len(signals),
            "outcomes_seen": len(outcomes),'''
        if old_body in src_new:
            src_new = src_new.replace(old_body, new_body)

    src_path.write_text(src_new)

    # Validate
    import ast
    try:
        ast.parse(src_new)
        r.ok(f"  Patched {src_path.name} — syntax OK ({src_new.count(chr(10))} LOC total)")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        raise SystemExit(1)

    # ─── 2. Re-deploy ──────────────────────────────────────────────────
    r.section("2. Re-deploy reports-builder")
    name = "justhodl-reports-builder"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, src_new)
    zbytes = buf.getvalue()
    lam.update_function_code(FunctionName=name, ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed ({len(zbytes)}B)")

    # Bump timeout to 180s — it's now scanning archive too
    lam.update_function_configuration(FunctionName=name, Timeout=180)
    lam.get_waiter("function_updated").wait(
        FunctionName=name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Timeout bumped to 180s")

    # ─── 3. Invoke + verify ─────────────────────────────────────────────
    r.section("3. Invoke + verify morning_archive populated")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    elapsed = time.time() - invoke_start
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  FunctionError: {payload[:600]}")
        raise SystemExit(1)
    body = json.loads(json.loads(resp.get("Payload").read().decode()).get("body", "{}"))
    r.ok(f"  Invoked in {elapsed:.1f}s: {body}")

    # Read scorecard.json + check morning_archive
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))
    archive = data.get("morning_archive", [])
    r.log(f"  morning_archive entries: {len(archive)}")
    if archive:
        r.log(f"  Sample (newest):")
        latest = archive[0]
        for k, v in latest.items():
            sval = json.dumps(v, default=str)[:120]
            r.log(f"    {k:20} {sval}")

    r.kv(
        morning_archive_days=len(archive),
        scorecard_size_kb=int(obj["ContentLength"] / 1024),
    )
    r.log("Done")
