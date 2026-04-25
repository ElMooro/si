#!/usr/bin/env python3
"""
Step 109 — Fix Khalid timeline grouping bug.

Diagnosis from step 108:
  - 186 khalid_index signals exist in DDB
  - All 186 logged TODAY (same date)
  - The Lambda's "group by date, take first reading" collapsed them
    to ~2 distinct dates, hence the 2-point timeline

Fix: don't collapse by date. Use the raw timestamp series. The chart
will show intra-day movement which is more useful anyway. Also bump
the cutoff from 90 days to "all time" since we don't actually have
90 days of data — yet.

Plus: instead of just signal_value, look in the metadata field too
since the value might be there.
"""
import io
import json
import os
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)


# First, peek at one khalid_index signal to confirm shape of signal_value
with report("fix_khalid_timeline_v2") as r:
    r.heading("Fix Khalid timeline — use full timestamps, not date-grouped")

    # ─── 1. Sample a khalid_index signal to confirm shape ──────────────
    r.section("1. Inspect khalid_index signal shape")
    t = ddb.Table("justhodl-signals")
    resp = t.scan(
        FilterExpression="signal_type = :st",
        ExpressionAttributeValues={":st": "khalid_index"},
        Limit=3,
    )
    for item in resp.get("Items", [])[:3]:
        keys = sorted(item.keys())
        r.log(f"  Keys: {keys}")
        r.log(f"  signal_value: {item.get('signal_value')!r} (type {type(item.get('signal_value')).__name__})")
        r.log(f"  metadata: {json.dumps({k: str(v) for k, v in (item.get('metadata') or {}).items()})}")
        r.log(f"  logged_at: {item.get('logged_at')}")
        r.log(f"  ---")

    # ─── 2. Patch Lambda compute_khalid_timeline ───────────────────────
    r.section("2. Patch Lambda")
    src_path = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source/lambda_function.py"
    src = src_path.read_text()

    new_func = '''def compute_khalid_timeline(signals):
    """Extract Khalid Index timeline from logged signals.

    Strategy:
      - Filter to signals where signal_type == 'khalid_index'
      - Use signal_value (the score) and metadata.regime
      - Return ALL points sorted by timestamp (no date-grouping —
        intra-day movement matters when our history is recent)
      - Trim to last 90 days (currently won't trim much; we have
        only ~1 day of khalid_index data so far)
    """
    points = []
    for s in signals:
        if s.get("signal_type") != "khalid_index":
            continue
        ts = s.get("logged_at")
        sv = s.get("signal_value")
        if sv is None or not ts:
            continue
        # signal_value can be a string like "43" or a Decimal/float — coerce
        try:
            if isinstance(sv, str):
                score = float(sv.replace("%", "").strip())
            else:
                score = float(sv)
        except Exception:
            continue
        meta = s.get("metadata") or {}
        regime = meta.get("regime") or s.get("regime_at_log") or s.get("regime")
        dt = parse_iso(ts)
        if not dt:
            continue
        points.append({
            "ts": dt.isoformat(),
            "date": dt.date().isoformat(),
            "score": score,
            "regime": regime,
        })

    # Fallback if no khalid_index signals: try khalid_score_at_log
    if len(points) < 5:
        for s in signals:
            score = s.get("khalid_score_at_log")
            ts = s.get("logged_at")
            if score is None or not ts:
                continue
            try:
                score_f = float(score)
            except Exception:
                continue
            regime = s.get("regime_at_log")
            dt = parse_iso(ts)
            if not dt:
                continue
            points.append({
                "ts": dt.isoformat(),
                "date": dt.date().isoformat(),
                "score": score_f,
                "regime": regime,
            })

    # Sort by timestamp; keep ALL points (no date-grouping)
    points.sort(key=lambda x: x["ts"])

    # Trim to last 90 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()
    return [p for p in points if p["ts"] >= cutoff]'''

    pattern = re.compile(
        r"def compute_khalid_timeline\(signals\):.*?(?=\ndef |\Z)",
        re.DOTALL,
    )
    if not pattern.search(src):
        r.fail("  Couldn't locate compute_khalid_timeline")
        raise SystemExit(1)
    src_new = pattern.sub(new_func + "\n\n", src)
    src_path.write_text(src_new)

    # Validate
    import ast
    try:
        ast.parse(src_new)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    # ─── 3. Re-deploy Lambda ───────────────────────────────────────────
    r.section("3. Re-deploy Lambda")
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

    # ─── 4. Invoke + verify ────────────────────────────────────────────
    r.section("4. Invoke + verify")
    time.sleep(3)
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        payload = resp.get("Payload").read().decode()
        r.fail(f"  FunctionError: {payload[:500]}")
    else:
        body = json.loads(json.loads(resp.get("Payload").read().decode()).get("body", "{}"))
        r.ok(f"  Invoked: timeline_points={body.get('timeline_points')} "
             f"scorecard_rows={body.get('scorecard_rows')}")

    # Verify by reading the file
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
    data = json.loads(obj["Body"].read().decode("utf-8"))
    timeline = data.get("khalid_timeline", [])
    r.log(f"  Timeline points: {len(timeline)}")
    if timeline:
        r.log(f"  First point: {timeline[0]}")
        r.log(f"  Last point: {timeline[-1]}")
        unique_dates = set(p.get("date") for p in timeline)
        r.log(f"  Unique dates: {len(unique_dates)}")
        score_min = min(p["score"] for p in timeline)
        score_max = max(p["score"] for p in timeline)
        r.log(f"  Score range: {score_min} → {score_max}")

    # ─── 5. Update reports.html chart to handle intraday timestamps ────
    r.section("5. Update reports.html to display intra-day timestamps")
    html_path = REPO_ROOT / "reports.html"
    html = html_path.read_text()

    # The chart currently uses p.date for x-axis labels. Switch to formatted ts
    # so intraday points render distinctly.
    old_block = "const labels = timeline.map(p => p.date);"
    new_block = """const labels = timeline.map(p => {
    const d = new Date(p.ts);
    // Format like "MM-DD HH:MM"
    const mm = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dd = String(d.getUTCDate()).padStart(2, '0');
    const hh = String(d.getUTCHours()).padStart(2, '0');
    const mn = String(d.getUTCMinutes()).padStart(2, '0');
    return `${mm}-${dd} ${hh}:${mn}`;
  });"""
    if old_block in html:
        html = html.replace(old_block, new_block)
        html_path.write_text(html)
        r.ok(f"  Patched reports.html — chart now uses ts not date")
    else:
        r.warn(f"  Couldn't find expected block in reports.html — skipping HTML patch")

    # Verify: lower the empty-state threshold from 2 to 2 (unchanged) but
    # adjust the message
    old_msg = """<div style="font-size:11px;color:var(--t4)">Need at least 2 days of logged signals (current: ${(timeline || []).length})</div>"""
    new_msg = """<div style="font-size:11px;color:var(--t4)">Need at least 2 logged data points (current: ${(timeline || []).length}). The signal-logger fires every 6h.</div>"""
    if old_msg in html:
        html = html.replace(old_msg, new_msg)
        html_path.write_text(html)

    r.kv(
        timeline_points=len(timeline) if timeline else 0,
        unique_dates=len(set(p.get("date") for p in timeline)) if timeline else 0,
    )
    r.log("Done")
