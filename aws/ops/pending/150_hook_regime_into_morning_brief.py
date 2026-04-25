#!/usr/bin/env python3
"""
Step 150 — Hook Phase 1A bond regime detector into morning-intelligence.

The morning brief currently has no view of the bond regime detector's
output. The regime/current.json file is being written every 4h with
genuine signal (today: NEUTRAL, slight risk-off tilt, 7 indicators)
but morning-intelligence doesn't read it. This means:
  - Briefs talk about Khalid Index and crypto regime
  - But miss the bond market upstream regime indicator
  - When bond detector eventually fires RISK_OFF, brief won't mention it

Fix: 3 small surgical edits to morning-intelligence:
  1. Add 'bond_regime': 'regime/current.json' to load_all() keys
  2. Add bond_regime fields to metrics dict in extract_metrics()
  3. Add a bond regime line to the prompt's metrics list

Each edit is a precise str_replace targeting exact existing text. No
restructuring. No new helper functions. The brief simply gets a few
new fields it can reference.

EXPECTED IMPACT TODAY:
  - Tomorrow's brief at 8AM ET will see:
    'BOND REGIME: NEUTRAL strength=57.9 extreme=0/7 (risk_off:0 risk_on:6)
     T5YIE rising +1.17σ — slight inflation pressure'
  - When detector fires RISK_OFF in the future, brief will lead with it

NO CALIBRATION CHANGES — this is purely additive context. The Loop 1
calibration logic is untouched.
"""
import io
import json
import os
import time
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

BUCKET = "justhodl-dashboard-live"


with report("hook_regime_into_morning_brief") as r:
    r.heading("Hook Phase 1A bond regime into morning brief")

    src_path = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py"
    src = src_path.read_text()
    r.log(f"  Current source: {len(src):,}B, {src.count(chr(10))} LOC")

    # ─── Edit 1: Add regime to load_all() keys ─────────────────────────
    r.section("1. Add 'bond_regime' to load_all() keys")
    OLD_KEYS = '''def load_all():
    keys={
        "main":"data/report.json",
        "intel":"intelligence-report.json",
        "crypto":"crypto-intel.json",
        "edge":"edge-data.json",
        "repo":"repo-data.json",
        "flow":"flow-data.json",
        "screener":"screener/data.json",
        "predictions":"predictions.json",
        "valuations":"valuations-data.json"
    }
    return {k:fs3(v) for k,v in keys.items()}'''

    NEW_KEYS = '''def load_all():
    keys={
        "main":"data/report.json",
        "intel":"intelligence-report.json",
        "crypto":"crypto-intel.json",
        "edge":"edge-data.json",
        "repo":"repo-data.json",
        "flow":"flow-data.json",
        "screener":"screener/data.json",
        "predictions":"predictions.json",
        "valuations":"valuations-data.json",
        "bond_regime":"regime/current.json",
        "divergence":"divergence/current.json"
    }
    return {k:fs3(v) for k,v in keys.items()}'''

    if OLD_KEYS in src:
        src = src.replace(OLD_KEYS, NEW_KEYS)
        r.ok("  Added bond_regime + divergence to load_all() keys")
    elif "bond_regime" in src:
        r.log("  Already added")
    else:
        r.fail("  Couldn't find anchor — manual fix needed")
        raise SystemExit(1)

    # ─── Edit 2: Add bond_regime extraction in metrics dict ────────────
    # Find the metrics dict by anchoring on a stable existing field.
    # Insert new fields right before "btc_price":btc.get("price"),
    r.section("2. Add bond_regime fields to metrics dict")

    OLD_BTC_LINE = '''        "btc_price":btc.get("price"),'''
    NEW_BTC_BLOCK = '''        # ─── Phase 1A bond regime + Phase 1B divergence — added 2026-04-25 ───
        "bond_regime":(data.get("bond_regime") or {}).get("regime","UNKNOWN"),
        "bond_regime_strength":(data.get("bond_regime") or {}).get("regime_strength"),
        "bond_extreme_count":(data.get("bond_regime") or {}).get("indicators_extreme",0),
        "bond_total_count":(data.get("bond_regime") or {}).get("indicators_total",0),
        "bond_n_off":(data.get("bond_regime") or {}).get("n_risk_off",0),
        "bond_n_on":(data.get("bond_regime") or {}).get("n_risk_on",0),
        "bond_changed":(data.get("bond_regime") or {}).get("regime_changed",False),
        "bond_extreme_signals":[
            (s.get("name"),s.get("z"),s.get("direction"))
            for s in ((data.get("bond_regime") or {}).get("signals") or [])
            if s.get("extreme")
        ][:5],
        "divergence_extreme_count":((data.get("divergence") or {}).get("summary") or {}).get("n_extreme",0),
        "divergence_alert_count":((data.get("divergence") or {}).get("summary") or {}).get("n_alert_worthy",0),
        "divergence_top":[
            (rel.get("name"),rel.get("z_score"),rel.get("mispricing"))
            for rel in ((data.get("divergence") or {}).get("relationships") or [])
            if rel.get("status")=="ok" and rel.get("extreme")
        ][:3],
        "btc_price":btc.get("price"),'''

    if OLD_BTC_LINE in src:
        src = src.replace(OLD_BTC_LINE, NEW_BTC_BLOCK)
        r.ok("  Added 11 bond_regime + divergence fields to metrics")
    elif "bond_regime\":" in src and "data.get(\"bond_regime\")" in src:
        r.log("  Already added")
    else:
        r.fail("  Couldn't find btc_price anchor — manual fix")
        raise SystemExit(1)

    # ─── Edit 3: Add bond regime line to prompt metrics ─────────────────
    # The prompt's metrics list is built around line 322-340. Find a
    # stable anchor: the EDGE line.
    r.section("3. Add BOND REGIME line to prompt metrics")

    OLD_EDGE_LINE = '''"EDGE: "+str(m["edge_score"])+"/100 ("+str(m["edge_regime"])+") ML_RISK:"+str(m["ml_risk"])+" CARRY:"+str(m["carry_risk"])+" CRISIS_DIST:"+str(m["crisis_dist"])+"pts",'''

    # Render the bond regime as a single string snapshot
    NEW_EDGE_BLOCK = '''"EDGE: "+str(m["edge_score"])+"/100 ("+str(m["edge_regime"])+") ML_RISK:"+str(m["ml_risk"])+" CARRY:"+str(m["carry_risk"])+" CRISIS_DIST:"+str(m["crisis_dist"])+"pts",
        # ─── Phase 1A: Bond regime (added 2026-04-25) ───
        "BOND_REGIME: "+str(m["bond_regime"])+" strength="+str(m["bond_regime_strength"])+"/100 extreme="+str(m["bond_extreme_count"])+"/"+str(m["bond_total_count"])+" (risk_off:"+str(m["bond_n_off"])+" risk_on:"+str(m["bond_n_on"])+")"+(" REGIME_CHANGED" if m["bond_changed"] else "")+(" extremes:"+",".join([s[0]+"("+("+" if s[1]>=0 else "")+str(round(s[1],1))+")" for s in m["bond_extreme_signals"]]) if m["bond_extreme_signals"] else ""),
        # ─── Phase 1B: Cross-asset divergence ───
        "DIVERGENCE: "+str(m["divergence_extreme_count"])+" pairs >2σ, "+str(m["divergence_alert_count"])+" >3σ alerts"+(" TOP:"+";".join([d[0]+"("+("+" if d[1]>=0 else "")+str(round(d[1],1))+")" for d in m["divergence_top"]]) if m["divergence_top"] else ""),'''

    if OLD_EDGE_LINE in src:
        src = src.replace(OLD_EDGE_LINE, NEW_EDGE_BLOCK)
        r.ok("  Added BOND_REGIME + DIVERGENCE lines to prompt metrics")
    elif "BOND_REGIME:" in src:
        r.log("  Already added")
    else:
        r.fail("  Couldn't find EDGE line anchor — manual fix")
        raise SystemExit(1)

    # ─── Validate ─────────────────────────────────────────────────────
    r.section("4. Validate + write")
    import ast
    try:
        ast.parse(src)
        r.ok(f"  Syntax OK — new size {len(src):,}B (was smaller)")
    except SyntaxError as e:
        r.fail(f"  Syntax: {e}")
        if hasattr(e, "lineno"):
            lines = src.split("\n")
            for i in range(max(0, e.lineno - 3), min(len(lines), e.lineno + 3)):
                marker = " >>> " if i == e.lineno - 1 else "     "
                r.log(f"  {marker}L{i+1}: {lines[i][:200]}")
        raise SystemExit(1)
    src_path.write_text(src)
    r.log(f"  Wrote patched source")

    # ─── Deploy ────────────────────────────────────────────────────────
    r.section("5. Deploy morning-intelligence")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-morning-intelligence/source"
    buf = io.BytesIO()
    files_added = 0
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for src_file in sorted(src_dir.rglob("*.py")):
            arcname = str(src_file.relative_to(src_dir))
            info = zipfile.ZipInfo(arcname)
            info.external_attr = 0o644 << 16
            zout.writestr(info, src_file.read_text())
            files_added += 1
    zbytes = buf.getvalue()
    lam.update_function_code(
        FunctionName="justhodl-morning-intelligence", ZipFile=zbytes,
        Architectures=["arm64"],
    )
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-morning-intelligence",
        WaiterConfig={"Delay": 3, "MaxAttempts": 30},
    )
    r.ok(f"  Deployed ({len(zbytes):,}B, {files_added} files)")

    # ─── Test invoke ───────────────────────────────────────────────────
    r.section("6. Test invoke morning-intelligence")
    time.sleep(3)
    invoke_start = time.time()
    resp = lam.invoke(
        FunctionName="justhodl-morning-intelligence",
        InvocationType="RequestResponse",
    )
    elapsed = time.time() - invoke_start
    payload = resp.get("Payload").read().decode()
    if resp.get("FunctionError"):
        r.fail(f"  FunctionError ({elapsed:.1f}s): {payload[:600]}")
        raise SystemExit(1)
    r.ok(f"  Invoked in {elapsed:.1f}s")

    try:
        outer = json.loads(payload)
        body = json.loads(outer.get("body", "{}"))
        r.log(f"  Response: success={body.get('success')}, "
              f"khalid={body.get('khalid')}, regime={body.get('regime')}")
    except Exception:
        r.log(f"  Raw: {payload[:300]}")

    # ─── Confirm regime made it into the run log ───────────────────────
    r.section("7. Verify regime data flowed into morning brief")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="learning/morning_run_log.json")
        log = json.loads(obj["Body"].read().decode())
        r.log(f"  morning_run_log keys: {sorted(log.keys())}")
        r.log(f"  khalid: {log.get('khalid')}")
        r.log(f"  regime: {log.get('regime')}")
    except Exception as e:
        r.warn(f"  read run_log: {e}")

    # ─── Sanity check: read the TG message that was sent ───────────────
    r.section("8. Verify regime appeared in latest brief")
    # The brief is dispatched via Telegram, also archived locally.
    # Look at archive/intelligence/ today
    from datetime import datetime, timezone
    today_prefix = datetime.now(timezone.utc).strftime("archive/intelligence/%Y/%m/%d/")
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=today_prefix, MaxKeys=10)
        objs = sorted(resp.get("Contents", []),
                      key=lambda o: o["LastModified"], reverse=True)
        if objs:
            latest = objs[0]
            r.log(f"  Latest brief: {latest['Key']}, {latest['LastModified'].isoformat()}")
            obj = s3.get_object(Bucket=BUCKET, Key=latest["Key"])
            data = json.loads(obj["Body"].read().decode())
            metrics_str = json.dumps(data)
            if "BOND_REGIME" in metrics_str or "bond_regime" in metrics_str:
                r.ok(f"  ✅ Bond regime data found in brief output")
            else:
                r.log(f"  Brief structure (top keys): {sorted(data.keys())[:8]}")
        else:
            r.log(f"  No brief archive yet today")
    except Exception as e:
        r.warn(f"  archive list: {e}")

    r.kv(
        zip_size=len(zbytes),
        invoke_s=f"{elapsed:.1f}",
    )
    r.log("Done")
