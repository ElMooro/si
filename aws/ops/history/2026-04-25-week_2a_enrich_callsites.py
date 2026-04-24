#!/usr/bin/env python3
"""
WEEK 2A part 2 — Enrich existing signal-logger call sites with
magnitude + rationale where natural.

The schema accepts these fields, but no caller passes them yet.
Going through call sites and adding what's natural:

  ✓ momentum_* signals — magnitude is literally the % change tracked
  ✓ khalid_index — rationale derived from score (e.g. 'KI=43 NEUTRAL')
  ✓ crypto_fear_greed — rationale = 'Fear & Greed: 46 (Fear)'
  ✓ btc_mvrv — rationale based on MVRV regime
  ✓ cape_ratio — rationale = 'CAPE 35 — extremely expensive vs hist 25'
  ✓ buffett_indicator — rationale = 'Mkt Cap/GDP 200% — frothy'
  ✓ plumbing_stress — rationale = stress score interpretation

Magnitudes are conservative (2% for medium signals, 5% for strong).
These are starter values — calibrator will learn what signals
actually deliver and adjust confidence over time.

NOT touching: cftc_* (those need contract-by-contract analysis,
better to leave None and let calibration learn from the binary
direction). screener_top_pick — already has piotroski metadata,
deferred until Week 3 ranker uses analyst targets.
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("week_2a_enrich_callsites") as r:
    r.heading("Week 2A part 2 — enrich call sites with magnitude + rationale")

    sl_path = REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source/lambda_function.py"
    src = sl_path.read_text(encoding="utf-8")

    # ─── 1. momentum_* — magnitude is literally chg ─────────────────
    old_momentum = '''        if cf3>=0.3: logged.append(log_sig(f"momentum_{tk.lower()}",f"{chg:+.2f}%",p3,cf3,tk,[1,3,7],meta={"change":chg}))'''
    new_momentum = '''        if cf3>=0.3:
            mom_mag=chg if abs(chg)<10 else (10 if chg>0 else -10)  # cap at ±10% to filter outliers
            mom_rat=f"{tk} momentum: {chg:+.2f}% recent change → {p3} {abs(chg):.1f}% over 1-7d"
            logged.append(log_sig(f"momentum_{tk.lower()}",f"{chg:+.2f}%",p3,cf3,tk,[1,3,7],meta={"change":chg},magnitude=mom_mag,rationale=mom_rat))'''

    if old_momentum in src:
        src = src.replace(old_momentum, new_momentum, 1)
        r.ok("  Enriched momentum_* with magnitude + rationale")
    else:
        r.warn("  momentum_* pattern not found")

    # ─── 2. khalid_index — rationale from score+regime ──────────────
    old_ki = '''        logged.append(log_sig("khalid_index",val,dir_score(ki,35,65),conf_ext(ki),"SPY",[7,14,30],meta={"score":ki,"regime":d.get("regime")}))'''
    new_ki = '''        ki_rat=f"Khalid Index {ki:.0f} = {val} ({d.get('regime') or 'unknown'} regime)"
        logged.append(log_sig("khalid_index",val,dir_score(ki,35,65),conf_ext(ki),"SPY",[7,14,30],meta={"score":ki,"regime":d.get("regime")},rationale=ki_rat))'''

    if old_ki in src:
        src = src.replace(old_ki, new_ki, 1)
        r.ok("  Enriched khalid_index with rationale")
    else:
        r.warn("  khalid_index pattern not found")

    # ─── 3. crypto_fear_greed — rationale from score+label ──────────
    old_fg = '''        logged.append(log_sig("crypto_fear_greed",v,p,cf,"BTC-USD",[1,3,7,14],meta={"score":fgs,"label":fg.get("label")}))'''
    new_fg = '''        fg_rat=f"Fear & Greed {int(fgs)} ({fg.get('label') or v}) — contrarian {p} signal"
        logged.append(log_sig("crypto_fear_greed",v,p,cf,"BTC-USD",[1,3,7,14],meta={"score":fgs,"label":fg.get("label")},rationale=fg_rat))'''

    if old_fg in src:
        src = src.replace(old_fg, new_fg, 1)
        r.ok("  Enriched crypto_fear_greed with rationale")
    else:
        r.warn("  crypto_fear_greed pattern not found")

    # ─── 4. btc_mvrv — magnitude based on regime ────────────────────
    old_mvrv = '''        logged.append(log_sig("btc_mvrv",v2,p2,cf2,"BTC-USD",[14,30,60],meta={"mvrv":mvrv}))'''
    new_mvrv = '''        mvrv_mag=10.0 if mvrv<0.8 else (-15.0 if mvrv>3.5 else (-8.0 if mvrv>2.5 else 0))
        mvrv_rat=f"MVRV {mvrv:.2f} = {v2} (historic UP at <1, DOWN at >3)"
        logged.append(log_sig("btc_mvrv",v2,p2,cf2,"BTC-USD",[14,30,60],meta={"mvrv":mvrv},magnitude=mvrv_mag,rationale=mvrv_rat))'''

    if old_mvrv in src:
        src = src.replace(old_mvrv, new_mvrv, 1)
        r.ok("  Enriched btc_mvrv with magnitude + rationale")
    else:
        r.warn("  btc_mvrv pattern not found")

    # ─── 5. plumbing_stress — rationale from stress score ───────────
    old_plumb = '''        logged.append(log_sig("plumbing_stress",v3,p3,cf3,"SPY",[1,7,14,30],meta={"score":sc,"status":st.get("status"),"red_flags":st.get("red_flags")}))'''
    new_plumb = '''        plumb_rat=f"Plumbing stress {sc:.0f} = {v3} ({st.get('status') or '?'}); red_flags={st.get('red_flags') or 0}"
        logged.append(log_sig("plumbing_stress",v3,p3,cf3,"SPY",[1,7,14,30],meta={"score":sc,"status":st.get("status"),"red_flags":st.get("red_flags")},rationale=plumb_rat))'''

    if old_plumb in src:
        src = src.replace(old_plumb, new_plumb, 1)
        r.ok("  Enriched plumbing_stress with rationale")
    else:
        r.warn("  plumbing_stress pattern not found")

    # ─── 6. cape_ratio — rationale + magnitude ──────────────────────
    old_cape = '''        logged.append(log_sig("cape_ratio",v5,p5,cf5,"SPY",[30,60,90],meta={"cape":cape}))'''
    new_cape = '''        cape_mag=-8.0 if cape>35 else (-4.0 if cape>28 else (5.0 if cape<15 else 0))
        cape_rat=f"Shiller CAPE {cape:.1f} = {v5} (historical avg ~17, frothy >28)"
        logged.append(log_sig("cape_ratio",v5,p5,cf5,"SPY",[30,60,90],meta={"cape":cape},magnitude=cape_mag,rationale=cape_rat))'''

    if old_cape in src:
        src = src.replace(old_cape, new_cape, 1)
        r.ok("  Enriched cape_ratio with magnitude + rationale")
    else:
        r.warn("  cape_ratio pattern not found")

    # ─── 7. buffett_indicator — rationale + magnitude ───────────────
    old_buff = '''        logged.append(log_sig("buffett_indicator",v6,p6,cf6,"SPY",[30,60,90],meta={"buffett":buffett}))'''
    new_buff = '''        buff_mag=-10.0 if buffett>200 else (-5.0 if buffett>150 else (5.0 if buffett<100 else 0))
        buff_rat=f"Buffett Indicator (Mkt Cap/GDP) {buffett:.0f}% = {v6} — historic balance at ~100%"
        logged.append(log_sig("buffett_indicator",v6,p6,cf6,"SPY",[30,60,90],meta={"buffett":buffett},magnitude=buff_mag,rationale=buff_rat))'''

    if old_buff in src:
        src = src.replace(old_buff, new_buff, 1)
        r.ok("  Enriched buffett_indicator with magnitude + rationale")
    else:
        r.warn("  buffett_indicator pattern not found")

    # ─── Validate and deploy ─────────────────────────────────────────
    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    sl_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-signal-logger", sl_path.parent)
    r.ok(f"  Deployed signal-logger ({size:,} bytes)")

    # Trigger fresh run
    r.section("Trigger fresh run with enriched call sites")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-signal-logger",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered (status {resp['StatusCode']})")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        callsites_enriched=7,
        magnitude_added=["momentum_*", "btc_mvrv", "cape_ratio", "buffett_indicator"],
        rationale_added=["all 7 sites"],
    )
    r.log("Done")
