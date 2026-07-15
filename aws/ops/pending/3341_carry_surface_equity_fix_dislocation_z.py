"""ops 3341 — carry-surface v1.1.0: fix the equity -3.62 wall + add dislocation z-score.

WHY (audit findings, confirmed against deployed source):
  BUG #1 — 40+ equities collapsed to an identical -3.62% (= -financing). Root cause:
    (a) fmp_dividend_yield_ttm queried /stable/ratios-ttm for EVERY equity, but ETFs
        (SPY/QQQ/all XL sectors/SCHD/EEM/...) are not covered by ratios-ttm (no income
        statement). The endpoint returned empty -> `or 0` coerced it to 0 -> the row was
        treated as VALID -> carry = 0 + 0 - 3.62 = -3.62. Silent-zero-on-wrong-endpoint,
        the exact FMP trap already in memory doctrine.
    (b) per-row magnitude heuristic (dy > 0.5 ? percent : *100) mis-scaled low-yielders
        and any percent-scaled field.
  FIX:
    - ETF-aware routing: ETFs -> /stable/dividends (declared TTM distributions / price =
      true trailing yield); single names -> /stable/ratios-ttm (deterministic decimal->pct).
    - NO silent zeros: unknown yield returns None; equity compute leaves such rows DORMANT
      with a named reason ('div_yield_unavailable') instead of masking as -financing.
      A verified non-payer (BRK-B etc.) legitimately keeps 0 -> -3.62.
    - Dormant/errored rows are EXCLUDED from the within-class z distribution so they can no
      longer flatten the stats.

  FEATURE #2 — dislocation z-score: current carry vs each asset's OWN trailing carry
    history (distinct from within-class z). Answers 'who is paying UNUSUALLY vs its norm'.
    Reads existing daily snapshots under data/carry-surface/history/ (already persisted by
    the handler — this is a pure READ-SIDE add, no writer needed). Gated at >=60 daily obs
    (~3 trading months; echoes MIN_COMPOSITE discipline); warms up gracefully, never fakes a
    z. Emits carry_own_z / carry_own_pctile / carry_own_mean_pct / dislocation_status, plus a
    dislocation_leaders board in the payload.

  Offline-tested (test_logic.py): SPY yield ~1.0% (was 0), dormant-not-masked, z excludes
  dormant, dislocation gate + firing all PASS.

  NOTE: separate tickets NOT touched here — commodity roll smoothing (USO +87/UNG -80),
  crypto n=0 (Binance geo-block), per-instrument financing.

VERIFY LOOP: smoke invoke -> then re-fetch data/carry-surface.json and assert the equity
  wall is gone (SPY carry != exactly -financing OR SPY is dormant with a reason) and that
  n_dormant is reported.
"""
import json
import sys
import time
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

FN = "justhodl-carry-surface"
SRC = Path(f"aws/lambdas/{FN}/source")
CFG = json.loads(Path(f"aws/lambdas/{FN}/config.json").read_text())
ENV = CFG["env"]
BUCKET = ENV["S3_BUCKET"]
OUT_KEY = ENV["OUT_KEY"]

with report("3341_carry_surface_equity_fix_dislocation_z") as r:
    r.section("Deploy carry-surface v1.1.0")
    smoke = deploy_lambda(
        report=r,
        function_name=FN,
        source_dir=SRC,
        env_vars=ENV,
        eb_rule_name=CFG["schedule"]["rule_name"],
        eb_schedule=CFG["schedule"]["cron"],
        timeout=CFG["timeout"],
        memory=CFG["memory"],
        description=CFG["description"],
        create_function_url=True,
        smoke=True,
    )
    r.log(f"smoke: {json.dumps(smoke)[:300] if smoke else 'None'}")

    # ---- VERIFY: full invoke, then read the produced JSON and check the wall is gone ----
    r.section("Verify: invoke + inspect output")
    lam = boto3.client("lambda", region_name=CFG["region"])
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    r.log(f"invoke status={inv.get('StatusCode')} err={inv.get('FunctionError')}")
    body = inv["Payload"].read().decode()
    r.log(f"invoke body: {body[:300]}")

    time.sleep(3)
    s3 = boto3.client("s3", region_name=CFG["region"])
    obj = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)
    payload = json.loads(obj["Body"].read().decode())

    fin = payload.get("financing_rate_pct")
    equities = [a for a in payload.get("all_assets", []) if a.get("asset_class") == "equity"]
    live = [a for a in equities if a.get("carry_pct") is not None and not a.get("dormant")]
    dormant = [a for a in equities if a.get("dormant")]
    # count how many live equities sit EXACTLY at -financing (the old wall signature)
    wall = [a for a in live if a.get("carry_pct") == round(-fin, 3)]
    r.log(f"version={payload.get('version')} financing={fin}%")
    r.log(f"equity: {len(live)} live, {len(dormant)} dormant, {len(wall)} exactly at -financing")
    r.log(f"n_dormant(payload)={payload.get('n_dormant')}")

    # SPY specifically
    spy = next((a for a in equities if a.get("symbol") == "SPY"), None)
    r.log(f"SPY: {json.dumps({k: spy.get(k) for k in ('carry_pct','div_yield_pct','dormant','dormant_reason')}) if spy else 'MISSING'}")

    # dislocation status sample
    disl = payload.get("dislocation_leaders", [])
    statuses = {}
    for a in payload.get("all_assets", []):
        statuses[a.get("dislocation_status", "n/a")] = statuses.get(a.get("dislocation_status", "n/a"), 0) + 1
    r.log(f"dislocation_status counts: {statuses}")
    r.log(f"dislocation_leaders (top3): {[ (a.get('symbol'), a.get('carry_own_z')) for a in disl[:3] ]}")

    # PASS criteria: the wall must be materially reduced. Before: ~40 equities at -financing.
    # After: only genuine non-payers may sit there; everything else is priced or dormant.
    if len(wall) <= 8:
        r.ok(f"WALL CLEARED — only {len(wall)} live equities at exactly -financing (was ~40).")
    else:
        r.fail(f"WALL PERSISTS — {len(wall)} live equities still at -financing; investigate div path.")

    if payload.get("version") == "1.1.0":
        r.ok("version 1.1.0 live")
    else:
        r.fail(f"version mismatch: {payload.get('version')}")
