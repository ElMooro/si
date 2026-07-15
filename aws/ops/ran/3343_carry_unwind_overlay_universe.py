"""ops 3343 — carry-surface v1.3.0: carry-unwind fragility overlay (KMPV crash-risk) +
universe expansion to ~170 assets.

WHAT:
  1. UNWIND OVERLAY — the KMPV crash insight made tradeable. Reads the fleet's fused
     data/risk-regime.json (RORO score + VIX + repo stress) and computes:
       • per-asset fragility 0..100 = 0.45*carry_richness + 0.35*realized_vol
         + 0.20*own_extension, scaled by a live regime multiplier (1.0 calm → 2.0 acute).
         Only positive-carry assets are scored (you're paid to hold → you carry the unwind).
       • cohort gauge = mean fragility of the top-carry decile (the classic carry basket).
       • verdict LOW / ELEVATED / HIGH + fragile/crowded asset lists.
     Surfaced in payload.unwind_overlay and on carry.html as the headline UNWIND RISK board.
  2. UNIVERSE EXPANSION — equities ~50→~110 (more sectors/single names/factor+intl ETFs),
     FX 13→17 pairs (added SEK/NOK/DKK/KOR/IDN/TUR), FI 7→18 (full UST curve + TIPS + IG/HY/
     CCC/EM/AAA credit sleeves), commodities 7→14 (Brent/copper/platinum/palladium/base
     metals/corn/wheat). New ETFs routed through ETF_SYMBOLS dividend path; new non-payers added.

  Offline-tested (test_unwind.py): risk-off escalates fragility, low-carry stays stable,
  negative-carry excluded, cohort gauge correct. ALL PASS.

VERIFY: redeploy, invoke, read data/carry-surface.json and assert:
  (a) unwind_overlay present with a cohort_fragility + verdict,
  (b) n_assets materially higher (>=140),
  (c) crypto still >=6 and no commodity |carry|>70 (regressions guard),
  (d) version 1.3.0.
"""
import json
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
DESCRIPTION = (CFG.get("description") or "")[:256]

with report("3343_carry_unwind_overlay_universe") as r:
    r.section("Deploy carry-surface v1.3.0")
    smoke = deploy_lambda(
        report=r, function_name=FN, source_dir=SRC, env_vars=ENV,
        eb_rule_name=CFG["schedule"]["rule_name"], eb_schedule=CFG["schedule"]["cron"],
        timeout=CFG["timeout"], memory=CFG["memory"], description=DESCRIPTION,
        create_function_url=True, smoke=True,
    )
    r.log(f"smoke: {json.dumps(smoke)[:200] if smoke else 'None'}")

    r.section("Verify: invoke + inspect")
    lam = boto3.client("lambda", region_name=CFG["region"])
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    r.log(f"invoke status={inv.get('StatusCode')} err={inv.get('FunctionError')}")

    time.sleep(3)
    s3 = boto3.client("s3", region_name=CFG["region"])
    payload = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read().decode())

    assets = payload.get("all_assets", [])
    n = payload.get("n_assets")
    by_cls = {}
    for a in assets:
        if a.get("carry_pct") is not None:
            by_cls[a["asset_class"]] = by_cls.get(a["asset_class"], 0) + 1
    crypto = by_cls.get("crypto", 0)
    commod = [a for a in assets if a.get("asset_class") == "commodity" and a.get("carry_pct") is not None]
    over70 = [a for a in commod if abs(a["carry_pct"]) > 70]

    u = payload.get("unwind_overlay") or {}
    rr = u.get("regime") or {}
    r.log(f"version={payload.get('version')} n_assets={n} by_class={by_cls}")
    r.log(f"UNWIND: cohort_fragility={u.get('cohort_fragility')} verdict={str(u.get('verdict'))[:70]}")
    r.log(f"  regime={rr.get('regime')} roro={rr.get('roro_score')} vix={rr.get('vix')} mult={u.get('regime_multiplier')}")
    r.log(f"  n_fragile={u.get('n_fragile')} n_crowded={u.get('n_crowded')}")
    top_frag = (u.get('fragile_assets') or [])[:5]
    r.log(f"  top fragile: {[(a['symbol'], a['unwind_fragility']) for a in top_frag]}")

    ok = True
    if u and "cohort_fragility" in u:
        r.ok(f"UNWIND OVERLAY LIVE — cohort={u.get('cohort_fragility')}, {u.get('n_fragile')} fragile / {u.get('n_crowded')} crowded.")
    else:
        r.fail("unwind_overlay missing from payload."); ok = False

    if rr.get("available"):
        r.ok(f"risk-regime joined: {rr.get('regime')} (RORO {rr.get('roro_score')}, VIX {rr.get('vix')}).")
    else:
        r.log("⚠ risk-regime.json not available — overlay ran on defaults (regime feed may be stale).")

    if n and n >= 140:
        r.ok(f"UNIVERSE EXPANDED — {n} assets ({by_cls}).")
    else:
        r.fail(f"universe smaller than expected: {n}"); ok = False

    if crypto >= 6 and not over70:
        r.ok(f"no regressions — crypto={crypto}, max commodity |carry|={max((abs(a['carry_pct']) for a in commod), default=0):.1f}.")
    else:
        r.fail(f"regression: crypto={crypto}, commodity_over70={[(a['symbol'],a['carry_pct']) for a in over70]}"); ok = False

    if payload.get("version") == "1.3.0":
        r.ok("version 1.3.0 live")
    else:
        r.fail(f"version mismatch: {payload.get('version')}")
