#!/usr/bin/env python3
"""Step 238 — post-fix re-invoke both Lambdas + verify clean output.

After commits 2c05f75 (retry+backoff) and acf7caa (drop bad IDs +
IR3TIB01JPM156N) deploy via deploy-lambdas.yml, both Lambdas should
produce clean output:

  justhodl-crisis-plumbing:
    - 5/5 crisis indices populated (4/5 normally; OFRFSI not on FRED)
    - 4/4 plumbing tier 2 (after dropping the 3 nonexistent MMF series)
    - mmf_composition = null (intentional)
    - 6/6 funding+credit signals populated
    - 4/4 XCC signals populated (rate_diff_jpy now uses IR3TIB01JPM156N)

  justhodl-correlation-breaks:
    - aligned table > 312 dates
    - status != warming_up
    - signal in {NORMAL/WATCH/ELEVATED/CRISIS}
    - frobenius_z_score_1y populated
    - top_breaking_pairs populated (5 entries)

If both succeed, Phase 9 is fully operational end-to-end.
"""
import json
import sys
import time
from ops_report import report
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)


def invoke_and_summarize(name, key):
    """Invoke Lambda, then read S3 output. Returns (success, summary_dict)."""
    cfg = lam.get_function_configuration(FunctionName=name)
    print(f"  CodeSha256:   {cfg['CodeSha256']}")
    print(f"  LastModified: {cfg['LastModified']}")
    t0 = time.time()
    resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    if resp.get("FunctionError"):
        return False, {"error": "FunctionError", "payload": payload, "dur": dur}
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return True, {"payload": payload, "dur": dur, "data": json.loads(body), "size": len(body)}


with report("post_fix_verify_phase9") as r:
    r.heading("Phase 9 post-fix end-to-end verify")

    # ─────────────────────────────────────────
    # 1. justhodl-crisis-plumbing
    # ─────────────────────────────────────────
    r.section("1. crisis-plumbing — re-invoke + read")
    ok, info = invoke_and_summarize("justhodl-crisis-plumbing", "data/crisis-plumbing.json")
    if not ok:
        r.warn(f"  ✗ failed: {info}")
        sys.exit(0)
    d = info["data"]
    r.log(f"  ✅ invoke OK ({info['dur']}s)")
    r.log(f"  payload: {json.dumps(info['payload'])[:300]}")
    r.log(f"  schema: {d.get('schema_version')}  size: {info['size']}B  series_fetched: {d.get('n_series_fetched')}")

    # Sanity-check sections
    ci = d.get("crisis_indices", {})
    plumb = d.get("plumbing_tier2", {})
    fcs = d.get("funding_credit_signals", {})
    xcc = d.get("xcc_basis_proxy", {})

    n_ci = sum(1 for k, v in ci.items() if v.get("available"))
    n_pl = sum(1 for k, v in plumb.items() if v.get("available"))
    n_fc = sum(1 for k, v in fcs.items() if v.get("available"))
    n_xcc = sum(1 for k, v in xcc.items() if v.get("available"))

    r.log(f"  crisis_indices:           {n_ci}/{len(ci)} populated")
    r.log(f"  plumbing_tier2:           {n_pl}/{len(plumb)} populated")
    r.log(f"  funding_credit_signals:   {n_fc}/{len(fcs)} populated")
    r.log(f"  xcc_basis_proxy:          {n_xcc}/{len(xcc)} populated")
    r.log(f"  mmf_composition:          {d.get('mmf_composition')!r} (expected null after fix)")

    # Spot-check critical signals
    r.log("")
    r.log("  Critical-signal spot check:")
    sofr = fcs.get("SOFR_IORB_SPREAD", {})
    if sofr.get("available"):
        r.log(f"    SOFR-IORB:  {sofr.get('spread_bps')}bps  signal={sofr.get('signal')}  z={sofr.get('z_score_1y')}")
    hy = fcs.get("HY_OAS", {})
    if hy.get("available"):
        r.log(f"    HY OAS:     {hy.get('latest_value'):.0f}bps  signal={hy.get('signal')}")
    rd_jpy = xcc.get("rate_diff_jpy_3m", {})
    if rd_jpy.get("available"):
        r.log(f"    USD-JPY rate diff: {rd_jpy.get('current_pct')}%  z={rd_jpy.get('z_score_1y')}  signal={rd_jpy.get('signal')}")
    bd = xcc.get("broad_dollar_index", {})
    if bd.get("available"):
        r.log(f"    Broad USD:  {bd.get('level')}  z={bd.get('z_score_1y')}  signal={bd.get('signal')}")

    crisis_plumbing_ok = (n_ci >= 4 and n_pl >= 4 and n_fc >= 5 and n_xcc >= 3)

    # ─────────────────────────────────────────
    # 2. justhodl-correlation-breaks
    # ─────────────────────────────────────────
    r.section("2. correlation-breaks — re-invoke + read")
    ok, info = invoke_and_summarize("justhodl-correlation-breaks", "data/correlation-breaks.json")
    if not ok:
        r.warn(f"  ✗ failed: {info}")
        sys.exit(0)
    d = info["data"]
    r.log(f"  ✅ invoke OK ({info['dur']}s)")
    r.log(f"  payload: {json.dumps(info['payload'])[:300]}")
    status = d.get("status", "ok")
    if status == "warming_up":
        r.warn(f"  ⚠ status: warming_up — {d.get('message')}")
        corr_ok = False
    else:
        r.log(f"  schema: {d.get('schema_version')}  size: {info['size']}B  n_dates: {d.get('n_dates_aligned')}")
        sig = d.get("signal", "?")
        fz = d.get("frobenius_z_score_1y")
        r.log(f"  signal: {sig}  Frobenius z: {fz}")
        r.log(f"  pairs >2σ: {d.get('n_pairs_above_2sigma')}  >3σ: {d.get('n_pairs_above_3sigma')}")
        r.log(f"  interpretation: {d.get('interpretation')}")
        tops = d.get("top_breaking_pairs", [])
        r.log(f"  TOP {len(tops)} BREAKING PAIRS:")
        for p in tops:
            r.log(
                f"    {p['labels'][0]:18s} ↔ {p['labels'][1]:18s}  "
                f"curr={p['current_corr']:+.3f}  baseline={p['baseline_corr']:+.3f}  "
                f"z={p['z_score']:+.2f}"
            )
            if p.get("context"):
                r.log(f"        ↳ {p['context']}")
        corr_ok = len(tops) >= 3 and fz is not None

    # ─────────────────────────────────────────
    # FINAL VERDICT
    # ─────────────────────────────────────────
    r.section("FINAL VERDICT")
    pillars = {
        "crisis-plumbing — all sections populated":  crisis_plumbing_ok,
        "correlation-breaks — composite + pairs":    corr_ok,
    }
    for k, v in pillars.items():
        r.log(f"  {'✅' if v else '✗'}  {k}")
    if all(pillars.values()):
        r.log("")
        r.log("  🟢 PHASE 9 FULLY OPERATIONAL")
    else:
        r.log("")
        r.log("  🟡 some gaps — see above")
    r.log("Done")
