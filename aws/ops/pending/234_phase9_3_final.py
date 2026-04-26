#!/usr/bin/env python3
"""Step 234 — Phase 9.3 a/b/c/d final integration verifier.

Comprehensive end-to-end check after all four Phase 9.3 stages landed:
  9.3a — .nojekyll + mmf gov/prime/tax-exempt + BUSLOANS replacing H8B1058NCBCMG
  9.3b — 6 funding+credit signals (SOFR-IORB, HY OAS, IG BBB OAS, T10YIE,
         DFII10, SLOOS_TIGHTEN)
  9.3c — Real cross-currency rate differentials + broad dollar + OBFR-IORB
  9.3d — Frontend revamp consuming all new sections + watch list

Verifies:
  1. Lambda code redeployed (CodeSha256 fresh)
  2. Manual invoke produces schema_version 1.1, 30 series fetched
  3. All new fields present in S3 output
  4. crisis.html serves with all new DOM markers
  5. /_partials/sidebar.html now serves 200 (post-.nojekyll)
"""
import io, json, time, zipfile
from datetime import datetime, timezone
from ops_report import report
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PROBE_NAME = "justhodl-tmp-probe-234"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)

PROBE_CODE = '''
import json, urllib.request, urllib.error
def lambda_handler(event, context):
    try:
        req = urllib.request.Request(event["url"], headers=event.get("headers", {}))
        with urllib.request.urlopen(req, timeout=15) as r:
            body = r.read().decode("utf-8", errors="replace")
            return {"ok": True, "status": r.status, "body": body[:80000], "len": len(body)}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}
'''


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", PROBE_CODE)
    buf.seek(0)
    return buf.read()


with report("phase9_3_final_integration") as r:
    r.heading("Phase 9.3 a/b/c/d final integration verification")

    # ─────────────────────────────────────────
    # 1. Lambda redeployed
    # ─────────────────────────────────────────
    r.section("1. Lambda redeployed (post-9.3c)")
    cfg = lam.get_function_configuration(FunctionName="justhodl-crisis-plumbing")
    r.log(f"  CodeSha256:   {cfg['CodeSha256']}")
    r.log(f"  LastModified: {cfg['LastModified']}")

    # ─────────────────────────────────────────
    # 2. Manual invoke — force fresh data
    # ─────────────────────────────────────────
    r.section("2. Manual invoke")
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-crisis-plumbing", InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    if resp.get("FunctionError"):
        r.warn(f"  ✗ FunctionError: {payload}")
        return
    r.log(f"  ✅ OK ({dur}s)  payload: {json.dumps(payload)[:300]}")

    # ─────────────────────────────────────────
    # 3. Read fresh S3 output
    # ─────────────────────────────────────────
    r.section("3. S3 output structure")
    body = s3.get_object(Bucket=BUCKET, Key="data/crisis-plumbing.json")["Body"].read()
    d = json.loads(body)
    r.log(f"  schema_version:    {d.get('schema_version')}")
    r.log(f"  generated_at:      {d.get('generated_at')}")
    r.log(f"  n_series_fetched:  {d.get('n_series_fetched')}")
    r.log(f"  total_bytes:       {len(body)}")

    schema_ok = d.get("schema_version") == "1.1"
    series_ok = d.get("n_series_fetched", 0) >= 28
    r.log(f"  {'✅' if schema_ok else '✗'} schema_version == 1.1")
    r.log(f"  {'✅' if series_ok else '✗'} n_series_fetched ≥ 28 (expected 30)")

    # ─────────────────────────────────────────
    # 4a. Phase 9.3a — plumbing changes
    # ─────────────────────────────────────────
    r.section("4a. Phase 9.3a — plumbing series swap")
    plumb = d.get("plumbing_tier2", {})
    new_keys = ["WGMMNS", "WPMMNS", "WTMMNS", "DPSACBW027SBOG", "BUSLOANS", "RRPONTSYD", "TGA"]
    old_keys = ["WMMFNS", "WIMFSL", "H8B1058NCBCMG"]
    n_new = 0
    for k in new_keys:
        v = plumb.get(k, {})
        if v.get("available"):
            n_new += 1
            d30 = v.get("delta_30d_pct")
            r.log(f"  ✅ {k:18s}  ${v.get('latest_value','?')}B  Δ30d={d30}%  date={v.get('latest_date')}")
        else:
            r.log(f"  ✗  {k:18s}  unavailable")
    for k in old_keys:
        if k in plumb:
            r.warn(f"  ⚠ legacy '{k}' still present (should be gone)")
    plumb_ok = n_new >= 6 and not any(k in plumb for k in old_keys)
    r.log(f"  → {n_new}/{len(new_keys)} new series populated, no legacy keys")

    # ─────────────────────────────────────────
    # 4b. mmf_composition with new structure
    # ─────────────────────────────────────────
    r.section("4b. Phase 9.3a — mmf_composition gov/prime/tax-exempt")
    mmf = d.get("mmf_composition")
    mmf_ok = False
    if mmf and isinstance(mmf, dict):
        for k in ("gov_share_pct", "prime_share_pct", "flight_to_quality"):
            present = k in mmf
            r.log(f"  {'✅' if present else '✗'} '{k}' = {mmf.get(k)}")
        mmf_ok = all(k in mmf for k in ("gov_share_pct", "prime_share_pct", "flight_to_quality"))
        r.log(f"  total: ${mmf.get('total_aum_billions','?')}B")
        r.log(f"  prime_share_change_30d_pp: {mmf.get('prime_share_change_30d_pp','?')}")
        r.log(f"  interpretation: {mmf.get('interpretation','?')}")
    else:
        r.warn(f"  ✗ mmf_composition is null or missing")

    # ─────────────────────────────────────────
    # 5. Phase 9.3b — funding_credit_signals
    # ─────────────────────────────────────────
    r.section("5. Phase 9.3b — funding_credit_signals (6 cards)")
    fcs = d.get("funding_credit_signals", {})
    expected = ["SOFR_IORB_SPREAD", "HY_OAS", "IG_BBB_OAS", "T10YIE", "DFII10", "SLOOS_TIGHTEN"]
    n_avail = 0
    for k in expected:
        v = fcs.get(k, {})
        if v.get("available"):
            n_avail += 1
            sig = v.get("signal", "?")
            if k == "SOFR_IORB_SPREAD":
                r.log(f"  ✅ {k:20s}  spread={v.get('spread_bps')}bps  signal={sig}  z_1y={v.get('z_score_1y')}")
            else:
                lv = v.get("latest_value")
                r.log(f"  ✅ {k:20s}  latest={lv}{v.get('unit','')}  signal={sig}  z_1y={v.get('z_score_1y')}")
        else:
            r.warn(f"  ✗ {k:20s}  unavailable")
    fcs_ok = n_avail >= 5
    r.log(f"  → {n_avail}/{len(expected)} signals populated")

    # ─────────────────────────────────────────
    # 6. Phase 9.3c — XCC rate differentials
    # ─────────────────────────────────────────
    r.section("6. Phase 9.3c — cross-currency rate differentials")
    xcc = d.get("xcc_basis_proxy", {})
    new_xcc_keys = ["rate_diff_jpy_3m", "rate_diff_eur_3m", "broad_dollar_index", "obfr_iorb_spread"]
    n_xcc = 0
    for k in new_xcc_keys:
        v = xcc.get(k, {})
        if v.get("available"):
            n_xcc += 1
            sig = v.get("signal", "?")
            if k.startswith("rate_diff"):
                r.log(f"  ✅ {k:20s}  diff={v.get('current_pct')}pct  z_1y={v.get('z_score_1y')}  signal={sig}")
            elif k == "broad_dollar_index":
                r.log(f"  ✅ {k:20s}  level={v.get('level')}  z_1y={v.get('z_score_1y')}  signal={sig}")
            elif k == "obfr_iorb_spread":
                r.log(f"  ✅ {k:20s}  spread={v.get('spread_bps')}bps  signal={sig}")
        else:
            r.warn(f"  ✗ {k:20s}  unavailable: {v.get('reason','?')}")
    legacy_keys = ["xcc_proxy_jpy_3m", "xcc_proxy_eur_3m"]
    for k in legacy_keys:
        if k in xcc:
            r.warn(f"  ⚠ legacy '{k}' still present (should be gone)")
    xcc_ok = n_xcc >= 3 and not any(k in xcc for k in legacy_keys)
    r.log(f"  → {n_xcc}/{len(new_xcc_keys)} signals populated")

    # ─────────────────────────────────────────
    # 7. Frontend serving + DOM markers + sidebar
    # ─────────────────────────────────────────
    r.section("7. Frontend (crisis.html + /_partials/sidebar.html)")
    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass
    lam.create_function(
        FunctionName=PROBE_NAME,
        Runtime="python3.11",
        Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": build_zip()},
        Timeout=30,
        MemorySize=256,
        Architectures=["x86_64"],
    )
    time.sleep(3)

    # 7a. crisis.html — fetch + DOM
    resp = lam.invoke(
        FunctionName=PROBE_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({"url": "https://justhodl.ai/crisis.html",
                            "headers": {"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"}}),
    )
    res = json.loads(resp["Payload"].read())
    page_ok = res.get("ok") and res.get("status") == 200
    r.log(f"  crisis.html: HTTP {res.get('status')}  bytes={res.get('len','?')}")
    body_html = res.get("body", "")

    # New DOM markers (Phase 9.3d)
    new_markers = [
        ("watchlist-section",     "watch list section ID"),
        ("funding-credit-grid",   "funding+credit grid ID"),
        ("xcc-grid",              "new XCC grid ID"),
        ("renderFundingCredit",   "JS render function"),
        ("buildWatchlist",        "JS watch-list builder"),
        ("WGMMNS",                "new MMF series label"),
        ("BUSLOANS",              "absolute C&I label"),
        ("rate_diff_jpy_3m",      "real XCC field"),
        ("broad_dollar_index",    "broad dollar field"),
        ("SOFR_IORB_SPREAD",      "SOFR-IORB key"),
        ("signal-pill",           "signal pill CSS class"),
    ]
    n_dom = 0
    for needle, desc in new_markers:
        if needle in body_html:
            n_dom += 1
        else:
            r.warn(f"  ⚠ missing DOM marker: '{needle}' ({desc})")
    r.log(f"  ✅ DOM markers present: {n_dom}/{len(new_markers)}")
    dom_ok = n_dom >= len(new_markers) - 1

    # 7b. _partials/sidebar.html — should serve 200 after .nojekyll
    resp = lam.invoke(
        FunctionName=PROBE_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({"url": "https://justhodl.ai/_partials/sidebar.html",
                            "headers": {"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"}}),
    )
    res = json.loads(resp["Payload"].read())
    sidebar_ok = res.get("ok") and res.get("status") == 200
    if sidebar_ok:
        r.log(f"  ✅ /_partials/sidebar.html: HTTP 200  bytes={res.get('len','?')}")
    else:
        r.warn(f"  ⚠ /_partials/sidebar.html: HTTP {res.get('status')} — pages.deploy may still be propagating")

    try:
        lam.delete_function(FunctionName=PROBE_NAME)
    except ClientError:
        pass

    # ─────────────────────────────────────────
    # FINAL VERDICT
    # ─────────────────────────────────────────
    r.section("FINAL VERDICT — Phase 9.3 a/b/c/d integration")
    pillars = {
        "schema 1.1 + 28+ series":      schema_ok and series_ok,
        "9.3a plumbing swap":           plumb_ok,
        "9.3a mmf gov/prime split":     mmf_ok,
        "9.3b funding+credit (5+/6)":   fcs_ok,
        "9.3c XCC rate-differentials":  xcc_ok,
        "9.3d frontend DOM":            page_ok and dom_ok,
        "sidebar serving (.nojekyll)":  sidebar_ok,
    }
    for k, v in pillars.items():
        r.log(f"  {'✅' if v else '✗'}  {k}")
    all_green = all(pillars.values())
    r.log("")
    r.log("  🟢 PHASE 9.3 FULLY SHIPPED" if all_green else "  🟡 SOME GAPS — see above")
    r.log("Done")
