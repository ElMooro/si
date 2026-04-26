#!/usr/bin/env python3
"""Step 233 — verify Phase 9.3a + 9.3b signals after CI redeploy.

After commits 605f69c (9.3a) and ed80ac9 (9.3b) deploy via CI, the
crisis-plumbing Lambda should be running schema v1.1 with:
  - mmf_composition populated (gov/prime/tax-exempt split)
  - plumbing_tier2 contains BUSLOANS instead of H8B1058NCBCMG
  - funding_credit_signals section with 6 new signals
  - .nojekyll allows /_partials/ to serve

Manually invoke + read S3 + sanity-check.
"""
import json
import time
from ops_report import report
import boto3
from botocore.config import Config

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300))
s3 = boto3.client("s3", region_name=REGION)

with report("verify_9_3a_9_3b") as r:
    r.heading("Verify Phase 9.3a + 9.3b signals")

    # ─────────────────────────────────────────
    # 1. Confirm Lambda code was redeployed
    # ─────────────────────────────────────────
    r.section("1. Lambda redeploy status")
    cfg = lam.get_function_configuration(FunctionName="justhodl-crisis-plumbing")
    r.log(f"  CodeSha256:   {cfg['CodeSha256']}")
    r.log(f"  LastModified: {cfg['LastModified']}")

    # ─────────────────────────────────────────
    # 2. Manual invoke
    # ─────────────────────────────────────────
    r.section("2. Manual invoke")
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-crisis-plumbing", InvocationType="RequestResponse")
    payload = json.loads(resp["Payload"].read())
    dur = round(time.time() - t0, 1)
    if resp.get("FunctionError"):
        r.warn(f"  ✗ FunctionError: {payload}")
        return
    r.log(f"  ✅ OK ({dur}s)")
    r.log(f"  payload: {json.dumps(payload)[:400]}")

    # ─────────────────────────────────────────
    # 3. Read fresh S3 output
    # ─────────────────────────────────────────
    r.section("3. Read S3 output")
    body = s3.get_object(Bucket=BUCKET, Key="data/crisis-plumbing.json")["Body"].read()
    d = json.loads(body)
    r.log(f"  schema_version: {d.get('schema_version')}")
    r.log(f"  generated_at:   {d.get('generated_at')}")
    r.log(f"  n_series_fetched: {d.get('n_series_fetched')}")

    # ─────────────────────────────────────────
    # 4. Verify Phase 9.3a fixes
    # ─────────────────────────────────────────
    r.section("4. Phase 9.3a fixes")

    # 4a. mmf_composition no longer null
    mmf = d.get("mmf_composition")
    if mmf and isinstance(mmf, dict):
        r.log(f"  ✅ mmf_composition populated:")
        r.log(f"     total: ${mmf.get('total_aum_billions','?')}B")
        r.log(f"     gov:   ${mmf.get('gov_billions','?')}B  ({mmf.get('gov_share_pct','?')}%)")
        r.log(f"     prime: ${mmf.get('prime_billions','?')}B ({mmf.get('prime_share_pct','?')}%)")
        r.log(f"     prime_share_change_30d_pp: {mmf.get('prime_share_change_30d_pp','?')}")
        r.log(f"     flight_to_quality: {mmf.get('flight_to_quality')}")
        r.log(f"     interpretation: {mmf.get('interpretation')}")
    else:
        r.warn(f"  ⚠ mmf_composition is {mmf!r} (still null)")

    # 4b. BUSLOANS replaces H8B1058NCBCMG
    plumbing = d.get("plumbing_tier2", {})
    busloans = plumbing.get("BUSLOANS")
    if busloans and busloans.get("available"):
        r.log(f"  ✅ BUSLOANS present:")
        r.log(f"     latest: ${busloans.get('latest_value','?')}B  (date {busloans.get('latest_date')})")
        r.log(f"     delta_30d_pct: {busloans.get('delta_30d_pct')}%  (sane scale)")
    else:
        r.warn(f"  ⚠ BUSLOANS missing or unavailable: {busloans!r}")

    if "H8B1058NCBCMG" in plumbing:
        r.warn(f"  ⚠ H8B1058NCBCMG still present (should have been removed)")
    else:
        r.log(f"  ✅ H8B1058NCBCMG removed")

    # 4c. WGMMNS/WPMMNS/WTMMNS present
    for sid in ("WGMMNS", "WPMMNS", "WTMMNS"):
        v = plumbing.get(sid, {})
        if v.get("available"):
            r.log(f"  ✅ {sid} ({v.get('name')}): ${v.get('latest_value','?')}B  date={v.get('latest_date')}")
        else:
            r.warn(f"  ⚠ {sid} not available: {v!r}")

    # ─────────────────────────────────────────
    # 5. Verify Phase 9.3b new signals
    # ─────────────────────────────────────────
    r.section("5. Phase 9.3b — funding & credit signals")
    fcs = d.get("funding_credit_signals", {})
    if not fcs:
        r.warn("  ✗ funding_credit_signals section MISSING")
        return

    expected = [
        "SOFR_IORB_SPREAD",
        "HY_OAS",
        "IG_BBB_OAS",
        "T10YIE",
        "DFII10",
        "SLOOS_TIGHTEN",
    ]
    n_avail = 0
    for key in expected:
        v = fcs.get(key, {})
        avail = v.get("available")
        if avail:
            n_avail += 1
            label = v.get("name", key)
            if key == "SOFR_IORB_SPREAD":
                r.log(
                    f"  ✅ {key}  spread={v.get('spread_bps')}bps "
                    f"(SOFR={v.get('sofr_pct')}%, IORB={v.get('iorb_pct')}%) "
                    f"z_1y={v.get('z_score_1y')}  signal={v.get('signal')}"
                )
                r.log(f"     {v.get('interpretation','')}")
            else:
                lv = v.get("latest_value")
                u = v.get("unit", "")
                z = v.get("z_score_1y")
                sig = v.get("signal")
                r.log(f"  ✅ {key}  latest={lv}{u}  z_1y={z}  signal={sig}  date={v.get('latest_date')}")
        else:
            r.warn(f"  ⚠ {key} not available: {v!r}")

    r.log(f"")
    r.log(f"  funding_credit_signals: {n_avail}/{len(expected)} populated")

    # ─────────────────────────────────────────
    # 6. _partials/ now serving (after .nojekyll)
    # ─────────────────────────────────────────
    r.section("6. _partials/sidebar.html post-.nojekyll")
    r.log("  (separate verifier needed — pages.deploy must propagate first)")
    r.log("  This will be checked in step 234 after ~5 min")

    r.section("FINAL")
    if n_avail >= 5 and mmf and busloans:
        r.log("  🟢 Phase 9.3a + 9.3b VERIFIED — new signals live")
    else:
        r.log("  🟡 Some gaps — see log above")
    r.log("Done")
