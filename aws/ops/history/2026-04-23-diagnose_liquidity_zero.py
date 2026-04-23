#!/usr/bin/env python3
"""
Diagnose why v2.1 AI briefing said 'Net liquidity flat at $0B' when
daily-report-v3 shows $5,954B.

Possible causes:
  1. FRED rate-limited and WALCL/RRP/TGA didn't fetch → calc_liquidity
     returns 0 silently
  2. calc_liquidity treats missing values as 0 without flagging
  3. Secretary and daily-report reading different series
  4. Temporary API outage during that specific scan

Check:
  - secretary-latest.json → liquidity block (what the Lambda computed)
  - fred block inside secretary-latest.json (what FRED actually returned)
  - Compare to data/report.json (daily-report-v3's view)
"""

import json
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("diagnose_liquidity_zero") as r:
    r.heading("Why did secretary v2.1 report net_liquidity as $0B?")

    # Pull secretary-latest.json
    r.section("1. secretary-latest.json liquidity block")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
        sec = json.loads(obj["Body"].read().decode())
        liq = sec.get("liquidity", {})
        r.log(f"  version: {sec.get('version')}")
        r.log(f"  timestamp: {sec.get('timestamp')}")
        r.log(f"  net_liquidity: ${liq.get('net_liquidity')}B")
        r.log(f"  net_liq_change_1m: ${liq.get('net_liq_change_1m')}B")
        r.log(f"  regime: {liq.get('regime')}")
        r.log(f"  fed_balance_sheet: ${liq.get('fed_balance_sheet')}B")
        r.log(f"  rrp: ${liq.get('rrp')}B")
        r.log(f"  tga: ${liq.get('tga')}B")
        r.log(f"  reserves: ${liq.get('reserves')}B")
        r.log(f"  sofr: {liq.get('sofr')}")
        r.kv(source="secretary-latest.json", net_liq=liq.get("net_liquidity"), regime=liq.get("regime"))
    except Exception as e:
        r.fail(f"  Fetch failed: {e}")

    # Pull FRED block from secretary
    r.section("2. FRED data snapshot (from secretary-latest.json)")
    fred_in_sec = sec.get("fred", {})
    r.log(f"  Total series fetched: {len(fred_in_sec)}")
    for key in ("WALCL", "RRPONTSYD", "WTREGEN", "WRESBAL", "SOFR", "VIXCLS", "DGS10", "NAPM"):
        v = fred_in_sec.get(key)
        if v:
            r.log(f"  {key} ({v.get('name', '?')}): value={v.get('value')} date={v.get('date')} chg_1d={v.get('chg_1d')}")
        else:
            r.log(f"  {key}: ❌ NOT PRESENT IN SCAN")
    r.kv(source="fred_in_secretary", walcl_present=bool(fred_in_sec.get("WALCL")),
         rrp_present=bool(fred_in_sec.get("RRPONTSYD")),
         tga_present=bool(fred_in_sec.get("WTREGEN")),
         series_count=len(fred_in_sec))

    # Pull data/report.json
    r.section("3. data/report.json (daily-report-v3 view)")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/report.json")
        rep = json.loads(obj["Body"].read().decode())
        r.log(f"  Keys: {list(rep.keys())[:15]}...")
        # FRED
        fred_in_rep = rep.get("fred", {})
        r.log(f"  FRED series: {len(fred_in_rep)}")
        for key in ("WALCL", "RRPONTSYD", "WTREGEN"):
            v = fred_in_rep.get(key)
            if isinstance(v, dict):
                r.log(f"  {key}: current={v.get('current')} date={v.get('date', 'N/A')}")
            else:
                r.log(f"  {key}: {v}")
        # Liquidity
        liq_r = rep.get("liquidity", {})
        if liq_r:
            r.log(f"  Liquidity block: {json.dumps(liq_r)[:300]}")
        r.kv(source="data/report.json",
             walcl_current=(fred_in_rep.get("WALCL") or {}).get("current") if isinstance(fred_in_rep.get("WALCL"), dict) else fred_in_rep.get("WALCL"),
             rrp_current=(fred_in_rep.get("RRPONTSYD") or {}).get("current") if isinstance(fred_in_rep.get("RRPONTSYD"), dict) else fred_in_rep.get("RRPONTSYD"),
             generated=rep.get("generated", rep.get("generated_at")))
    except Exception as e:
        r.fail(f"  Fetch failed: {e}")

    # Raw FRED test — fetch WALCL directly right now
    r.section("4. Live FRED API test (WALCL right now)")
    import urllib.request, urllib.error, ssl, os
    FRED_KEY = "2f057499936072679d8843d7fce99989"
    url = f"https://api.stlouisfed.org/fred/series/observations?series_id=WALCL&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=5"
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        with urllib.request.urlopen(url, timeout=10, context=ctx) as resp:
            data = json.loads(resp.read().decode())
        obs = data.get("observations", [])
        r.log(f"  Observations returned: {len(obs)}")
        for o in obs[:3]:
            r.log(f"    {o.get('date')}: {o.get('value')}")
        r.kv(source="live_fred_walcl",
             latest_date=obs[0].get("date") if obs else None,
             latest_value=obs[0].get("value") if obs else None)
    except Exception as e:
        r.fail(f"  Live FRED fetch failed: {e}")

    r.log("Done")
