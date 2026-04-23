#!/usr/bin/env python3
"""Check the v2.2 scan actually populated liquidity and fixed sector rotation."""
import json, time
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


with report("verify_v22_live") as r:
    r.heading("v2.2 verification — did FRED + liquidity + sector all come through?")

    # Wait for the async scan to complete
    r.log("Waiting 45s for scan to complete…")
    time.sleep(45)

    obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
    data = json.loads(obj["Body"].read().decode())

    r.section("Metadata")
    r.log(f"  version: {data.get('version')}")
    r.log(f"  timestamp: {data.get('timestamp')}")
    r.log(f"  scan_time_seconds: {data.get('scan_time_seconds')}")

    r.section("Liquidity block")
    liq = data.get("liquidity", {})
    for k in ("net_liquidity", "regime", "fed_balance_sheet", "rrp", "tga", "reserves", "sofr"):
        r.log(f"  {k}: {liq.get(k)}")
    if liq.get("error"):
        r.warn(f"  error: {liq.get('error')}")
    r.kv(check="liquidity", net_liq=liq.get("net_liquidity"), regime=liq.get("regime"))

    r.section("FRED series")
    fred = data.get("fred", {})
    from_cache = sum(1 for v in fred.values() if isinstance(v, dict) and v.get("_from_cache"))
    r.log(f"  Series populated: {len(fred)}")
    r.log(f"  Of which from cache: {from_cache}")
    for key in ("WALCL", "RRPONTSYD", "WTREGEN", "VIXCLS", "NAPM", "CPIAUCSL", "DTWEXBGS"):
        v = fred.get(key)
        if v:
            src = "CACHED" if v.get("_from_cache") else "LIVE"
            r.log(f"  {key}: value={v.get('value')} date={v.get('date')} [{src}]")
        else:
            r.log(f"  {key}: ❌ missing")
    r.kv(check="fred", series_populated=len(fred), from_cache=from_cache)

    r.section("fred-cache.json existence")
    try:
        c = s3.get_object(Bucket=BUCKET, Key="data/fred-cache.json")
        r.ok(f"  Cache file: {c['ContentLength']} bytes, modified {c['LastModified'].isoformat()}")
        cache_data = json.loads(c["Body"].read().decode())
        r.log(f"  Cache contains {len(cache_data)} series")
        r.kv(check="cache-file", size=c["ContentLength"], series=len(cache_data))
    except Exception as e:
        r.warn(f"  Cache file not yet created: {e}")
        r.kv(check="cache-file", status="missing")

    r.section("Sector rotation card")
    sr = (data.get("tier2") or {}).get("sector_rotation") or {}
    r.log(f"  Keys: {list(sr.keys())}")
    if sr.get("top_inflow"):
        r.log(f"  Top inflow: {sr.get('top_inflow_name')} ({sr.get('top_inflow')}) → ${sr.get('top_inflow_flow')}M")
    if sr.get("top_outflow"):
        r.log(f"  Top outflow: {sr.get('top_outflow_name')} ({sr.get('top_outflow')}) → ${sr.get('top_outflow_flow')}M")
    if sr.get("rotation_signal"):
        r.log(f"  Rotation signal: {sr.get('rotation_signal')}")
    r.kv(check="sector_rotation",
         has_inflow=bool(sr.get("top_inflow")),
         has_outflow=bool(sr.get("top_outflow")))

    r.section("AI briefing — first 30 lines")
    ai = data.get("ai_briefing", "")
    r.log(f"  Length: {len(ai)} chars")
    for line in ai.splitlines()[:30]:
        r.log(f"    {line[:180]}")

    r.log("Done")
