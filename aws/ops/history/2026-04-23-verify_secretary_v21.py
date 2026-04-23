#!/usr/bin/env python3
"""Verify secretary-latest.json now contains tier2 data."""
import json, time
from ops_report import report
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

with report("verify_secretary_v21") as r:
    r.heading("Verify Secretary v2.1 scan output")

    # Wait for the async scan to finish (it usually takes ~45s)
    r.log("Waiting 45s for async scan to complete…")
    time.sleep(45)

    obj = s3.get_object(Bucket=BUCKET, Key="data/secretary-latest.json")
    data = json.loads(obj["Body"].read().decode())

    r.section("Top-level scan metadata")
    r.log(f"  version: {data.get('version')}")
    r.log(f"  timestamp: {data.get('timestamp')}")
    r.log(f"  scan_time_seconds: {data.get('scan_time_seconds')}")
    r.log(f"  recommendations: {len(data.get('recommendations', []))}")
    r.log(f"  top_buys: {len(data.get('top_buys', []))}")
    r.log(f"  has tier2 key: {'tier2' in data}")
    r.log(f"  has deltas key: {'deltas' in data}")
    r.log(f"  has cftc key: {'cftc' in data}")

    tier2 = data.get("tier2") or {}
    r.section("tier2.options")
    opts = tier2.get("options") or {}
    if opts:
        r.log(f"  put_call_ratio: {opts.get('put_call_ratio')}")
        r.log(f"  pc_signal: {opts.get('pc_signal')}")
        r.log(f"  gamma_regime: {opts.get('gamma_regime')}")
        r.log(f"  max_gamma_strike: {opts.get('max_gamma_strike')}")
        r.log(f"  spy_price: {opts.get('spy_price')}")
        r.log(f"  trading_signals count: {len(opts.get('trading_signals', []))}")
        for s in opts.get("trading_signals", [])[:3]:
            r.log(f"    - {s.get('type')} ({s.get('strength')}): {s.get('message', '')[:80]}")
        r.kv(card="options", present="yes", signals=len(opts.get("trading_signals", [])))
    else:
        r.warn("  No options data — card won't render")
        r.kv(card="options", present="no")

    r.section("tier2.crypto")
    crypto = tier2.get("crypto") or {}
    if crypto:
        r.log(f"  btc_dominance: {crypto.get('btc_dominance')}")
        r.log(f"  total_mcap_fmt: {crypto.get('total_mcap_fmt')}")
        r.log(f"  mcap_change_24h: {crypto.get('mcap_change_24h')}%")
        r.log(f"  stablecoin_net_signal: {crypto.get('stablecoin_net_signal')}")
        r.log(f"  fear_greed_value: {crypto.get('fear_greed_value')} ({crypto.get('fear_greed_label')})")
        r.log(f"  risk_score: {crypto.get('risk_score')}")
        r.log(f"  top_movers count: {len(crypto.get('top_movers', []))}")
        for m in (crypto.get("top_movers") or [])[:5]:
            r.log(f"    - {m.get('symbol')}: ${m.get('price')} ({m.get('change_24h', 0):+.2f}%)")
        r.kv(card="crypto", present="yes", movers=len(crypto.get("top_movers", [])))
    else:
        r.warn("  No crypto data — card won't render")
        r.kv(card="crypto", present="no")

    r.section("tier2.sector_rotation")
    sr = tier2.get("sector_rotation") or {}
    if sr:
        r.log(f"  keys: {list(sr.keys())}")
        for k in ("leaders", "top", "laggards", "bottom"):
            v = sr.get(k, [])
            if v:
                r.log(f"  {k}: {len(v)} entries")
                for e in v[:3]:
                    if isinstance(e, dict):
                        r.log(f"    - {e}")
        r.kv(card="sector_rotation", present="yes")
    else:
        r.warn("  No sector_rotation data")
        r.kv(card="sector_rotation", present="no")

    r.section("AI briefing snippet")
    ai = data.get("ai_briefing", "")
    r.log(f"  length: {len(ai)} chars")
    # Show first 2000 chars so we can see if tier2 info surfaced
    for line in ai.splitlines()[:40]:
        r.log(f"    {line[:180]}")

    r.log("Done")
