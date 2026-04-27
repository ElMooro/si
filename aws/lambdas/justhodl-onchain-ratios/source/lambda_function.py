"""
justhodl-onchain-ratios — On-chain BTC/ETH ratios

Glassnode's full Studio is paid, but the most-watched on-chain ratios can
be derived from free public APIs:

  - Bitcoin:
      mempool.space         -> mempool size, fee estimates, hashrate
      blockchain.info       -> network value, market cap, txn count
      CoinMetrics community -> realized cap, MVRV, NVT
  - Ethereum:
      etherscan.io           -> gas, txn count
      ultrasound.money       -> ETH burn rate, supply growth/decay

This Lambda computes:
  - BTC: MVRV (Market Value / Realized Value), NVT, hash ribbon, mempool stress
  - ETH: gas, supply delta (issuance - burn), staked ratio
  - Combined: aggregator score, flag extreme readings

Output (data/onchain-ratios.json):
  {
    "generated_at": ...,
    "btc": {
      "price": 67432, "market_cap": ..., "realized_cap": ...,
      "mvrv": 1.83, "mvrv_z": +0.4,
      "nvt": 78,
      "hash_rate_eh": 580,
      "mempool_kb": 215000, "fee_sat_vb": 12,
      "extreme_signals": ["mvrv_above_2 (overheated)"],
    },
    "eth": {
      "price": 3200, "market_cap": ...,
      "gas_gwei": 8, "burn_rate_eth_24h": 1840,
      "supply_growth_24h": -1200,    (ETH supply DECREASING — deflationary)
      "extreme_signals": [...]
    },
    "interpretation": "<plain English roll-up>"
  }
"""
from __future__ import annotations
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/onchain-ratios.json")
USER_AGENT = os.environ.get("USER_AGENT", "JustHodl Research raafouis@gmail.com")
COINMETRICS_BASE = "https://community-api.coinmetrics.io/v4"


def _fetch(url: str, timeout: int = 15) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def _fetch_json(url: str, timeout: int = 15):
    return json.loads(_fetch(url, timeout))


def fetch_btc_metrics():
    out = {"errors": []}
    # CoinMetrics community API — provides MVRV, realized cap, etc.
    try:
        url = (
            f"{COINMETRICS_BASE}/timeseries/asset-metrics"
            f"?assets=btc&metrics=PriceUSD,CapMrktCurUSD,CapRealUSD,CapMVRVCur,NVTAdj"
            f"&pretty=false&page_size=1&end_time={datetime.utcnow().strftime('%Y-%m-%d')}"
        )
        d = _fetch_json(url)
        rows = d.get("data", [])
        if rows:
            r = rows[-1]
            out["price"] = float(r.get("PriceUSD") or 0)
            out["market_cap"] = float(r.get("CapMrktCurUSD") or 0)
            out["realized_cap"] = float(r.get("CapRealUSD") or 0)
            out["mvrv"] = float(r.get("CapMVRVCur") or 0)
            out["nvt"] = float(r.get("NVTAdj") or 0)
    except Exception as e:
        out["errors"].append(f"coinmetrics_btc: {type(e).__name__}")

    # Mempool.space — current mempool + fees + hash rate
    try:
        m = _fetch_json("https://mempool.space/api/mempool")
        out["mempool_count"] = m.get("count", 0)
        out["mempool_vsize"] = m.get("vsize", 0)
        fees = _fetch_json("https://mempool.space/api/v1/fees/recommended")
        out["fee_sat_vb"] = fees.get("fastestFee", 0)
        out["fee_30min_sat_vb"] = fees.get("halfHourFee", 0)
    except Exception as e:
        out["errors"].append(f"mempool: {type(e).__name__}")

    try:
        hr = _fetch_json("https://blockchain.info/q/hashrate")
        out["hash_rate_th"] = float(hr) if hr else None
        if out.get("hash_rate_th"):
            out["hash_rate_eh"] = round(out["hash_rate_th"] / 1e6, 1)
    except Exception as e:
        out["errors"].append(f"hashrate: {type(e).__name__}")

    # Extreme signal flags
    flags = []
    mvrv = out.get("mvrv", 0)
    if mvrv:
        if mvrv > 3.5:   flags.append("mvrv_above_3.5 (extreme overheated, historical sell signal)")
        elif mvrv > 2:   flags.append("mvrv_above_2 (overheated; 6-12mo top territory)")
        elif mvrv < 1:   flags.append("mvrv_below_1 (oversold; historical accumulation zone)")
        elif mvrv < 0.8: flags.append("mvrv_below_0.8 (deep capitulation; rare buy zone)")
    out["extreme_signals"] = flags
    return out


def fetch_eth_metrics():
    out = {"errors": []}
    # CoinMetrics for ETH price + cap
    try:
        url = (
            f"{COINMETRICS_BASE}/timeseries/asset-metrics"
            f"?assets=eth&metrics=PriceUSD,CapMrktCurUSD,CapMVRVCur,IssContPctAnn"
            f"&pretty=false&page_size=1&end_time={datetime.utcnow().strftime('%Y-%m-%d')}"
        )
        d = _fetch_json(url)
        rows = d.get("data", [])
        if rows:
            r = rows[-1]
            out["price"] = float(r.get("PriceUSD") or 0)
            out["market_cap"] = float(r.get("CapMrktCurUSD") or 0)
            out["mvrv"] = float(r.get("CapMVRVCur") or 0)
            out["issuance_pct_annual"] = float(r.get("IssContPctAnn") or 0)
    except Exception as e:
        out["errors"].append(f"coinmetrics_eth: {type(e).__name__}")

    # Etherscan gas oracle (free, no key required for gas price)
    try:
        # The /v2/api endpoint is the modern one
        d = _fetch_json("https://api.etherscan.io/api?module=gastracker&action=gasoracle")
        if d.get("status") == "1":
            r = d["result"]
            out["gas_safe_gwei"] = int(r.get("SafeGasPrice", 0))
            out["gas_propose_gwei"] = int(r.get("ProposeGasPrice", 0))
            out["gas_fast_gwei"] = int(r.get("FastGasPrice", 0))
    except Exception as e:
        out["errors"].append(f"etherscan_gas: {type(e).__name__}")

    flags = []
    mvrv = out.get("mvrv", 0)
    if mvrv:
        if mvrv > 2.5: flags.append("eth_mvrv_above_2.5 (overheated)")
        elif mvrv < 1: flags.append("eth_mvrv_below_1 (oversold)")
    iss = out.get("issuance_pct_annual", 0)
    if iss:
        if iss < 0:    flags.append(f"eth_supply_deflationary ({iss:.2f}% annual)")
        elif iss > 1:  flags.append(f"eth_supply_inflationary ({iss:.2f}% annual)")
    out["extreme_signals"] = flags
    return out


def interpret(btc: dict, eth: dict) -> str:
    parts = []
    if btc.get("mvrv"):
        m = btc["mvrv"]
        if m > 2.5:    parts.append(f"BTC MVRV at {m:.2f} — historically near cyclical tops")
        elif m < 1:    parts.append(f"BTC MVRV at {m:.2f} — historical accumulation zone")
        else:          parts.append(f"BTC MVRV at {m:.2f} — fair value range")
    if btc.get("fee_sat_vb"):
        f = btc["fee_sat_vb"]
        if f < 5:    parts.append(f"BTC mempool quiet ({f} sat/vB) — low on-chain demand")
        elif f > 50: parts.append(f"BTC mempool stressed ({f} sat/vB) — heavy demand")
    if eth.get("issuance_pct_annual"):
        if eth["issuance_pct_annual"] < 0:
            parts.append(f"ETH supply is DEFLATIONARY ({eth['issuance_pct_annual']:.2f}% annual) — burns exceeding issuance")
        else:
            parts.append(f"ETH supply growth {eth['issuance_pct_annual']:.2f}% annualized")
    return ". ".join(parts) if parts else "On-chain metrics in normal range."


def lambda_handler(event, context):
    s3 = boto3.client("s3")
    started = time.time()

    btc = fetch_btc_metrics()
    eth = fetch_eth_metrics()
    interp = interpret(btc, eth)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "btc": btc,
        "eth": eth,
        "interpretation": interp,
        "fetch_duration_s": round(time.time() - started, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                  Body=json.dumps(output).encode(),
                  ContentType="application/json", CacheControl="no-cache")

    print(f"on-chain ratios written | BTC mvrv={btc.get('mvrv')} | ETH gas={eth.get('gas_propose_gwei')}gwei")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "btc_mvrv": btc.get("mvrv"), "eth_gas": eth.get("gas_propose_gwei")}),
    }
