"""justhodl-hyperliquid-perps · v1.0 — cross-venue perp leverage gauge.

Hyperliquid is now a top perpetuals venue with a fully public on-chain API. This engine reads OI,
funding and premium across all ~230 perps and turns them into the leverage signal the fleet was
missing:

  - TOTAL OI (USD) = how much leverage is in the system right now
  - OI-vs-price divergence: rising OI + flat/falling price = leverage BUILDUP (fragile);
    OI collapse while price moves = DELEVERAGING / liquidation cascade
  - funding extremes: which coins are crowded long (positive) / short (negative)
  - premium (mark vs oracle) dislocation = aggressive flow / stress
  - liquidation-pressure PROXY: sharp OI drop + premium dislocation (true per-event liq $ needs the
    WS feed; this captures the on-chain signature of a cascade)

SOURCE: api.hyperliquid.xyz/info metaAndAssetCtxs (free, public). Self-history for OI deltas.
Feeds crypto-intel / cycle-clock / crypto-confluence / morning-intelligence. Central FDR ledger.
"""
import json
import time
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/hyperliquid-perps.json"
HIST_KEY = "data/hyperliquid-perps-history.json"
API = "https://api.hyperliquid.xyz/info"


def _post(body, timeout=20):
    req = urllib.request.Request(API, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json", "User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def lambda_handler(event, context):
    t0 = time.time()
    out = {"generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "version": "1.0"}
    try:
        d = _post({"type": "metaAndAssetCtxs"})
        meta, ctxs = d[0], d[1]
        uni = meta.get("universe", [])
    except Exception as e:
        out["_err"] = str(e)[:120]
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(), ContentType="application/json")
        return {"statusCode": 500, "body": json.dumps({"err": out["_err"]})}

    coins = {}
    total_oi = 0.0
    for i, u in enumerate(uni):
        if i >= len(ctxs):
            break
        c = ctxs[i]
        name = u.get("name")
        mark = _f(c.get("markPx"))
        oi_ntv = _f(c.get("openInterest"))
        oi_usd = oi_ntv * mark
        oracle = _f(c.get("oraclePx"))
        fund_1h = _f(c.get("funding"))
        prem = _f(c.get("premium"))
        total_oi += oi_usd
        coins[name] = {"oi_usd": round(oi_usd), "funding_1h": fund_1h,
                       "funding_ann_pct": round(fund_1h * 24 * 365 * 100, 2),
                       "premium_bps": round(prem * 10000, 1), "mark": mark,
                       "day_vol_usd": round(_f(c.get("dayNtlVlm")))}

    def coin(sym):
        return coins.get(sym, {})

    top_oi = sorted(coins.items(), key=lambda x: -x[1]["oi_usd"])[:10]
    # funding extremes (annualized)
    fund_sorted = sorted(coins.items(), key=lambda x: x[1]["funding_ann_pct"])
    top_short = [(k, v["funding_ann_pct"]) for k, v in fund_sorted[:5] if v["funding_ann_pct"] < 0]
    top_long = [(k, v["funding_ann_pct"]) for k, v in fund_sorted[-5:] if v["funding_ann_pct"] > 0][::-1]

    out["total_oi_usd"] = round(total_oi)
    out["btc"] = coin("BTC")
    out["eth"] = coin("ETH")
    out["sol"] = coin("SOL")
    out["top_oi"] = [{"coin": k, "oi_usd": v["oi_usd"], "funding_ann_pct": v["funding_ann_pct"]} for k, v in top_oi]
    out["funding_extremes"] = {"most_long": top_long, "most_short": top_short}

    # ── self-history → OI deltas + leverage regime ──
    try:
        try:
            hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        except Exception:
            hist = {"series": []}
        ser = hist.get("series", [])
        now_stamp = out["generated_at"]
        snap = {"t": now_stamp, "total_oi": round(total_oi),
                "btc_oi": coin("BTC").get("oi_usd"), "btc_px": coin("BTC").get("mark"),
                "eth_oi": coin("ETH").get("oi_usd")}
        ser.append(snap)
        ser = ser[-2000:]
        hist["series"] = ser
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist, default=str).encode(),
                      ContentType="application/json")
        out["history_n"] = len(ser)

        # ~24h ago (assume hourly cadence -> 24 back; else nearest)
        def ago(n):
            return ser[-(n + 1)] if len(ser) > n else (ser[0] if ser else None)
        ref = ago(24)
        if ref and ref.get("total_oi"):
            out["total_oi_chg_24h_pct"] = round((total_oi / ref["total_oi"] - 1) * 100, 1)
            bo = coin("BTC").get("oi_usd"); bpx = coin("BTC").get("mark")
            if ref.get("btc_oi") and bo:
                oi_chg = (bo / ref["btc_oi"] - 1) * 100
                px_chg = ((bpx / ref["btc_px"] - 1) * 100) if ref.get("btc_px") else 0
                out["btc_oi_chg_24h_pct"] = round(oi_chg, 1)
                out["btc_px_chg_24h_pct"] = round(px_chg, 1)
                # leverage regime
                if oi_chg <= -8:
                    out["leverage_regime"] = "DELEVERAGING / LIQUIDATION CASCADE" if abs(px_chg) >= 3 else "DELEVERAGING"
                elif oi_chg >= 8 and abs(px_chg) < 2:
                    out["leverage_regime"] = "LEVERAGE BUILDUP (fragile)"
                elif oi_chg >= 5 and px_chg > 2:
                    out["leverage_regime"] = "HEALTHY EXPANSION"
                else:
                    out["leverage_regime"] = "NEUTRAL"
        # liquidation-pressure proxy: large negative BTC premium + OI drop
        prem_bps = coin("BTC").get("premium_bps", 0)
        liq = "ELEVATED" if (out.get("btc_oi_chg_24h_pct", 0) <= -6 and abs(prem_bps) >= 5) else "NORMAL"
        out["liq_pressure_proxy"] = liq
    except Exception as e:
        out["_hist_err"] = str(e)[:60]

    lr = out.get("leverage_regime")
    out["interpretation"] = (("HL perp leverage: $%.1fB total OI, BTC funding %.0f%%/yr, regime %s."
                              % (total_oi / 1e9, coin("BTC").get("funding_ann_pct", 0), lr or "—")))
    out["duration_s"] = round(time.time() - t0, 1)
    out["sources"] = ["api.hyperliquid.xyz/info metaAndAssetCtxs"]
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({"total_oi_usd": out["total_oi_usd"],
                                                    "leverage_regime": out.get("leverage_regime"),
                                                    "btc_funding_ann": coin("BTC").get("funding_ann_pct")})}
