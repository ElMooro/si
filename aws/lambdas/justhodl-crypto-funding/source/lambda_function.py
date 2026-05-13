"""
justhodl-crypto-funding v1.0.0 — Perp Positioning Engine

WHY FUNDING RATES MATTER
========================
Perpetual swaps fund every 8 hours. The funding rate is paid by the side
with the dominant exposure to the other side. When funding is POSITIVE,
longs are paying shorts — the market is leveraged-long. When NEGATIVE,
shorts are paying longs — leveraged-short.

Extreme funding = crowded positioning = squeeze risk:
  Funding > +0.03% (per 8h, ~32% annualized) → LONG SQUEEZE risk
  Funding < -0.03% (per 8h, -32% annualized) → SHORT SQUEEZE risk

Open interest tells us how much SIZE is on the table; funding tells us
which side is paying. Together: complete perp positioning picture.

DATA SOURCE
===========
OKX public API (verified working from AWS Lambda in probe 519):
  /api/v5/public/funding-rate?instId={INST}       — current 8h rate
  /api/v5/public/funding-rate-history?instId=...  — historical for z-score
  /api/v5/public/open-interest?instType=SWAP      — current OI in USD
  /api/v5/market/index-tickers?instId={INDEX}     — spot price

INSTRUMENTS COVERED
===================
Top 10 by volume: BTC, ETH, SOL, BNB, XRP, ADA, DOGE, AVAX, LINK, MATIC
(All -USDT-SWAP perps on OKX)

METRICS PER COIN
================
  current_funding_rate (8h fraction, e.g. 0.000196 = 0.0196%)
  annualized_pct (rate × 3 × 365 × 100)
  open_interest_usd
  spot_price + 24h_change
  funding_history (30 periods = ~10 days at 8h cadence)
  funding_30p_mean, funding_30p_stdev, funding_z_score
  funding_5p_mean (recent momentum)
  funding_momentum (5p_mean - 30p_mean)
  regime: HIGHLY_BULLISH_LEVERAGE / BULLISH / BALANCED / BEARISH / HIGHLY_BEARISH
  crowding_flag: when |z| > 2 (extreme positioning)

COMPOSITE METRICS
=================
  vw_funding_pct: open-interest weighted funding across top 10
  median_funding_pct: median across coins
  funding_dispersion: max - min (cross-asset disagreement)
  n_extreme_long: coins with z > 2
  n_extreme_short: coins with z < -2
  composite_regime + signal

ALERTS
======
Telegram on:
  • Composite regime change
  • Any coin entering HIGHLY_BULLISH or HIGHLY_BEARISH leverage
  • Cross-coin dispersion spike (>0.10pp range)
"""
import io, json, os, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"

S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/crypto-funding.json"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
HTTP_TIMEOUT = 15
OKX_BASE = "https://www.okx.com"

COINS = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "AVAX", "LINK", "MATIC"]

# Thresholds (per 8h funding fraction)
FUNDING_HIGHLY_BULL = 0.0003   # +0.03% per 8h = ~33% annualized
FUNDING_BULL = 0.0001          # +0.01% per 8h = ~11% annualized
FUNDING_BEAR = -0.0001
FUNDING_HIGHLY_BEAR = -0.0003

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


def _mean(xs): return sum(xs) / len(xs) if xs else 0.0
def _stdev(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs); return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def fetch_json(path, timeout=HTTP_TIMEOUT):
    url = f"{OKX_BASE}{path}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_coin_data(coin):
    """Returns dict of all metrics for one coin or None on err."""
    swap = f"{coin}-USDT-SWAP"
    idx = f"{coin}-USDT"
    out = {"coin": coin, "instId": swap}
    try:
        # ─── Current funding ───
        d = fetch_json(f"/api/v5/public/funding-rate?instId={swap}")
        if d.get("code") != "0" or not d.get("data"):
            return None
        cur = d["data"][0]
        out["current_funding_rate"] = float(cur.get("fundingRate") or 0)
        out["max_funding_rate"] = float(cur.get("maxFundingRate") or 0)
        out["next_funding_time"] = cur.get("fundingTime")

        # ─── Historical funding (30 periods) ───
        d = fetch_json(f"/api/v5/public/funding-rate-history?instId={swap}&limit=30")
        history = []
        if d.get("code") == "0":
            for row in d.get("data") or []:
                try:
                    history.append({
                        "ts": row.get("fundingTime"),
                        "rate": float(row.get("realizedRate") or row.get("fundingRate") or 0),
                    })
                except Exception: continue
        history.reverse()  # ascending
        out["funding_history"] = history

        # ─── Open interest in USD ───
        d = fetch_json(f"/api/v5/public/open-interest?instType=SWAP&instId={swap}")
        if d.get("code") == "0" and d.get("data"):
            oi = d["data"][0]
            try: out["oi_usd"] = float(oi.get("oiUsd") or 0)
            except: out["oi_usd"] = None
            try: out["oi_ccy"] = float(oi.get("oiCcy") or 0)
            except: out["oi_ccy"] = None

        # ─── Spot index ticker ───
        d = fetch_json(f"/api/v5/market/index-tickers?instId={idx}")
        if d.get("code") == "0" and d.get("data"):
            ix = d["data"][0]
            try:
                out["spot_price"] = float(ix.get("idxPx") or 0)
                o24 = float(ix.get("open24h") or 0)
                if o24 > 0:
                    out["change_24h_pct"] = round(
                        (out["spot_price"] / o24 - 1) * 100, 2)
            except: pass

        # ─── Derived stats ───
        rates = [h["rate"] for h in history]
        if len(rates) >= 5:
            out["funding_30p_mean"] = _mean(rates)
            out["funding_30p_stdev"] = _stdev(rates)
            out["funding_5p_mean"] = _mean(rates[-5:])
            out["funding_momentum"] = out["funding_5p_mean"] - out["funding_30p_mean"]
            sd = out["funding_30p_stdev"]
            z = ((out["current_funding_rate"] - out["funding_30p_mean"]) / sd
                  if sd > 0 else 0)
            out["funding_z_score"] = round(z, 2)
        else:
            out["funding_30p_mean"] = None
            out["funding_z_score"] = None

        # ─── Annualized + regime ───
        fr = out["current_funding_rate"]
        out["annualized_pct"] = round(fr * 3 * 365 * 100, 2)
        if fr > FUNDING_HIGHLY_BULL: out["regime"] = "HIGHLY_BULLISH_LEVERAGE"
        elif fr > FUNDING_BULL: out["regime"] = "BULLISH_LEVERAGE"
        elif fr > FUNDING_BEAR: out["regime"] = "BALANCED"
        elif fr > FUNDING_HIGHLY_BEAR: out["regime"] = "BEARISH_LEVERAGE"
        else: out["regime"] = "HIGHLY_BEARISH_LEVERAGE"

        out["crowding_flag"] = (out.get("funding_z_score") is not None
                                  and abs(out["funding_z_score"]) > 2)

    except Exception as e:
        print(f"  {coin} err: {str(e)[:120]}")
        return None
    return out


def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text):
    if not TELEGRAM_TOKEN: return False
    chat = get_chat_id()
    if not chat: return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": chat, "text": text[:4096],
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10); return True
    except Exception as e:
        print(f"  telegram err: {str(e)[:80]}"); return False


def load_prior():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read()).get("composite_regime")
    except Exception:
        return None


def lambda_handler(event, context):
    started = time.time()
    print(f"=== CRYPTO-FUNDING v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    prior = load_prior()

    # Fetch all coins in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        future_to_coin = {ex.submit(fetch_coin_data, c): c for c in COINS}
        for fut in as_completed(future_to_coin):
            c = future_to_coin[fut]
            try:
                r = fut.result()
                if r: results[c] = r
            except Exception as e:
                print(f"  {c} fut err: {str(e)[:80]}")
    print(f"  fetched {len(results)}/{len(COINS)} coins")

    if not results:
        return {"statusCode": 500, "body": json.dumps({"err": "no data"})}

    # ─── Composite metrics ───
    valid = [r for r in results.values() if r.get("current_funding_rate") is not None]
    rates = [r["current_funding_rate"] for r in valid]
    oi_total = sum((r.get("oi_usd") or 0) for r in valid)
    vw_funding = (sum(r["current_funding_rate"] * (r.get("oi_usd") or 0) for r in valid)
                   / oi_total) if oi_total > 0 else _mean(rates)

    sorted_rates = sorted(rates)
    median_funding = sorted_rates[len(sorted_rates)//2] if sorted_rates else 0
    funding_max = max(rates) if rates else 0
    funding_min = min(rates) if rates else 0
    dispersion = funding_max - funding_min

    n_extreme_long = sum(1 for r in valid
                          if r.get("funding_z_score") is not None and r["funding_z_score"] > 2)
    n_extreme_short = sum(1 for r in valid
                           if r.get("funding_z_score") is not None and r["funding_z_score"] < -2)
    n_highly_bull = sum(1 for r in valid if r.get("regime") == "HIGHLY_BULLISH_LEVERAGE")
    n_highly_bear = sum(1 for r in valid if r.get("regime") == "HIGHLY_BEARISH_LEVERAGE")
    n_bull = sum(1 for r in valid if "BULLISH" in (r.get("regime") or ""))
    n_bear = sum(1 for r in valid if "BEARISH" in (r.get("regime") or ""))

    # Composite regime
    vw_ann = vw_funding * 3 * 365 * 100
    if n_highly_bull >= 3:
        composite_regime = "EUPHORIC_LEVERAGE"
        composite_signal = f"{n_highly_bull} coins at extreme bullish funding — long squeeze risk elevated · consider taking profits or hedging"
    elif n_highly_bear >= 3:
        composite_regime = "CAPITULATION_LEVERAGE"
        composite_signal = f"{n_highly_bear} coins at extreme bearish funding — short squeeze setup · contrarian long opportunities"
    elif vw_ann > 20:
        composite_regime = "BULLISH_LEVERAGE_DOMINANT"
        composite_signal = "Majority bullish positioning · trend higher with mean-revert risk on news"
    elif vw_ann < -20:
        composite_regime = "BEARISH_LEVERAGE_DOMINANT"
        composite_signal = "Majority bearish positioning · short squeeze risk on positive catalyst"
    elif dispersion > 0.0010:  # 10bps spread across coins
        composite_regime = "HIGH_DISPERSION"
        composite_signal = "Cross-coin disagreement · idiosyncratic positioning · trade individual names not the index"
    else:
        composite_regime = "BALANCED"
        composite_signal = "Funding balanced across coins · no major positioning tilt"

    # Squeeze candidates (most extreme |z|)
    squeeze_candidates = sorted(
        [r for r in valid if r.get("funding_z_score") is not None],
        key=lambda r: -abs(r["funding_z_score"])
    )[:5]

    # Build payload
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "OKX /api/v5/public + /api/v5/market",
        "elapsed_seconds": round(time.time() - started, 2),
        "config": {
            "coins": COINS, "n_history_periods": 30,
            "thresholds_per_8h": {
                "highly_bullish": FUNDING_HIGHLY_BULL,
                "bullish": FUNDING_BULL,
                "bearish": FUNDING_BEAR,
                "highly_bearish": FUNDING_HIGHLY_BEAR,
            },
        },
        "market_composite": {
            "n_coins_analyzed": len(valid),
            "vw_funding_per_8h": round(vw_funding, 7),
            "vw_funding_annualized_pct": round(vw_ann, 2),
            "median_funding_per_8h": round(median_funding, 7),
            "median_funding_annualized_pct": round(median_funding * 3 * 365 * 100, 2),
            "funding_max": round(funding_max, 7),
            "funding_min": round(funding_min, 7),
            "funding_dispersion_pp": round(dispersion * 100, 4),
            "total_oi_usd_billions": round(oi_total / 1e9, 2),
            "n_extreme_long_positioning": n_extreme_long,
            "n_extreme_short_positioning": n_extreme_short,
            "n_highly_bullish_leverage": n_highly_bull,
            "n_highly_bearish_leverage": n_highly_bear,
            "n_bullish": n_bull,
            "n_bearish": n_bear,
        },
        "composite_regime": composite_regime,
        "composite_signal": composite_signal,
        "regime_changed_from_prior": (prior != composite_regime) if prior else False,
        "squeeze_candidates": [{
            "coin": r["coin"],
            "regime": r["regime"],
            "z_score": r.get("funding_z_score"),
            "annualized_pct": r.get("annualized_pct"),
            "oi_usd_b": round((r.get("oi_usd") or 0) / 1e9, 2),
            "spot": r.get("spot_price"),
            "change_24h_pct": r.get("change_24h_pct"),
        } for r in squeeze_candidates],
        "by_coin": {
            r["coin"]: {
                "instId": r["instId"],
                "current_funding_rate": r["current_funding_rate"],
                "current_funding_pct": round(r["current_funding_rate"] * 100, 5),
                "annualized_pct": r["annualized_pct"],
                "funding_z_score": r.get("funding_z_score"),
                "funding_5p_mean": r.get("funding_5p_mean"),
                "funding_30p_mean": r.get("funding_30p_mean"),
                "funding_momentum": (round(r["funding_momentum"], 7)
                                       if r.get("funding_momentum") is not None else None),
                "oi_usd": r.get("oi_usd"),
                "oi_usd_b": round((r.get("oi_usd") or 0) / 1e9, 2),
                "oi_ccy": r.get("oi_ccy"),
                "spot_price": r.get("spot_price"),
                "change_24h_pct": r.get("change_24h_pct"),
                "regime": r["regime"],
                "crowding_flag": r["crowding_flag"],
                "n_history_periods": len(r.get("funding_history") or []),
            } for r in valid
        },
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ crypto-funding.json written ({round(len(json.dumps(payload))/1024,1)} KB)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Alerts
    alert_sent = False
    if ((prior and prior != composite_regime) or
        composite_regime in ("EUPHORIC_LEVERAGE", "CAPITULATION_LEVERAGE")):
        lines = [
            f"💹 *Crypto Perp Positioning · {datetime.now(timezone.utc).strftime('%b %d %H:%M')}*\n",
            f"⚡ {composite_regime}",
            f"_{composite_signal}_\n",
            f"📊 VW funding: *{vw_ann:+.1f}%* (annualized)",
            f"📊 Median: {median_funding*3*365*100:+.1f}% · Range: {dispersion*100:+.4f}pp",
            f"💰 Total OI: *${oi_total/1e9:.1f}B*",
            f"\n🎯 Top squeeze candidates:",
        ]
        for s in squeeze_candidates[:3]:
            lines.append(f"  · *{s['coin']}* {s['regime']} · z={s.get('z_score',0):+.1f} "
                         f"· {s.get('annualized_pct',0):+.1f}% ann · ${s.get('spot',0):,.0f}")
        if prior and prior != composite_regime:
            lines.insert(2, f"_(was {prior})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_coins": len(valid),
        "vw_funding_ann_pct": round(vw_ann, 2),
        "composite_regime": composite_regime,
        "n_extreme_long": n_extreme_long,
        "n_extreme_short": n_extreme_short,
        "total_oi_usd_b": round(oi_total / 1e9, 2),
        "regime_changed": prior != composite_regime if prior else False,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
