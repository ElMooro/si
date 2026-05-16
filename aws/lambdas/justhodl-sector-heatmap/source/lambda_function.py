"""justhodl-sector-heatmap — #12/15 Bloomberg roadmap.

Finviz-style sector treemap: every S&P 500 ticker rendered as a rectangle
sized by market cap, colored by 1-day or multi-period return %. Sectors
group together visually so you can see — at a glance — which sectors are
leading, which are dragging, and which mega-caps are moving against their
sector.

DATA FLOW:
1. Read screener/data.json (already has sector, marketCap, stealScore, chg1m, chg6m)
2. Batch-fetch FMP /stable/quote-short for all tickers (gives intraday change%)
3. Group by sector, sort by marketCap within sector
4. Compute aggregates: sector totals, weighted returns, breadth stats
5. Output: data/sector-heatmap.json

The output structure is treemap-ready: any frontend (D3 / ECharts / Plotly)
can render it as nested rectangles.

OUTPUT (data/sector-heatmap.json):
{
  "schema_version": "1.0",
  "generated_at": "...",
  "as_of_date": "2026-05-16",
  "n_tickers": 487,
  "total_market_cap_usd": 51.2e12,
  "sectors": {
    "Technology": {
      "n_tickers": 71,
      "total_market_cap": 22.5e12,
      "weight_pct": 43.9,
      "weighted_return_1d_pct": -0.42,
      "weighted_return_1m_pct": 3.1,
      "n_advancers_1d": 38,
      "n_decliners_1d": 33,
      "breadth_1d_pct": 53.5,
      "biggest_winner_1d": {"ticker": "NVDA", "return_pct": 4.8, "marketCap": 3.2e12},
      "biggest_loser_1d": {"ticker": "INTC", "return_pct": -2.3, "marketCap": 0.15e12},
      "tickers": [
        {"symbol":"AAPL","name":"Apple Inc.","marketCap":3.45e12,
         "price":234.5,"change_1d_pct":0.8,"chg1m":3.2,"chg6m":12.5,
         "stealScore":71},
        ...
      ]
    },
    "Financials": { ... },
    ...
  },
  "market_regime": {
    "advancers": 251,
    "decliners": 236,
    "breadth_pct": 51.5,
    "weighted_return_1d_pct": 0.13,
    "leaders": ["NVDA","JPM"],
    "laggers": ["INTC","CRM"]
  }
}
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/sector-heatmap.json"
SCREENER_KEY = "screener/data.json"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")


def _fetch_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.5 * (i + 1))
    if last: raise last
    raise RuntimeError("http")


def fmp_quote_batch(tickers, max_workers=10):
    """FMP /stable/quote works only with single-symbol queries.
    Path /stable/quote/AAPL returns 404; ?symbol=AAPL,MSFT returns []
    (treats comma string as one symbol). So we parallelize single-symbol
    fetches with a small thread pool to stay within rate limits.
    Confirmed via ops/600 probe."""
    if not FMP_KEY: return {}
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _fetch_one(sym):
        url = f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP_KEY}"
        try:
            data = _fetch_json(url, timeout=10, retries=2)
            if not isinstance(data, list) or not data:
                return sym, None
            q = data[0]
            return sym, {
                "price": q.get("price"),
                "change_1d": q.get("change"),
                "change_1d_pct": q.get("changePercentage"),
                "day_low": q.get("dayLow"),
                "day_high": q.get("dayHigh"),
                "volume": q.get("volume"),
                "avg_volume": q.get("avgVolume"),
            }
        except Exception:
            return sym, None

    out = {}
    fetched = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_fetch_one, s): s for s in tickers}
        for f in as_completed(futures):
            sym, q = f.result()
            if q is not None:
                out[sym] = q
                fetched += 1
            else:
                failed += 1
            if (fetched + failed) % 100 == 0:
                print(f"[fmp_quote] progress {fetched + failed}/{len(tickers)} (fetched={fetched} failed={failed})")
    print(f"[fmp_quote] DONE: fetched={fetched} failed={failed} of {len(tickers)}")
    return out


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID: return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def get_screener_data():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=SCREENER_KEY)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[screener] failed to read: {e}")
        return None


def compute_sector_aggregates(sector_tickers):
    """For a list of ticker dicts in one sector, compute aggregates."""
    valid = [t for t in sector_tickers if t.get("marketCap") and t.get("marketCap") > 0]
    if not valid: return None
    total_mc = sum(t["marketCap"] for t in valid)
    # Cap-weighted returns where 1d data is available
    with_1d = [t for t in valid if t.get("change_1d_pct") is not None]
    if with_1d:
        wsum_1d = sum(t["change_1d_pct"] * t["marketCap"] for t in with_1d)
        wmc_1d = sum(t["marketCap"] for t in with_1d)
        weighted_1d = wsum_1d / wmc_1d if wmc_1d else None
    else:
        weighted_1d = None
    # 1m chg from screener
    with_1m = [t for t in valid if t.get("chg1m") is not None]
    if with_1m:
        wsum_1m = sum(t["chg1m"] * t["marketCap"] for t in with_1m)
        wmc_1m = sum(t["marketCap"] for t in with_1m)
        weighted_1m = wsum_1m / wmc_1m if wmc_1m else None
    else:
        weighted_1m = None
    # Breadth: how many up vs down today
    advancers = sum(1 for t in with_1d if t["change_1d_pct"] > 0)
    decliners = sum(1 for t in with_1d if t["change_1d_pct"] < 0)
    unchanged = len(with_1d) - advancers - decliners
    breadth_pct = (advancers / len(with_1d) * 100) if with_1d else None
    # Biggest winner/loser today
    biggest_winner = max(with_1d, key=lambda t: t["change_1d_pct"], default=None)
    biggest_loser = min(with_1d, key=lambda t: t["change_1d_pct"], default=None)
    return {
        "n_tickers": len(valid),
        "total_market_cap": total_mc,
        "weighted_return_1d_pct": round(weighted_1d, 3) if weighted_1d is not None else None,
        "weighted_return_1m_pct": round(weighted_1m, 3) if weighted_1m is not None else None,
        "n_advancers_1d": advancers,
        "n_decliners_1d": decliners,
        "n_unchanged_1d": unchanged,
        "breadth_pct_1d": round(breadth_pct, 1) if breadth_pct is not None else None,
        "biggest_winner_1d": (
            {"ticker": biggest_winner["symbol"], "name": biggest_winner.get("name"),
              "return_pct": round(biggest_winner["change_1d_pct"], 2),
              "marketCap": biggest_winner["marketCap"]}
            if biggest_winner else None),
        "biggest_loser_1d": (
            {"ticker": biggest_loser["symbol"], "name": biggest_loser.get("name"),
              "return_pct": round(biggest_loser["change_1d_pct"], 2),
              "marketCap": biggest_loser["marketCap"]}
            if biggest_loser else None),
    }


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[sector-heatmap] starting {datetime.now(timezone.utc).isoformat()}")

    sc = get_screener_data()
    if not sc or not sc.get("stocks"):
        return {"statusCode": 500, "body": json.dumps({"err": "screener data unavailable"})}

    stocks = sc["stocks"]
    print(f"[sector-heatmap] screener has {len(stocks)} stocks")

    # Filter to those with sector + marketCap
    valid = [s for s in stocks if s.get("sector") and s.get("marketCap") and s.get("marketCap") > 0]
    print(f"[sector-heatmap] {len(valid)} have sector+cap")

    # Sort by market cap descending to prioritize FMP-quote calls on the big names
    valid.sort(key=lambda s: -s.get("marketCap", 0))

    # Fetch FMP intraday quotes for all valid tickers
    tickers = [s["symbol"] for s in valid if s.get("symbol")]
    quotes = fmp_quote_batch(tickers, max_workers=12)
    print(f"[sector-heatmap] got {len(quotes)} intraday quotes from FMP")

    # Enrich and group by sector
    by_sector = {}
    enriched = []
    for s in valid:
        sym = s["symbol"]
        q = quotes.get(sym, {})
        e = {
            "symbol": sym,
            "name": s.get("name") or sym,
            "marketCap": s.get("marketCap"),
            "price": q.get("price") or s.get("price"),
            "change_1d": q.get("change_1d"),
            "change_1d_pct": q.get("change_1d_pct"),
            "chg1m": s.get("chg1m"),
            "chg6m": s.get("chg6m"),
            "stealScore": s.get("stealScore"),
            "stealBucket": s.get("stealBucket"),
            "volume": q.get("volume"),
            "avg_volume": q.get("avg_volume"),
            "vol_relative": (round(q.get("volume") / q.get("avg_volume"), 2)
                              if q.get("volume") and q.get("avg_volume") else None),
            "insiderSignal": s.get("insiderSignal"),
            "instSignal": s.get("instSignal"),
            "gradesScore": s.get("gradesScore"),
            "priceTargetUpsidePct": s.get("priceTargetUpsidePct"),
        }
        enriched.append(e)
        by_sector.setdefault(s["sector"], []).append(e)

    # Compute aggregates per sector
    sectors_out = {}
    for sector, tickers_in_sector in by_sector.items():
        agg = compute_sector_aggregates(tickers_in_sector)
        if not agg: continue
        # Sort tickers within sector by market cap desc
        tickers_in_sector.sort(key=lambda t: -t.get("marketCap", 0))
        agg["tickers"] = tickers_in_sector
        sectors_out[sector] = agg

    total_market_cap = sum(s["total_market_cap"] for s in sectors_out.values())
    for sector, agg in sectors_out.items():
        agg["weight_pct"] = round(agg["total_market_cap"] / total_market_cap * 100, 2) if total_market_cap else 0

    # Market-wide regime / breadth
    all_with_1d = [t for t in enriched if t.get("change_1d_pct") is not None]
    market_adv = sum(1 for t in all_with_1d if t["change_1d_pct"] > 0)
    market_dec = sum(1 for t in all_with_1d if t["change_1d_pct"] < 0)
    if all_with_1d and total_market_cap:
        market_weighted_1d = sum(t["change_1d_pct"] * t["marketCap"]
                                   for t in all_with_1d if t.get("marketCap")) / total_market_cap
    else:
        market_weighted_1d = None
    sorted_by_perf = sorted(all_with_1d, key=lambda t: t["change_1d_pct"], reverse=True)
    leaders = [{"symbol": t["symbol"], "name": t.get("name"),
                  "change_pct": round(t["change_1d_pct"], 2),
                  "marketCap": t["marketCap"]} for t in sorted_by_perf[:5]]
    laggers = [{"symbol": t["symbol"], "name": t.get("name"),
                  "change_pct": round(t["change_1d_pct"], 2),
                  "marketCap": t["marketCap"]} for t in sorted_by_perf[-5:][::-1]]

    market_regime = {
        "n_with_1d_data": len(all_with_1d),
        "advancers": market_adv,
        "decliners": market_dec,
        "breadth_pct": round(market_adv / len(all_with_1d) * 100, 1) if all_with_1d else None,
        "weighted_return_1d_pct": round(market_weighted_1d, 3) if market_weighted_1d is not None else None,
        "leaders": leaders,
        "laggers": laggers,
        "regime": ("BROAD_RISK_ON" if market_adv > market_dec * 1.5
                    else "BROAD_RISK_OFF" if market_dec > market_adv * 1.5
                    else "MIXED"),
    }

    # Rank sectors by weighted 1d return
    sector_rank = sorted(
        [(name, s.get("weighted_return_1d_pct"))
          for name, s in sectors_out.items()
          if s.get("weighted_return_1d_pct") is not None],
        key=lambda x: x[1], reverse=True,
    )

    output = {
        "schema_version": "1.0",
        "method": "sector_heatmap_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "as_of_date": datetime.now(timezone.utc).date().isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "n_tickers_total": len(enriched),
        "n_tickers_with_1d": len(all_with_1d),
        "total_market_cap_usd": total_market_cap,
        "market_regime": market_regime,
        "sector_rank_1d": [{"sector": n, "weighted_return_1d_pct": r} for n, r in sector_rank],
        "sectors": sectors_out,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(output, default=str).encode("utf-8"),
                   ContentType="application/json",
                   CacheControl="public, max-age=300")

    # Alerts: extreme sector moves
    alerts = []
    if sector_rank:
        top = sector_rank[0]; bot = sector_rank[-1]
        if abs(top[1] - bot[1]) >= 2.0:
            alerts.append(f"Sector dispersion {top[0]} {top[1]:+.2f}% vs {bot[0]} {bot[1]:+.2f}%")
        for name, ret in sector_rank:
            if ret is not None and abs(ret) >= 2.0:
                alerts.append(f"{name} {ret:+.2f}% (1d weighted)")
    if market_regime.get("breadth_pct") is not None and market_regime["breadth_pct"] < 30:
        alerts.append(f"Narrow breadth: only {market_regime['breadth_pct']}% advancing")
    elif market_regime.get("breadth_pct") is not None and market_regime["breadth_pct"] > 80:
        alerts.append(f"Broad rally: {market_regime['breadth_pct']}% advancing")

    if alerts:
        msg = ("<b>SECTOR HEATMAP</b>\n" + "\n".join(f"- {a}" for a in alerts[:6])
                + f"\n\nRegime: {market_regime.get('regime')}"
                + f"\nMarket 1d: {market_regime.get('weighted_return_1d_pct')}%")
        maybe_telegram(msg)

    print(f"[sector-heatmap] done {output['elapsed_s']}s "
          f"sectors={len(sectors_out)} regime={market_regime.get('regime')} "
          f"breadth={market_regime.get('breadth_pct')}% alerts={len(alerts)}")

    return {"statusCode": 200, "body": json.dumps({
        "ok": True,
        "n_tickers": len(enriched),
        "n_sectors": len(sectors_out),
        "market_regime": market_regime.get("regime"),
        "breadth_pct": market_regime.get("breadth_pct"),
        "weighted_1d": market_regime.get("weighted_return_1d_pct"),
        "alerts": len(alerts),
    })}
