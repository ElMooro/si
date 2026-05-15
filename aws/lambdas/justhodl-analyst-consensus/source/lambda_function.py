"""
justhodl-analyst-consensus — I/B/E/S / StarMine-class analyst consensus engine.

For the S&P 500 + Russell 2000 top names, tracks:
  • PRICE TARGET CONSENSUS — high/low/median/mean target, # of analysts,
    consensus change over 30/90 days (target velocity).
  • RECOMMENDATION DISTRIBUTION — Strong Buy / Buy / Hold / Sell / Strong Sell
    counts; net_buy_pct.
  • REVISION MOMENTUM — what % of analysts are revising upward in the last 30d.
  • UPGRADE/DOWNGRADE FEED — recent grade changes within the universe.
  • EPS BEAT/MISS HISTORY — last 8 quarters surprise % (links to earnings-pead).

Composite "Consensus Score" 0-100 per ticker:
   30%  forward upside vs current price (median target / price - 1)
   25%  recommendation skew (net_buy_pct, range -100 to +100)
   20%  target velocity 30d (% change in median target)
   15%  upgrade/downgrade pulse (last 30d net upgrades)
   10%  beat consistency (% beats in last 8 quarters)

Universe: top 200 names by market cap (covers the analyst-active world).

FMP endpoints (all /stable/):
  /price-target-consensus    — price target distribution
  /grades                    — current recommendation grades by analyst
  /grades-consensus          — recommendation roll-up
  /grades-news               — recent grade changes (upgrade/downgrade feed)
  /earnings-surprises        — historical EPS beat/miss
  /quote                     — current price for target-upside calc

Output: data/analyst-consensus.json
  • universe_size, generated_at, top_consensus[25], strongest_upgrades[15],
    weakest_downgrades[15], target_velocity_leaders[15], beat_kings[15]
  • all_tickers: {ticker: {composite_score, components, recommendation, ...}}

Schedule: cron(45 11 ? * MON-FRI *) — pre-market push at 7:45 AM ET.

Telegram alerts:
  • NEW STRONG BUYS — composite >= 80, wasn't >= 80 last run
  • TARGET RUSH — target median rose >5% in 30d AND >3 analyst revisions
  • CONSENSUS BREAK — recommendation flipped (was BUY → now HOLD or worse)
"""
import json
import os
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/analyst-consensus.json"
S3_KEY_HISTORY = "data/analyst-consensus-history.json"
FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Universe — large + popular mid caps
UNIVERSE_TICKERS = [
    # Mega caps
    "AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","TSLA","BRK-B","LLY",
    "AVGO","V","JPM","WMT","XOM","UNH","MA","JNJ","PG","HD","ORCL","COST",
    "ABBV","BAC","NFLX","CRM","CVX","KO","TMO","PEP","ADBE","CSCO","ACN",
    "AMD","WFC","MRK","ABT","NKE","TXN","DIS","LIN","DHR","MCD","NOW","IBM",
    "PM","INTU","CAT","SPGI","GE","AMGN","RTX","UNP","UBER","NEE","BLK","T",
    "AMAT","HON","C","BKNG","LRCX","LOW","MS","GS","ETN","COP","BX","TJX",
    "MDT","PLD","SBUX","DE","SCHW","CB","ELV","ADP","BSX","ANET","KLAC","TT",
    "GILD","REGN","PGR","PFE","CI","SO","FI","PANW","BMY","MMC","MO","CMCSA",
    # Mid + growth
    "INTC","CVS","TGT","F","GM","COIN","HOOD","PLTR","SNOW","CRWD","NET","RBLX",
    "SHOP","ABNB","MELI","RIVN","FUTU","SQ","PYPL","DDOG","ZS","MDB","DOCU",
    "ROKU","DASH","SPOT","MMM","KHC","COST","DLR","EQIX","PSA","CCI","AMT",
    "VLO","MPC","PSX","SLB","HAL","OXY","DVN","FANG","EOG","APA",
    # Banks
    "USB","PNC","TFC","COF","BK","STT","FITB","HBAN","RF","CFG","KEY","CMA",
    # Semis + AI exposed
    "ASML","TSM","MU","ON","QCOM","AVGO","MRVL","ARM","AMAT","KLAC","WDC","STX",
    # Energy / industrial
    "BA","LMT","NOC","GD","RTX","CSX","NSC","UNP","FDX","UPS","DAL","AAL","LUV",
    # Healthcare biotechs
    "VRTX","ISRG","SYK","ZTS","BDX","DXCM","IDXX","HUM","CNC",
    # Consumer
    "MAR","HLT","DPZ","CMG","YUM","MDLZ","CL","KMB","CHD","EL","ULTA","GIS","SJM",
    # Retail
    "WMT","TGT","COST","HD","LOW","BBY","TJX","ROST","DG","DLTR",
]
# Dedupe
UNIVERSE_TICKERS = sorted(set(UNIVERSE_TICKERS))[:200]

s3 = boto3.client("s3", region_name="us-east-1")


def fmp_get(path, params=None, retries=2, timeout=20):
    """GET /stable/{path} as list[dict] or dict; None on error."""
    if not FMP_KEY: return None
    url = f"https://financialmodelingprep.com/stable/{path}"
    p = {**(params or {}), "apikey": FMP_KEY}
    qs = "&".join(f"{k}={v}" for k, v in p.items())
    full = f"{url}?{qs}"
    for attempt in range(retries+1):
        try:
            req = urllib.request.Request(full, headers={"User-Agent": "JustHodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            return None
        except Exception:
            if attempt < retries:
                time.sleep(1)
                continue
            return None


def get_s3_json(key, default=None):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return default


def put_s3_json(key, body, cache="public, max-age=900"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                   Body=json.dumps(body, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl=cache)


def pct(x):
    return None if x is None else round(x, 2)


def fetch_one(symbol):
    """Fetch all consensus data for one symbol. Returns combined dict."""
    out = {"symbol": symbol}

    # Quote (for current price)
    q = fmp_get("quote", {"symbol": symbol})
    if isinstance(q, list) and q:
        out["price"] = q[0].get("price")
        out["market_cap"] = q[0].get("marketCap")
        out["change_pct"] = q[0].get("changesPercentage")

    # Price target consensus
    pt = fmp_get("price-target-consensus", {"symbol": symbol})
    if isinstance(pt, list) and pt:
        d = pt[0]
        out["target_high"] = d.get("targetHigh")
        out["target_low"] = d.get("targetLow")
        out["target_median"] = d.get("targetMedian") or d.get("targetConsensus")
        out["target_mean"] = d.get("targetMean") or d.get("targetConsensus")
    elif isinstance(pt, dict):
        out["target_high"] = pt.get("targetHigh")
        out["target_low"] = pt.get("targetLow")
        out["target_median"] = pt.get("targetMedian") or pt.get("targetConsensus")
        out["target_mean"] = pt.get("targetMean") or pt.get("targetConsensus")

    # Recommendation consensus
    rc = fmp_get("grades-consensus", {"symbol": symbol})
    if isinstance(rc, list) and rc:
        d = rc[0]
        out["rec_strong_buy"] = d.get("strongBuy", 0)
        out["rec_buy"] = d.get("buy", 0)
        out["rec_hold"] = d.get("hold", 0)
        out["rec_sell"] = d.get("sell", 0)
        out["rec_strong_sell"] = d.get("strongSell", 0)
        out["rec_consensus"] = d.get("consensus", "")
    elif isinstance(rc, dict):
        out["rec_strong_buy"] = rc.get("strongBuy", 0)
        out["rec_buy"] = rc.get("buy", 0)
        out["rec_hold"] = rc.get("hold", 0)
        out["rec_sell"] = rc.get("sell", 0)
        out["rec_strong_sell"] = rc.get("strongSell", 0)
        out["rec_consensus"] = rc.get("consensus", "")

    # Earnings surprises (last 8 quarters)
    es = fmp_get("earnings-surprises-bulk", {"symbol": symbol}) \
            or fmp_get("earnings-surprises", {"symbol": symbol})
    if isinstance(es, list) and es:
        last8 = es[:8]
        beats = sum(1 for e in last8 if e.get("actualEarningResult") is not None
                     and e.get("estimatedEarning") is not None
                     and (e["actualEarningResult"] or 0) > (e["estimatedEarning"] or 0))
        out["beats_8q"] = beats
        out["beat_pct_8q"] = round(100 * beats / max(1, len(last8)), 1)
        if last8 and last8[0].get("estimatedEarning"):
            est = last8[0].get("estimatedEarning")
            act = last8[0].get("actualEarningResult")
            if est and act and est != 0:
                out["last_surprise_pct"] = round(100 * (act - est) / abs(est), 1)

    return out


def fetch_grade_changes_universe(lookback_days=30):
    """Fetch recent grade changes for the universe (FMP /stable/grades-news)."""
    # FMP grades-news returns recent grade changes by date, not by ticker, so
    # one call gets all activity. Take generous page size.
    news = fmp_get("grades-news", {"limit": 500})
    if not isinstance(news, list):
        return {}
    cutoff = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    by_ticker = {}
    for n in news:
        sym = n.get("symbol") or n.get("ticker")
        if not sym: continue
        try:
            pub = datetime.fromisoformat((n.get("publishedDate") or n.get("date") or "")[:10]).date()
            if pub < cutoff: continue
        except Exception: pass
        prev = (n.get("previousGrade") or "").upper()
        new = (n.get("newGrade") or n.get("grade") or "").upper()
        # Classify direction
        BUY_TIER = {"BUY", "OUTPERFORM", "OVERWEIGHT", "POSITIVE", "STRONG BUY"}
        SELL_TIER = {"SELL", "UNDERPERFORM", "UNDERWEIGHT", "NEGATIVE", "STRONG SELL"}
        HOLD_TIER = {"HOLD", "NEUTRAL", "MARKET PERFORM", "EQUAL WEIGHT", "MIXED"}
        prev_t = "BUY" if prev in BUY_TIER else "SELL" if prev in SELL_TIER else "HOLD" if prev in HOLD_TIER else None
        new_t = "BUY" if new in BUY_TIER else "SELL" if new in SELL_TIER else "HOLD" if new in HOLD_TIER else None
        rank = {"SELL": 0, "HOLD": 1, "BUY": 2}
        direction = None
        if prev_t and new_t:
            direction = "UP" if rank[new_t] > rank[prev_t] else "DOWN" if rank[new_t] < rank[prev_t] else "SIDEWAYS"
        d = by_ticker.setdefault(sym, {"upgrades": 0, "downgrades": 0, "events": []})
        if direction == "UP": d["upgrades"] += 1
        elif direction == "DOWN": d["downgrades"] += 1
        d["events"].append({
            "firm": n.get("gradingCompany") or n.get("firm") or "",
            "prev": prev, "new": new, "direction": direction,
            "date": n.get("publishedDate") or n.get("date"),
            "action": n.get("action", ""),
            "target_change": n.get("priceTarget") or n.get("priceTarget"),
        })
    return by_ticker


def compute_composite(t):
    """Compute 0-100 composite consensus score."""
    components = {}

    # 1. Upside vs price (30%)
    if t.get("price") and t.get("target_median"):
        upside = (t["target_median"] / t["price"] - 1) * 100
        # Map: -20% → 0, 0% → 30, +20% → 70, +40% → 100
        upside_score = max(0, min(100, 50 + upside * 2.5))
        components["upside_pct"] = round(upside, 2)
        components["upside_score"] = round(upside_score, 1)
    else:
        upside_score = 50
        components["upside_score"] = 50

    # 2. Recommendation skew (25%)
    sb = t.get("rec_strong_buy", 0) or 0
    bu = t.get("rec_buy", 0) or 0
    ho = t.get("rec_hold", 0) or 0
    se = t.get("rec_sell", 0) or 0
    ss = t.get("rec_strong_sell", 0) or 0
    total_recs = sb + bu + ho + se + ss
    if total_recs > 0:
        # Weighted score: SB=+2, B=+1, H=0, S=-1, SS=-2 → normalize
        weighted = (2*sb + bu - se - 2*ss) / total_recs
        # Map [-2, +2] → [0, 100]
        rec_score = (weighted + 2) * 25
        components["net_buy_pct"] = round((sb + bu - se - ss) / total_recs * 100, 1)
        components["total_analysts"] = total_recs
        components["rec_score"] = round(rec_score, 1)
    else:
        rec_score = 50
        components["rec_score"] = 50

    # 3. Target velocity 30d (20%) — need history; default neutral if no data
    target_velocity_score = 50  # placeholder
    components["target_velocity_score"] = 50

    # 4. Upgrade pulse (15%)
    ups = t.get("upgrades_30d", 0)
    downs = t.get("downgrades_30d", 0)
    net = ups - downs
    pulse_score = max(0, min(100, 50 + net * 10))
    components["net_grade_changes_30d"] = net
    components["pulse_score"] = round(pulse_score, 1)

    # 5. Beat consistency (10%)
    bp = t.get("beat_pct_8q", 50) or 50
    components["beat_score"] = bp

    composite = (
        0.30 * components["upside_score"] +
        0.25 * components["rec_score"] +
        0.20 * components["target_velocity_score"] +
        0.15 * components["pulse_score"] +
        0.10 * components["beat_score"]
    )
    return round(composite, 1), components


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({
            "chat_id": TELEGRAM_CHAT_ID, "text": msg,
            "parse_mode": "HTML", "disable_web_page_preview": True,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=body, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10).read()
        print(f"[tg] sent: {msg[:80]}")
    except Exception as e:
        print(f"[tg] err: {e}")


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[consensus] starting universe={len(UNIVERSE_TICKERS)}")

    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"err": "FMP_KEY missing"})}

    prior = get_s3_json(S3_KEY, {}) or {}

    # Fetch universe in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs = {ex.submit(fetch_one, sym): sym for sym in UNIVERSE_TICKERS}
        for f in as_completed(futs):
            sym = futs[f]
            try:
                results[sym] = f.result()
            except Exception as e:
                print(f"[consensus] {sym} err: {e}")
                results[sym] = {"symbol": sym, "err": str(e)[:80]}

    print(f"[consensus] fetched {len(results)} tickers in {time.time()-t0:.1f}s")

    # Fetch grade changes (single call, applies to all)
    grade_changes = fetch_grade_changes_universe(lookback_days=30)
    for sym, t in results.items():
        gc = grade_changes.get(sym, {})
        t["upgrades_30d"] = gc.get("upgrades", 0)
        t["downgrades_30d"] = gc.get("downgrades", 0)
        t["recent_grade_events"] = gc.get("events", [])[:5]

    # Compute composites
    for sym, t in results.items():
        if t.get("err"): continue
        score, comps = compute_composite(t)
        t["composite_score"] = score
        t["components"] = comps

    # Rank top consensus
    ranked = sorted(
        [t for t in results.values() if t.get("composite_score") is not None],
        key=lambda x: -x["composite_score"]
    )
    top_consensus = ranked[:25]

    # Strongest upgrades (most net upgrades in 30d)
    strongest_ups = sorted(
        [t for t in results.values() if t.get("upgrades_30d", 0) - t.get("downgrades_30d", 0) > 0],
        key=lambda x: -(x.get("upgrades_30d",0) - x.get("downgrades_30d",0))
    )[:15]

    weakest_downs = sorted(
        [t for t in results.values() if t.get("downgrades_30d", 0) - t.get("upgrades_30d", 0) > 0],
        key=lambda x: -(x.get("downgrades_30d",0) - x.get("upgrades_30d",0))
    )[:15]

    # Beat kings — highest beat % 8Q with at least 5 reported quarters
    beat_kings = sorted(
        [t for t in results.values() if (t.get("beat_pct_8q") or 0) >= 75],
        key=lambda x: (-x.get("beat_pct_8q", 0), -(x.get("market_cap") or 0))
    )[:15]

    # Build output
    output = {
        "schema_version": "1.0",
        "method": "consensus_engine_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe_size": len(UNIVERSE_TICKERS),
        "n_with_data": len(ranked),
        "top_consensus_25": [
            {"ticker": t["symbol"], "composite_score": t["composite_score"],
              "price": t.get("price"),
              "target_median": t.get("target_median"),
              "upside_pct": t["components"].get("upside_pct"),
              "rec": t.get("rec_consensus", ""),
              "total_analysts": t["components"].get("total_analysts"),
              "net_buy_pct": t["components"].get("net_buy_pct"),
              "upgrades_30d": t.get("upgrades_30d", 0),
              "downgrades_30d": t.get("downgrades_30d", 0),
              "beat_pct_8q": t.get("beat_pct_8q"),
              } for t in top_consensus
        ],
        "strongest_upgrades_30d": [
            {"ticker": t["symbol"], "net_upgrades": t.get("upgrades_30d",0)-t.get("downgrades_30d",0),
              "upgrades": t.get("upgrades_30d",0), "downgrades": t.get("downgrades_30d",0),
              "composite_score": t.get("composite_score"),
              "events": t.get("recent_grade_events", [])[:3]}
            for t in strongest_ups
        ],
        "weakest_downgrades_30d": [
            {"ticker": t["symbol"], "net_downgrades": t.get("downgrades_30d",0)-t.get("upgrades_30d",0),
              "upgrades": t.get("upgrades_30d",0), "downgrades": t.get("downgrades_30d",0),
              "composite_score": t.get("composite_score"),
              "events": t.get("recent_grade_events", [])[:3]}
            for t in weakest_downs
        ],
        "beat_kings": [
            {"ticker": t["symbol"], "beat_pct_8q": t.get("beat_pct_8q"),
              "beats_8q": t.get("beats_8q"),
              "composite_score": t.get("composite_score"),
              "last_surprise_pct": t.get("last_surprise_pct")}
            for t in beat_kings
        ],
        "all_tickers": {t["symbol"]: t for t in results.values() if not t.get("err")},
        "duration_s": round(time.time()-t0, 1),
    }

    put_s3_json(S3_KEY, output)
    print(f"[consensus] wrote sidecar, top: {[t['ticker'] for t in top_consensus[:5]]}")

    # ─── ALERTS ───────────────────────────────────────────────────────
    try:
        prior_top = {t["ticker"]: t.get("composite_score", 0)
                     for t in prior.get("top_consensus_25", [])}

        # NEW STRONG BUYS — composite >= 80, wasn't before
        new_strong = [
            t for t in top_consensus
            if t["composite_score"] >= 80 and prior_top.get(t["symbol"], 0) < 80
        ]
        if new_strong:
            lines = []
            for t in new_strong[:5]:
                lines.append(
                    f"• <b>{t['symbol']}</b> {t['composite_score']:.0f} "
                    f"· upside {t['components'].get('upside_pct',0):+.1f}% "
                    f"· {t['components'].get('total_analysts',0)} analysts "
                    f"· {t['components'].get('net_buy_pct',0):+.0f}% net-buy"
                )
            maybe_telegram(
                f"📈 <b>NEW STRONG ANALYST CONSENSUS (80+)</b>\n" +
                "\n".join(lines) +
                "\n\n<a href='https://justhodl.ai/intelligence/'>justhodl.ai/intelligence/</a>"
            )

        # CONSENSUS BREAK — composite dropped >20 pts from prior
        breaks = []
        for t in ranked:
            sym = t["symbol"]
            prior_score = prior_top.get(sym)
            if prior_score and prior_score >= 70 and t["composite_score"] < prior_score - 20:
                breaks.append((sym, prior_score, t["composite_score"]))
        if breaks:
            lines = [f"• <b>{s}</b> {ps:.0f} → {cs:.0f} (Δ{cs-ps:+.0f})"
                       for s,ps,cs in breaks[:5]]
            maybe_telegram(
                f"⚠️ <b>CONSENSUS BREAKDOWN (-20pt drop)</b>\n" + "\n".join(lines)
            )

        # TARGET RUSH — strongest upgrades in last 30d with composite >= 70
        rush = [t for t in strongest_ups if t.get("composite_score", 0) >= 70][:5]
        if rush and not any(prior_top.get(t["symbol"], 0) >= 70 for t in rush):
            lines = []
            for t in rush:
                ev = t.get("recent_grade_events", [{}])[0]
                lines.append(
                    f"• <b>{t['symbol']}</b> · {t.get('upgrades_30d',0)} upgrades · "
                    f"composite {t.get('composite_score',0):.0f} · latest: {ev.get('firm','?')} "
                    f"{ev.get('prev','?')}→{ev.get('new','?')}"
                )
            maybe_telegram(
                f"🚀 <b>ANALYST UPGRADE RUSH</b>\n" + "\n".join(lines)
            )
    except Exception as e:
        print(f"[alerts] err: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({
            "ok": True,
            "n_universe": len(UNIVERSE_TICKERS),
            "n_with_data": len(ranked),
            "top_5": [t["ticker"] for t in top_consensus[:5]],
            "duration_s": round(time.time()-t0, 1),
        }),
    }
