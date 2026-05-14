"""
justhodl-retail-sentiment — Retail Sentiment Engine (BUILD 9/15)

WHY THIS EXISTS
===============
Retail flow is the marginal driver in many small-cap and meme moves.
Bloomberg's "Social Sentiment" indicator is $24k/yr.
This builds the same primitive from public data:

  ApeWisdom (Reddit aggregator) — mention counts across WSB/stocks/investing
  StockTwits — per-ticker bullish/bearish message tags + trending list

THE ALPHA: VELOCITY
===================
Absolute mention count is noisy — the alpha is in DELTA:
  mention_velocity = (mentions_now - mentions_24h_ago) / max(mentions_24h_ago, 1)
  rank_velocity = rank_24h_ago - rank  (positive = climbing)

A name jumping from #50 to #5 with 10x mentions is far more actionable
than the persistent #1 (which is already priced in by retail flow).

DATA SOURCES
============
ApeWisdom (working from AWS):
  /filter/all-stocks/page/1 — top 100 across ALL subreddits
  /filter/wallstreetbets/page/1 — WSB-only
  /filter/stocks/page/1 — r/stocks (more sober crowd)
  /filter/crypto/page/1 — crypto subs

StockTwits (working from AWS):
  /streams/symbol/{TKR}.json — last 30 messages with Bullish/Bearish tags
  /trending/symbols.json — top 30 trending with trending_score

OUTPUTS
=======
data/retail-sentiment.json:
  top_30 mention table with velocity + rank delta + sentiment
  biggest_velocity_surges (climbing fastest)
  biggest_rank_climbers (newcomers to the top)
  most_bullish_tickers (highest bull_bear_ratio)
  most_bearish_tickers
  stocktwits_trending (top 10 trending_score)
  subreddit_breakdown (WSB vs stocks vs investing)
  market_regime (MANIA/NORMAL/QUIET)

SCHEDULE
========
cron(0,30 * ? * * *) — every 30 minutes around the clock
(retail flow is global, doesn't pause at US close)
"""
import io, json, os, time, urllib.request, urllib.error, re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/retail-sentiment.json"
HISTORY_KEY = "data/retail-sentiment-history.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 20
MAX_PARALLEL = 8
N_TOP_FOR_STOCKTWITS = 25  # only fetch StockTwits for top 25 by mentions (rate-aware)

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# HTTP HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def http_get_json(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


# ═══════════════════════════════════════════════════════════════════════════
# DATA FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_apewisdom(filter_name, max_pages=2):
    """Fetch one or more pages from apewisdom. Returns list of result dicts."""
    out = []
    for page in range(1, max_pages + 1):
        try:
            data = http_get_json(
                f"https://apewisdom.io/api/v1.0/filter/{filter_name}/page/{page}",
                timeout=15)
            out.extend(data.get("results") or [])
        except Exception as e:
            print(f"  apewisdom {filter_name} p{page} err: {str(e)[:80]}")
    return out


def fetch_stocktwits_stream(ticker):
    """Fetch last 30 messages for a ticker, return sentiment counts + sample."""
    try:
        data = http_get_json(
            f"https://api.stocktwits.com/api/2/streams/symbol/{ticker}.json",
            timeout=15)
        msgs = data.get("messages") or []
        bull, bear, none_ = 0, 0, 0
        total_likes = 0
        for m in msgs:
            ent = m.get("entities") or {}
            sent = (ent.get("sentiment") or {}).get("basic") or m.get("sentiment") or "none"
            if sent == "Bullish": bull += 1
            elif sent == "Bearish": bear += 1
            else: none_ += 1
            total_likes += (m.get("likes") or {}).get("total") or 0
        n_total = len(msgs)
        n_tagged = bull + bear
        bull_pct = (bull / n_tagged * 100) if n_tagged > 0 else None
        bear_pct = (bear / n_tagged * 100) if n_tagged > 0 else None
        bull_bear_ratio = (bull / bear) if bear > 0 else (bull * 10 if bull > 0 else None)
        return {
            "n_messages": n_total,
            "n_bullish": bull,
            "n_bearish": bear,
            "n_untagged": none_,
            "bull_pct": round(bull_pct, 1) if bull_pct is not None else None,
            "bear_pct": round(bear_pct, 1) if bear_pct is not None else None,
            "bull_bear_ratio": round(bull_bear_ratio, 2) if bull_bear_ratio is not None else None,
            "total_likes": total_likes,
            "watchlist_count": ((data.get("symbol") or {}).get("watchlist_count")),
        }
    except Exception as e:
        return {"err": str(e)[:120]}


def fetch_stocktwits_trending():
    try:
        data = http_get_json(
            "https://api.stocktwits.com/api/2/trending/symbols.json", timeout=15)
        return [
            {"symbol": s.get("symbol"), "title": s.get("title"),
              "trending_score": round(s.get("trending_score", 0), 2),
              "watchlist_count": s.get("watchlist_count"),
              "sector": s.get("sector"), "industry": s.get("industry")}
            for s in (data.get("symbols") or [])
        ]
    except Exception as e:
        print(f"  trending err: {str(e)[:100]}")
        return []


# ═══════════════════════════════════════════════════════════════════════════
# COMPUTE METRICS
# ═══════════════════════════════════════════════════════════════════════════

def compute_velocity(row):
    """For an apewisdom entry, compute mention + rank velocity."""
    mentions = row.get("mentions", 0)
    prev = row.get("mentions_24h_ago", 0) or 0
    rank = row.get("rank")
    prev_rank = row.get("rank_24h_ago")

    velocity_pct = None
    if prev > 0:
        velocity_pct = round((mentions - prev) / prev * 100, 1)
    elif mentions > 0:
        velocity_pct = 9999.0  # brand new

    rank_climb = None
    if prev_rank is not None and rank is not None:
        rank_climb = prev_rank - rank  # positive = climbing

    return {"velocity_pct": velocity_pct, "rank_climb": rank_climb}


def classify_market_regime(all_results, prior_total):
    """Classify retail sentiment regime by total volume + breadth."""
    if not all_results:
        return "UNKNOWN", "No data", 0
    total_mentions = sum(r.get("mentions", 0) for r in all_results)
    n_active = sum(1 for r in all_results if r.get("mentions", 0) >= 10)

    if total_mentions == 0:
        return "QUIET", "No retail activity detected", total_mentions

    # Compare to prior_total if available
    delta_pct = None
    if prior_total and prior_total > 0:
        delta_pct = (total_mentions - prior_total) / prior_total * 100

    if total_mentions > 15000 or (delta_pct is not None and delta_pct > 80):
        return "MANIA", f"Total mentions {total_mentions:,} signals retail frenzy", total_mentions
    if total_mentions > 8000:
        return "ELEVATED", f"Total mentions {total_mentions:,} above-trend retail engagement", total_mentions
    if total_mentions < 3000:
        return "QUIET", f"Total mentions {total_mentions:,} below normal — retail disengaged", total_mentions
    return "NORMAL", f"Total mentions {total_mentions:,} in typical range", total_mentions


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

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
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"  tg err: {str(e)[:80]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== retail-sentiment v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Prior state
    try:
        prior = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_total = (prior.get("market_regime_data") or {}).get("total_mentions")
        prior_regime = prior.get("market_regime")
    except Exception:
        prior = {}
        prior_total = None
        prior_regime = None

    # ─── Parallel fetch: all subreddit filters + StockTwits trending ───
    print(f"  fetching apewisdom + stocktwits trending in parallel...")
    feeds = {}
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(fetch_apewisdom, "all-stocks", 2): "all_stocks",
            ex.submit(fetch_apewisdom, "wallstreetbets", 1): "wsb",
            ex.submit(fetch_apewisdom, "stocks", 1): "stocks",
            ex.submit(fetch_apewisdom, "investing", 1): "investing",
            ex.submit(fetch_stocktwits_trending): "trending",
        }
        for f in as_completed(futures):
            feeds[futures[f]] = f.result()

    all_stocks = feeds.get("all_stocks", [])
    wsb = feeds.get("wsb", [])
    stocks = feeds.get("stocks", [])
    investing = feeds.get("investing", [])
    trending = feeds.get("trending", [])

    print(f"  apewisdom: all={len(all_stocks)} wsb={len(wsb)} stocks={len(stocks)} investing={len(investing)}")
    print(f"  stocktwits trending: {len(trending)}")

    # ─── Enrich top 25 by mentions with StockTwits sentiment ───
    top_tickers = [r.get("ticker") for r in all_stocks[:N_TOP_FOR_STOCKTWITS] if r.get("ticker")]
    print(f"  fetching stocktwits streams for {len(top_tickers)} top tickers...")
    stwt = {}
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futs = {ex.submit(fetch_stocktwits_stream, t): t for t in top_tickers}
        for f in as_completed(futs):
            stwt[futs[f]] = f.result()

    # ─── Compose enriched top list ───
    enriched = []
    for r in all_stocks:
        tkr = r.get("ticker")
        vel = compute_velocity(r)
        s = stwt.get(tkr, {})
        entry = {
            "rank": r.get("rank"),
            "ticker": tkr,
            "name": r.get("name"),
            "mentions": r.get("mentions"),
            "mentions_24h_ago": r.get("mentions_24h_ago"),
            "rank_24h_ago": r.get("rank_24h_ago"),
            "upvotes": r.get("upvotes"),
            "velocity_pct": vel["velocity_pct"],
            "rank_climb": vel["rank_climb"],
        }
        if s and not s.get("err"):
            entry.update({
                "stwt_bull_pct": s.get("bull_pct"),
                "stwt_bear_pct": s.get("bear_pct"),
                "stwt_bull_bear_ratio": s.get("bull_bear_ratio"),
                "stwt_n_messages": s.get("n_messages"),
                "stwt_total_likes": s.get("total_likes"),
                "stwt_watchlist_count": s.get("watchlist_count"),
            })
        enriched.append(entry)

    # ─── Rankings ───
    # Biggest velocity surges (high mentions + high velocity)
    velocity_filtered = [e for e in enriched
                          if e.get("velocity_pct") is not None
                          and e.get("mentions", 0) >= 20]  # min absolute floor
    biggest_velocity = sorted(velocity_filtered,
                                key=lambda x: -x.get("velocity_pct", 0))[:15]

    # Biggest rank climbers (newcomers to the top)
    rank_filtered = [e for e in enriched
                      if e.get("rank_climb") is not None
                      and e.get("rank_climb", 0) > 0]
    biggest_climbers = sorted(rank_filtered,
                                key=lambda x: -x.get("rank_climb", 0))[:15]

    # Most bullish (need StockTwits data)
    bull_filtered = [e for e in enriched
                      if e.get("stwt_bull_bear_ratio") is not None
                      and e.get("stwt_n_messages", 0) >= 10]
    most_bullish = sorted(bull_filtered,
                            key=lambda x: -x.get("stwt_bull_bear_ratio", 0))[:10]
    most_bearish = sorted(bull_filtered,
                            key=lambda x: x.get("stwt_bull_bear_ratio", 999))[:10]

    # New entrants (not in apewisdom 24h ago = mentions_24h_ago is 0 or null)
    new_entrants = [e for e in enriched
                     if (e.get("mentions_24h_ago") or 0) <= 2
                     and e.get("mentions", 0) >= 20][:15]

    # Subreddit breakdown — compare WSB top 10 to stocks top 10
    wsb_top = [r.get("ticker") for r in wsb[:10] if r.get("ticker")]
    stocks_top = [r.get("ticker") for r in stocks[:10] if r.get("ticker")]
    investing_top = [r.get("ticker") for r in investing[:10] if r.get("ticker")]
    # Tickers that appear in WSB but not in stocks → "meme-only"
    wsb_only = [t for t in wsb_top if t not in stocks_top]
    stocks_only = [t for t in stocks_top if t not in wsb_top]
    consensus = [t for t in wsb_top if t in stocks_top]

    # Total mentions for regime
    total_mentions = sum(e.get("mentions", 0) for e in enriched)
    regime, signal, _ = classify_market_regime(enriched, prior_total)

    # ─── Build payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "sources": ["apewisdom.io", "api.stocktwits.com"],
        "elapsed_seconds": round(time.time() - started, 2),
        "n_all_stocks": len(all_stocks),
        "n_wsb": len(wsb),
        "n_stocks": len(stocks),
        "n_investing": len(investing),
        "n_with_stwt_data": sum(1 for e in enriched if e.get("stwt_n_messages") is not None),
        "market_regime": regime,
        "market_regime_signal": signal,
        "market_regime_data": {
            "total_mentions": total_mentions,
            "prior_total": prior_total,
            "delta_pct": round((total_mentions - prior_total) / prior_total * 100, 1) if prior_total else None,
        },
        "regime_changed_from_prior": (prior_regime != regime) if prior_regime else False,
        "top_30_by_mentions": enriched[:30],
        "ranked": {
            "biggest_velocity_surges": biggest_velocity,
            "biggest_rank_climbers": biggest_climbers,
            "most_bullish_stwt": most_bullish,
            "most_bearish_stwt": most_bearish,
            "new_entrants": new_entrants,
        },
        "stocktwits_trending": trending[:20],
        "subreddit_breakdown": {
            "wsb_top_10": wsb_top,
            "stocks_top_10": stocks_top,
            "investing_top_10": investing_top,
            "consensus_wsb_and_stocks": consensus,
            "wsb_only": wsb_only,
            "stocks_only": stocks_only,
        },
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=600")
        print(f"  ✓ data/retail-sentiment.json written ({round(len(json.dumps(payload))/1024,1)} KB)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # ─── Telegram on regime change or extreme readings ───
    alert_sent = False
    big_surges = [v for v in biggest_velocity[:5] if (v.get("velocity_pct") or 0) >= 200]
    if regime == "MANIA" or (prior_regime and prior_regime != regime) or big_surges:
        lines = [f"📱 *Retail Sentiment · {datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC*\n",
                  f"📊 Regime: *{regime}*",
                  f"_{signal}_\n",
                  f"📈 Total mentions: {total_mentions:,}"]
        if prior_regime and prior_regime != regime:
            lines.insert(2, f"_(was {prior_regime})_")
        if big_surges:
            lines.append("\n🚀 *Velocity surges (>200%):*")
            for s in big_surges[:5]:
                lines.append(f"  • {s.get('ticker')}: {s.get('mentions')} mentions "
                              f"(+{s.get('velocity_pct')}% vs 24h ago)")
        if biggest_climbers[:3]:
            lines.append("\n📊 *Rank climbers:*")
            for c in biggest_climbers[:3]:
                lines.append(f"  • {c.get('ticker')}: rank #{c.get('rank')} (was #{c.get('rank_24h_ago')}, Δ+{c.get('rank_climb')})")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "market_regime": regime,
        "total_mentions": total_mentions,
        "n_top_with_stwt": payload["n_with_stwt_data"],
        "n_velocity_surges": len(biggest_velocity),
        "n_rank_climbers": len(biggest_climbers),
        "n_new_entrants": len(new_entrants),
        "top_1": enriched[0].get("ticker") if enriched else None,
        "top_1_mentions": enriched[0].get("mentions") if enriched else None,
        "regime_changed": (prior_regime != regime) if prior_regime else False,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
