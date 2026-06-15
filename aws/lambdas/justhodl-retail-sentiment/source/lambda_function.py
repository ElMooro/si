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
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import equity_enrich as EE

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/retail-sentiment.json"
HISTORY_KEY = "data/retail-sentiment-history.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 20
MAX_PARALLEL = 8
N_TOP_FOR_STOCKTWITS = 25  # only fetch StockTwits for top 25 by mentions (rate-aware)
FMP_KEY = os.environ.get("FMP_KEY") or "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
FMP_BASE = "https://financialmodelingprep.com/stable"
N_TOP_FOR_QUOTES = 35  # fetch price for the top names shown on the page

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

def fetch_quote(ticker):
    """Live price + day change + 52w position + relative volume (FMP /stable/quote)."""
    try:
        url = f"{FMP_BASE}/quote?symbol={ticker}&apikey={FMP_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-retail"})
        with urllib.request.urlopen(req, timeout=8) as r:
            arr = json.loads(r.read())
        q = (arr[0] if isinstance(arr, list) and arr else arr) or {}
        if not isinstance(q, dict):
            return {}
        chg = q.get("changePercentage")
        if chg is None:
            chg = q.get("changesPercentage")
        price = q.get("price")
        yh = q.get("yearHigh")
        off_high = round((price / yh - 1) * 100, 1) if (price and yh) else None
        vol = q.get("volume")
        avg = q.get("avgVolume") or q.get("averageVolume")
        rvol = round(vol / avg, 2) if (vol and avg) else None
        return {
            "price": price,
            "change_pct": round(chg, 2) if isinstance(chg, (int, float)) else None,
            "off_52w_high": off_high, "rel_volume": rvol, "market_cap": q.get("marketCap"),
        }
    except Exception:
        return {}


def compute_heat(e):
    """Transparent 0-100 'retail heat': blend of mention volume, velocity, rank-climb, bullishness."""
    mentions = e.get("mentions") or 0
    vel = e.get("velocity_pct")
    climb = e.get("rank_climb") or 0
    bull = e.get("stwt_bull_pct")
    m_c = min(mentions / 300.0, 1.0) * 100
    v_c = (min((vel or 0) / 300.0, 1.0) * 100) if vel is not None else 40
    c_c = min(max(climb, 0) / 50.0, 1.0) * 100
    b_c = bull if bull is not None else 50
    heat = 0.30 * v_c + 0.30 * m_c + 0.20 * c_c + 0.20 * b_c
    return round(max(0, min(100, heat)))


def buzz_state(e):
    """Is the chatter translating into price? The actionable read.
    IGNITION=just appeared · MOMENTUM=buzz+price up · DIVERGENCE=buzz+price down · RISING/FADING/STEADY."""
    vel = e.get("velocity_pct"); chg = e.get("change_pct")
    is_new = (e.get("mentions_24h_ago") or 0) <= 2 and (e.get("mentions") or 0) >= 20
    if is_new:
        return "IGNITION"
    if chg is None:
        return "STEADY"
    hot = (vel or 0) >= 50 or (e.get("rank_climb") or 0) >= 10
    if hot and chg >= 1.5:
        return "MOMENTUM"
    if hot and chg <= -1.5:
        return "DIVERGENCE"
    if chg >= 1.5:
        return "RISING"
    if chg <= -1.5:
        return "FADING"
    return "STEADY"


def log_retail_signals(momentum, divergence):
    """Log MOMENTUM (predict UP) and DIVERGENCE (predict fade) calls to justhodl-signals for forward grading vs SPY."""
    try:
        tbl = boto3.resource("dynamodb", region_name="us-east-1").Table("justhodl-signals")
        now = datetime.now(timezone.utc); d0 = now.strftime("%Y-%m-%d"); n = 0
        def put(e, stype, direction):
            px = e.get("price")
            if not px:
                return 0
            tbl.put_item(Item={
                "signal_id": f"{stype}#{e['ticker']}#{d0}", "signal_type": stype,
                "predicted_direction": direction, "baseline_price": str(px), "benchmark": "SPY",
                "measure_against": "ticker", "check_windows": ["day_5", "day_21", "day_63"],
                "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
                "status": "pending", "schema_version": "2", "horizon_days_primary": 21,
                "ttl": int(now.timestamp()) + 120 * 86400, "signal_value": str(e.get("heat")),
                "metadata": {"buzz_state": str(e.get("buzz_state")), "mentions": str(e.get("mentions")),
                             "velocity_pct": str(e.get("velocity_pct")), "engine": "retail-sentiment"},
            })
            return 1
        for e in (momentum or [])[:10]:
            n += put(e, "retail_momentum", "UP")
        for e in (divergence or [])[:10]:
            n += put(e, "retail_divergence", "DOWN")
        return n
    except Exception as ex:
        print(f"[retail-signals] {str(ex)[:120]}")
        return 0


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

    # ─── Price confirmation: is the chatter translating into a move? ───
    quote_tickers = [e.get("ticker") for e in enriched[:N_TOP_FOR_QUOTES] if e.get("ticker")]
    print(f"  fetching quotes for {len(quote_tickers)} top tickers...")
    quotes = {}
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        qf = {ex.submit(fetch_quote, t): t for t in quote_tickers}
        for f in as_completed(qf):
            quotes[qf[f]] = f.result() or {}
    for e in enriched:
        q = quotes.get(e.get("ticker")) or {}
        if q:
            e["price"] = q.get("price"); e["change_pct"] = q.get("change_pct")
            e["off_52w_high"] = q.get("off_52w_high"); e["rel_volume"] = q.get("rel_volume")
            e["market_cap"] = q.get("market_cap")
        e["heat"] = compute_heat(e)
        e["buzz_state"] = buzz_state(e)
    n_with_price = sum(1 for e in enriched if e.get("change_pct") is not None)
    print(f"  price-confirmed {n_with_price}/{len(quote_tickers)}")

    # ─── #4 Attention history: sustained vs spike (multi-day mention trend) ───
    HIST_KEY = "data/retail-attention-history.json"
    try:
        _hraw = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=HIST_KEY)["Body"].read())
        by_t = _hraw.get("by_ticker", {}) if isinstance(_hraw, dict) else {}
    except Exception:
        by_t = {}
    today_d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for e in enriched[:60]:
        tk = e.get("ticker")
        if not tk:
            continue
        ser = [p for p in by_t.get(tk, []) if p.get("date") != today_d]
        ser.append({"date": today_d, "mentions": e.get("mentions"), "heat": e.get("heat"), "wl": e.get("stwt_watchlist_count")})
        by_t[tk] = sorted(ser, key=lambda p: p.get("date") or "")[-30:]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=31)).strftime("%Y-%m-%d")
    by_t = {t: ss for t, ss in by_t.items() if ss and (ss[-1].get("date") or "") >= cutoff}
    if len(by_t) > 400:
        by_t = dict(sorted(by_t.items(), key=lambda kv: (kv[1][-1].get("mentions") or 0), reverse=True)[:400])

    def attention_stage(ms):
        if len(ms) < 3:
            return None
        cur, prev = ms[-1], ms[-2]
        srt = sorted(ms[:-1]); med = srt[len(srt) // 2] or 1; mx = max(ms) or 1
        days_elev = sum(1 for m in ms[-5:] if m >= 0.5 * mx)
        if cur >= 2.5 * med and cur >= 30:
            return "SPIKE"
        if days_elev >= 4:
            return "SUSTAINED"
        if len(ms) >= 3 and cur > prev >= ms[-3]:
            return "BUILDING"
        if cur < prev:
            return "COOLING"
        return "STEADY"

    for e in enriched:
        ser = by_t.get(e.get("ticker"), [])
        ms = [(p.get("mentions") or 0) for p in ser]
        if ms:
            e["mentions_hist"] = ms[-14:]
            e["days_tracked"] = len(ser)
            st = attention_stage(ms)
            if st:
                e["attention_stage"] = st
            if len(ms) >= 8:
                wk = ms[-8] or 1
                e["trend_7d_pct"] = round((ms[-1] - wk) / max(wk, 1) * 100)
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=HIST_KEY,
            Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "by_ticker": by_t},
                            separators=(",", ":"), default=str).encode(),
            ContentType="application/json", CacheControl="public, max-age=600")
        print(f"  attention-history: {len(by_t)} tickers tracked")
    except Exception as _e:
        print(f"[retail-hist] save fail {str(_e)[:80]}")

    # ─── #2 Contrarian / crowding: exhaustion (fade risk) vs capitulation (bottom) ───
    _ment_all = sorted([(e.get("mentions") or 0) for e in enriched], reverse=True)
    def _ment_pctile(m):
        if not _ment_all:
            return 0
        below = sum(1 for x in _ment_all if x < m)
        return round(below / len(_ment_all) * 100)
    for e in enriched:
        m = e.get("mentions") or 0; bull = e.get("stwt_bull_pct"); off_hi = e.get("off_52w_high")
        chg = e.get("change_pct"); vel = e.get("velocity_pct") or 0
        mp = _ment_pctile(m); e["mention_pctile"] = mp
        ext = max(0, min(100, (off_hi + 15) / 15 * 100)) if off_hi is not None else 50
        bx = bull if bull is not None else 50
        e["crowding_score"] = round(0.45 * mp + 0.30 * bx + 0.25 * ext)
        flag = None
        if bull is not None and bull >= 80 and mp >= 90 and (off_hi is None or off_hi > -8) and vel >= 80:
            flag = "EXHAUSTION"      # crowded + euphoric + extended → fade risk
        elif bull is not None and bull <= 35 and ((off_hi is not None and off_hi < -40) or (chg is not None and chg <= -6)):
            flag = "CAPITULATION"    # bearish + washed-out → possible bottom
        if flag:
            e["contrarian_flag"] = flag
    crowded_exhaustion = sorted([e for e in enriched if e.get("contrarian_flag") == "EXHAUSTION"],
                                key=lambda x: -(x.get("crowding_score") or 0))[:10]
    capitulation = sorted([e for e in enriched if e.get("contrarian_flag") == "CAPITULATION"],
                          key=lambda x: -(x.get("mention_pctile") or 0))[:10]

    # ─── #3 Squeeze overlay: retail heat × short interest (the GME/AMC mechanic) ───
    try:
        si_feed, f13_feed, _fwd, _ch = EE.load_confirmation_feeds()
    except Exception as _e:
        print(f"[retail-squeeze] feeds {str(_e)[:80]}"); si_feed, f13_feed = {}, {}
    for e in enriched:
        sdat = si_feed.get(e.get("ticker")) or {}
        sp = sdat.get("latest_short_pct") or sdat.get("short_interest_pct") or sdat.get("short_pct")
        dtc = sdat.get("days_to_cover") or sdat.get("days_cover")
        ff = f13_feed.get(e.get("ticker")) or {}
        if ff.get("n_funds_holding") is not None:
            e["sm_funds"] = ff.get("n_funds_holding")
        try:
            spf = float(sp) if sp is not None else None
        except Exception:
            spf = None
        if spf is not None:
            e["short_pct"] = round(spf, 1)
            if dtc is not None:
                e["days_to_cover"] = dtc
            heat = e.get("heat") or 0
            sp_c = min(spf / 30.0, 1) * 100
            dtc_c = (min(float(dtc) / 10.0, 1) * 100) if dtc else 50
            e["squeeze_score"] = round(0.50 * heat + 0.35 * sp_c + 0.15 * dtc_c)
            if heat >= 50 and spf >= 15:
                e["squeeze_setup"] = True
    squeeze_radar = sorted([e for e in enriched if e.get("squeeze_setup")],
                           key=lambda x: -(x.get("squeeze_score") or 0))[:10]
    n_squeeze = len(squeeze_radar)

    # ─── #6 Cross-source corroboration: independent venues confirming the buzz ───
    def _load_json(key):
        try:
            return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        except Exception:
            return {}
    def _collect_tickers(obj, depth=0, acc=None):
        if acc is None:
            acc = set()
        if depth > 4:
            return acc
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k in ("by_ticker", "by_symbol") and isinstance(v, dict):
                    for tk in v.keys():
                        if isinstance(tk, str) and 1 <= len(tk) <= 6:
                            acc.add(tk.upper())
                elif k in ("ticker", "symbol") and isinstance(v, str) and 1 <= len(v) <= 6:
                    acc.add(v.upper())
                else:
                    _collect_tickers(v, depth + 1, acc)
        elif isinstance(obj, list):
            for it in obj[:250]:
                _collect_tickers(it, depth + 1, acc)
        return acc
    news_set = _collect_tickers(_load_json("data/news-velocity.json"))
    # Search venue = LIVE ticker-trends (google-trends Lambda retired ops 547). Count only RISING/stealth search interest, not the whole tracked universe.
    _tt = _load_json("data/ticker-trends.json")
    trends_set = set()
    for _r in (_tt.get("all_results") or []):
        if isinstance(_r, dict) and _r.get("ticker"):
            _v = _r.get("velocity")
            if _r.get("stealth") or (_v is not None and _v >= 1.5):
                trends_set.add(str(_r["ticker"]).upper())
    options_set = _collect_tickers(_load_json("data/options-flow.json"))
    stwt_trend_set = set((t.get("symbol") or "").upper() for t in trending if t.get("symbol"))
    print(f"  corroboration: news={len(news_set)} trends={len(trends_set)} options={len(options_set)} stwt_trend={len(stwt_trend_set)}")
    for e in enriched:
        tk = (e.get("ticker") or "").upper()
        srcs = ["Reddit"]
        if e.get("stwt_n_messages") is not None or tk in stwt_trend_set:
            srcs.append("StockTwits")
        if tk in news_set:
            srcs.append("News")
        if tk in trends_set:
            srcs.append("Search")
        if tk in options_set:
            srcs.append("Options")
        e["corroboration"] = srcs
        e["corroboration_count"] = len(srcs)
    multi_venue_confirmed = sorted(
        [e for e in enriched if (e.get("corroboration_count") or 0) >= 3 and (e.get("mentions") or 0) >= 15],
        key=lambda x: (-(x.get("corroboration_count") or 0), -(x.get("heat") or 0)))[:12]

    # ─── #7 Retail flow proxy: options call/put skew + watchlist growth ───
    opt_doc = _load_json("data/options-flow.json")
    opt_map = {}
    for r in (opt_doc.get("all_qualifying") or (opt_doc.get("summary", {}) or {}).get("top_25_overall") or []):
        sym = (r.get("symbol") or "").upper()
        if not sym:
            continue
        m = r.get("metrics") or r
        opt_map[sym] = {
            "cpr": m.get("avg_cpr_recent_5d") if m.get("avg_cpr_recent_5d") is not None else r.get("cpr_recent"),
            "call_surge": m.get("call_vol_surge") if m.get("call_vol_surge") is not None else r.get("call_vol_surge"),
            "score": r.get("score"),
        }
    for e in enriched:
        o = opt_map.get((e.get("ticker") or "").upper())
        if o:
            e["opt_cpr"] = o["cpr"]; e["opt_call_surge"] = o["call_surge"]; e["opt_score"] = o["score"]
            cpr = o["cpr"]; surge = o["call_surge"]
            if cpr is not None and cpr >= 1.3 and (surge is None or surge >= 1.3):
                e["flow_signal"] = "BULLISH_CALLS"
            elif cpr is not None and cpr <= 0.7:
                e["flow_signal"] = "PUT_HEAVY"
        ser = by_t.get(e.get("ticker"), [])
        wls = [p.get("wl") for p in ser if p.get("wl") is not None]
        if len(wls) >= 2 and wls[-2]:
            e["watchlist_chg_pct"] = round((wls[-1] - wls[-2]) / wls[-2] * 100, 1)
    flow_leaders = sorted([e for e in enriched if e.get("flow_signal") == "BULLISH_CALLS" and (e.get("mentions") or 0) >= 15],
                          key=lambda x: -(x.get("heat") or 0))[:10]

    # ─── #9 Manipulation / quality flags (trust the feed) ───
    for e in enriched:
        vel = e.get("velocity_pct"); chg = e.get("change_pct"); relvol = e.get("rel_volume")
        ment = e.get("mentions") or 0; stwt_n = e.get("stwt_n_messages")
        qf = []
        if vel is not None and vel >= 300 and ment >= 20:
            if ((chg is None) or (chg <= 1)) and ((relvol is None) or (relvol < 1.2)):
                qf.append("UNCONFIRMED_SPIKE")   # loud, but no price/volume follow-through
        if ment >= 50 and (stwt_n is not None and stwt_n < 5):
            qf.append("SINGLE_VENUE")            # Reddit echo not seen on StockTwits
        if vel is not None and vel >= 800:
            qf.append("PARABOLIC_BUZZ")          # extreme velocity = pump risk
        if qf:
            e["quality_flags"] = qf
    suspicious = sorted([e for e in enriched if e.get("quality_flags")],
                        key=lambda x: -(x.get("velocity_pct") or 0))[:10]
    data_quality = {
        "price_coverage_pct": round(n_with_price / max(len(enriched), 1) * 100),
        "stwt_coverage_pct": round(sum(1 for e in enriched if e.get("stwt_n_messages") is not None) / max(len(enriched), 1) * 100),
        "n_suspicious": len(suspicious),
        "note": "Mention counts can be botted or pumped. Flags surface attention with no price/volume/multi-venue confirmation — caution, not conviction.",
    }

    # ─── #10 Signal decay / staging: where each name sits in its lifecycle ───
    def lifecycle_stage(ser, e):
        ms = [(p.get("mentions") or 0) for p in ser]
        hs = [(p.get("heat") or 0) for p in ser]
        chg = e.get("change_pct"); vel = e.get("velocity_pct")
        if len(ms) < 3:
            if vel is not None and vel >= 150 and (chg is None or chg >= -1):
                return "IGNITING"
            return None
        m_now, m_prev, m_max = ms[-1], ms[-2], (max(ms) or 1)
        cur_h, mx_h = hs[-1], (max(hs) or 1)
        if m_now >= m_prev and m_now < 0.7 * m_max and (chg is None or chg >= 0):
            return "IGNITING"
        if m_now >= 0.8 * m_max and cur_h >= 0.85 * mx_h:
            return "PEAKING"
        if m_now < m_prev and m_now < 0.6 * m_max:
            return "FADING"
        return "ACTIVE"
    for e in enriched:
        ls = lifecycle_stage(by_t.get(e.get("ticker"), []), e)
        if ls:
            e["lifecycle"] = ls
    _by_heat = sorted(enriched, key=lambda x: -(x.get("heat") or 0))
    igniting = [e for e in _by_heat if e.get("lifecycle") == "IGNITING"][:10]
    peaking = [e for e in _by_heat if e.get("lifecycle") == "PEAKING"][:10]
    fading = [e for e in _by_heat if e.get("lifecycle") == "FADING"][:10]
    _persist = []
    for tk, ser in by_t.items():
        hs = [(p.get("heat") or 0) for p in ser]
        if len(hs) >= 4 and max(hs) > 0:
            _persist.append(sum(1 for h in hs if h >= 0.5 * max(hs)))
    _persist.sort()
    signal_persistence = {
        "median_days_elevated": (_persist[len(_persist) // 2] if _persist else None),
        "n_tickers_measured": len(_persist),
        "note": "Median days a hot name stays within 50% of its peak heat — a half-life proxy. Accumulates as history grows.",
    }

    # ─── #8 Theme rollup: what retail is rotating into (curated theme map) ───
    THEMES = {
        "AI": ["NVDA","AMD","SMCI","PLTR","AVGO","MRVL","ARM","TSM","MSFT","GOOGL","GOOG","META","DELL","ANET","VRT","CRWV","BBAI","SOUN","AI"],
        "Quantum": ["IONQ","RGTI","QBTS","QUBT","ARQQ","LAES"],
        "Nuclear/Uranium": ["SMR","OKLO","CCJ","LEU","UEC","NNE","CEG","VST","TLN","UUUU","DNN"],
        "Crypto-equity": ["MSTR","COIN","MARA","RIOT","CLSK","HUT","BITF","HOOD","CIFR","WULF","BTBT","IREN","SMLR","BMNR"],
        "EV/Battery": ["TSLA","RIVN","LCID","NIO","XPEV","LI","QS","CHPT","BLNK"],
        "Space/eVTOL": ["RKLB","ASTS","LUNR","RDW","ACHR","JOBY","PL","SPCE"],
        "Biotech/GLP-1": ["LLY","NVO","VKTX","HIMS","ALT","SMMT"],
        "Meme/Retail-classic": ["GME","AMC","BB","KOSS","TLRY","DJT","RDDT","BYND","OPEN"],
        "Semis": ["NVDA","AMD","INTC","MU","TSM","AVGO","MRVL","ARM","QCOM","ASML","LRCX","AMAT","KLAC"],
    }
    _ta = {}
    for e in enriched:
        tk = (e.get("ticker") or "").upper()
        for th, lst in THEMES.items():
            if tk in lst:
                a = _ta.setdefault(th, {"theme": th, "n_names": 0, "total_mentions": 0, "_sv": 0, "_nv": 0, "_sc": 0, "_nc": 0, "_sb": 0, "_nb": 0, "names": []})
                a["n_names"] += 1; a["total_mentions"] += (e.get("mentions") or 0)
                if e.get("velocity_pct") is not None:
                    a["_sv"] += e["velocity_pct"]; a["_nv"] += 1
                if e.get("change_pct") is not None:
                    a["_sc"] += e["change_pct"]; a["_nc"] += 1
                if e.get("stwt_bull_pct") is not None:
                    a["_sb"] += e["stwt_bull_pct"]; a["_nb"] += 1
                a["names"].append({"ticker": tk, "mentions": e.get("mentions"), "heat": e.get("heat"), "change_pct": e.get("change_pct")})
    theme_rollup = []
    for th, a in _ta.items():
        a["avg_velocity"] = round(a["_sv"] / a["_nv"]) if a["_nv"] else None
        a["avg_change"] = round(a["_sc"] / a["_nc"], 1) if a["_nc"] else None
        a["avg_bull"] = round(a["_sb"] / a["_nb"]) if a["_nb"] else None
        a["names"] = sorted(a["names"], key=lambda x: -(x.get("mentions") or 0))[:6]
        for k in ("_sv", "_nv", "_sc", "_nc", "_sb", "_nb"):
            a.pop(k, None)
        theme_rollup.append(a)
    theme_rollup = sorted(theme_rollup, key=lambda x: -(x.get("total_mentions") or 0))

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

    # Hottest by composite heat (what retail is most excited about right now)
    hottest = sorted([e for e in enriched if (e.get("mentions") or 0) >= 15],
                      key=lambda x: -(x.get("heat") or 0))[:20]
    # Buzz CONFIRMED by price (piling in AND it's working) vs DIVERGING (talk, but fading)
    momentum_confirmed = sorted([e for e in enriched if e.get("buzz_state") == "MOMENTUM"],
                                 key=lambda x: -(x.get("heat") or 0))[:12]
    fading_divergence = sorted([e for e in enriched if e.get("buzz_state") == "DIVERGENCE"],
                                key=lambda x: -(x.get("heat") or 0))[:12]

    # ─── #1 Track record: log buzz calls + grade prior calls forward vs SPY ───
    n_signals = log_retail_signals(momentum_confirmed, fading_divergence)
    try:
        track_record = {
            "momentum": EE.grade_track_record("retail_momentum", "data/retail-momentum-track.json"),
            "divergence": EE.grade_track_record("retail_divergence", "data/retail-divergence-track.json"),
        }
    except Exception as _e:
        print(f"[retail-track] {str(_e)[:100]}"); track_record = None

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

    # ─── #11 Watchlist-aware alerting (Telegram best-effort + on-page feed) ───
    try:
        _astate = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/retail-alert-state.json")["Body"].read())
    except Exception:
        _astate = {}
    if not isinstance(_astate, dict):
        _astate = {}
    last_alert = _astate.get("last_alert") if isinstance(_astate.get("last_alert"), dict) else {}
    held = set()
    try:
        held = set(_collect_tickers(json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/portfolio.json")["Body"].read())))
    except Exception:
        pass
    now_ep = int(time.time()); COOLDOWN = 12 * 3600
    events = []
    def _consider(e, etype, detail):
        tk = e.get("ticker")
        if not tk:
            return
        key = f"{tk}#{etype}"
        if now_ep - (last_alert.get(key, 0) or 0) < COOLDOWN:
            return
        last_alert[key] = now_ep
        events.append({"ticker": tk, "type": etype, "detail": detail, "held": tk in held,
                       "heat": e.get("heat"), "change_pct": e.get("change_pct"), "ts": now_ep})
    for e in (squeeze_radar or [])[:5]:
        if (e.get("squeeze_score") or 0) >= 70:
            _consider(e, "SQUEEZE", f"heat {e.get('heat')}, short {e.get('short_pct')}%, score {e.get('squeeze_score')}")
    for e in (igniting or [])[:5]:
        if (e.get("heat") or 0) >= 70:
            _consider(e, "IGNITION", f"heat {e.get('heat')}, {int(e.get('velocity_pct') or 0)}% velocity")
    for e in (fading_divergence or [])[:5]:
        if (e.get("heat") or 0) >= 55:
            _consider(e, "DIVERGENCE", f"loud but price {e.get('change_pct')}%")
    for e in (crowded_exhaustion or [])[:3]:
        _consider(e, "EXHAUSTION", f"crowding {e.get('crowding_score')}, {e.get('stwt_bull_pct')}% bull")
    try:
        _af = json.loads(s3.get_object(Bucket=S3_BUCKET, Key="data/retail-alerts.json")["Body"].read())
        feed = _af.get("alerts", []) if isinstance(_af, dict) else []
    except Exception:
        feed = []
    feed = (events + feed)
    feed = [a for a in feed if now_ep - (a.get("ts") or 0) <= 7 * 86400][:60]
    recent_alerts = feed[:12]
    if events:
        _em = {"SQUEEZE": "🎯", "IGNITION": "🌱", "DIVERGENCE": "⚠️", "EXHAUSTION": "🧨"}
        lines = ["📱 RETAIL ALERTS"] + [f"{_em.get(a['type'],'•')} {'⭐' if a['held'] else ''}{a['ticker']} {a['type']} — {a['detail']}" for a in events[:10]]
        try:
            send_telegram("\n".join(lines))
        except Exception as _e:
            print(f"[retail-alert] tg {str(_e)[:60]}")
    _astate["last_alert"] = last_alert
    try:
        s3.put_object(Bucket=S3_BUCKET, Key="data/retail-alert-state.json",
                      Body=json.dumps(_astate, default=str).encode(), ContentType="application/json")
        s3.put_object(Bucket=S3_BUCKET, Key="data/retail-alerts.json",
                      Body=json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "alerts": feed}, default=str).encode(),
                      ContentType="application/json", CacheControl="public, max-age=300")
        print(f"  alerts: {len(events)} new, {len(feed)} in feed")
    except Exception as _e:
        print(f"[retail-alert] save {str(_e)[:60]}")

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
            "hottest": hottest,
            "crowded_exhaustion": crowded_exhaustion,
            "capitulation": capitulation,
            "squeeze_radar": squeeze_radar,
            "multi_venue_confirmed": multi_venue_confirmed,
            "flow_leaders": flow_leaders,
            "suspicious": suspicious,
            "igniting": igniting,
            "peaking": peaking,
            "fading": fading,
            "momentum_confirmed": momentum_confirmed,
            "fading_divergence": fading_divergence,
            "biggest_velocity_surges": biggest_velocity,
            "biggest_rank_climbers": biggest_climbers,
            "most_bullish_stwt": most_bullish,
            "most_bearish_stwt": most_bearish,
            "new_entrants": new_entrants,
        },
        "n_with_price": n_with_price,
        "data_quality": data_quality,
        "signal_persistence": signal_persistence,
        "theme_rollup": theme_rollup,
        "recent_alerts": recent_alerts,
        "signals_logged": n_signals,
        "track_record": track_record,
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
