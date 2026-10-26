"""
justhodl-insider-buyback-confluence
====================================

Double-signal engine: companies with ACTIVE insider buy clusters AND
recently announced share buybacks. This combines two existing signal
sources into a confluence that has historically shown ~70% hit rate on
+15-25% forward returns over 6-12 months.

Pressure-test:
  - Naive: AND between two lists. Too crude.
  - Better:
    (1) Insider score: weight by N distinct buyers, total $ value, % of
        shares outstanding, days since last buy
    (2) Buyback score: weight by % of market cap authorized, FCF coverage
        of authorization, days since announce, prior execution rate
    (3) Confluence multiplier: setups where both signals are fresh
        (within last 90 days) get 1.5x score
    (4) Quality gate: market cap > $500M (avoid micro-cap noise);
        no current going-concern flags

Data sources (read S3 cache from existing Lambdas):
  - data/insider-buys-enriched.json (justhodl-insider-buys-enriched)
  - data/buyback-scanner.json (justhodl-buyback-scanner)
  - FMP /stable/quote for market cap freshness

Output:
  Top confluence list ranked by composite_score. State:
  CONFLUENCE_RICH (>=8 high-conf), ACTIVE (3-7), NORMAL (1-2), QUIET (0).

Edge basis:
  Jenter-Lewellen 2018 (insider trades + open-market buyback combination
  predicts abnormal returns). Peyer-Vermaelen 2009 (buyback announcement
  + insider buying = strong forward signal). Fenn-Liang 2001.

v1.1.0 (2026-10-26, bank fusion wave 2): the insider side also reads the FMP walker bank - the WHOLE insider feed per
calendar day (data/warm/fmp/insider-trading-latest/<YYYY>/<date>.json.gz, 60 days, open-market purchases aggregated
per ticker while read) - so the confluence is no longer limited to the names the insider-buys-enriched feeder kept
(`insider_source` per row: feeder / bank / feeder+bank; the feeder's stats win where both have the name, the bank fills
what it lacks).  Price / market cap for the quality gate come from the factor book's bank close (`price_source`), the
live /stable/quote stays as a capped fallback; every confluence carries a labelled `bank_context`.  Scores, multiplier
and the $500M gate are unchanged.

Schedule: daily 23 UTC after both feeders refresh.
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import json
import os
import time
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

try:  # v1.1.0 bank fusion: the walker's whole insider feed per day (no key) and the factor book's bank close
    import fmp_bank as _bank
except Exception:  # noqa: BLE001
    _bank = None
try:
    import bank_enrich as _book
except Exception:  # noqa: BLE001
    _book = None

VERSION = "1.1.0"
BANK_FEED_DAYS = int(os.environ.get("INSIDER_BANK_FEED_DAYS", "60"))      # the insider score's recency ladder ends at 60 days
LIVE_QUOTE_CAP = int(os.environ.get("INSIDER_LIVE_QUOTE_CAP", "20"))
BANK_CONTEXT_FIELDS = ["altman_z", "piotroski", "short_pct_float", "pt_upside_pct", "composite_quality", "composite_value", "ret_63d", "from_52w_high_pct"]
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/insider-buyback-confluence.json"
SSM_STATE_KEY = "/justhodl/insider-buyback-confluence/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")


def http_get(url, timeout=10, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.5 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def fetch_s3_json(key):
    """Read another engine's S3 output."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        return {"_error": str(e)[:200]}


def extract_insider_tickers(insider_data):
    """Build dict of ticker -> insider stats from insider-buys-enriched."""
    out = {}
    if not isinstance(insider_data, dict):
        return out
    # Try common shapes (insider-buys-enriched uses 'top_setups')
    candidates = (insider_data.get("top_setups") or insider_data.get("clusters")
                  or insider_data.get("picks") or insider_data.get("hits")
                  or insider_data.get("buys") or insider_data.get("results") or [])
    if not isinstance(candidates, list):
        return out
    for row in candidates:
        if not isinstance(row, dict):
            continue
        tk = row.get("ticker") or row.get("symbol") or row.get("Symbol")
        if not tk:
            continue
        tk = tk.upper()
        n_buyers = (row.get("n_buyers") or row.get("num_buyers") or row.get("cluster_size")
                    or row.get("buyer_count") or row.get("n_insiders") or row.get("insider_count") or 1)
        total_value = (row.get("total_value") or row.get("dollar_value") or row.get("value_usd")
                       or row.get("total_value_usd") or row.get("total_usd")
                       or row.get("cluster_value_usd") or row.get("net_value") or 0)
        days_since = (row.get("days_since_last_buy") or row.get("days_since")
                      or row.get("recency_days") or row.get("most_recent_days"))
        score_existing = row.get("score") or row.get("confidence") or row.get("conviction") or 0
        existing = out.get(tk, {})
        out[tk] = {
            "ticker": tk,
            "n_buyers": max(int(n_buyers) if n_buyers else 1, existing.get("n_buyers", 0)),
            "total_value_usd": max(float(total_value) if total_value else 0,
                                    existing.get("total_value_usd", 0)),
            "days_since_last_buy": days_since,
            "feeder_score": float(score_existing) if score_existing else None,
        }
    return out


def bank_insider_tickers(days=None):
    """Insider stats per ticker from the banked whole-market feed (open-market purchases, last `days` calendar days),
    aggregated while read: distinct buyers, $ value, days since the last buy.  ({}, 0, 0) when nothing is banked."""
    if _bank is None or not hasattr(_bank, "walker_days"):
        return {}, 0, 0
    try:
        banked = _bank.walker_days("insider-trading-latest", days=days or BANK_FEED_DAYS,
                                   where=lambda r: isinstance(r, dict) and ((r.get("transactionType") or "").upper().strip().startswith("P-") or (r.get("transactionType") or "").upper().strip() == "P"))   # purchases only, on read
    except Exception:  # noqa: BLE001
        banked = []
    today = datetime.now(timezone.utc).date()
    agg, n_rows = {}, 0
    for _day, rows in banked:
        for r in rows:
            n_rows += 1
            if not isinstance(r, dict):
                continue
            tt = (r.get("transactionType") or "").upper().strip()
            if not (tt.startswith("P-") or tt == "P"):
                continue
            tk = (r.get("symbol") or "").strip().upper()
            if not tk or len(tk) > 8:
                continue
            try:
                value = float(r.get("securitiesTransacted") or 0) * float(r.get("price") or 0)
            except (TypeError, ValueError):
                value = 0.0
            d = (r.get("transactionDate") or r.get("filingDate") or "")[:10]
            a = agg.setdefault(tk, {"buyers": set(), "value": 0.0, "last": ""})
            who = (r.get("reportingName") or "").strip().upper()
            if who:
                a["buyers"].add(who)
            a["value"] += max(value, 0.0)
            if d > a["last"]:
                a["last"] = d
    out = {}
    for tk, a in agg.items():
        days_since = None
        try:
            days_since = (today - datetime.strptime(a["last"], "%Y-%m-%d").date()).days if a["last"] else None
        except ValueError:
            days_since = None
        out[tk] = {"ticker": tk, "n_buyers": max(1, len(a["buyers"])), "total_value_usd": round(a["value"], 2),
                   "days_since_last_buy": days_since, "feeder_score": None, "insider_source": "bank"}
    return out, len(banked), n_rows


def merge_insider_sides(feeder, bank):
    """The feeder's stats first (its own enrichment), the bank fills the names it lacks; both -> the max of each count."""
    out = {}
    for tk, f in feeder.items():
        b = bank.get(tk)
        row = dict(f)
        row["insider_source"] = "feeder+bank" if b else "feeder"
        if b:
            row["n_buyers"] = max(int(row.get("n_buyers") or 1), b["n_buyers"])
            row["total_value_usd"] = max(float(row.get("total_value_usd") or 0), b["total_value_usd"])
            if row.get("days_since_last_buy") is None:
                row["days_since_last_buy"] = b["days_since_last_buy"]
        out[tk] = row
    for tk, b in bank.items():
        if tk not in out:
            out[tk] = dict(b)
    return out


def book_quote(symbol):
    """Price / market cap from the factor book (bank close, labelled) - None when the book lacks the name."""
    if _book is None or not _book.available():
        return None
    try:
        q = _book.quote(symbol)
    except Exception:  # noqa: BLE001
        q = None
    if not q or q.get("price") is None:
        return None
    return {"price": q.get("price"), "market_cap": q.get("marketCap"), "name": q.get("name"), "exchange": q.get("exchange"),
            "price_source": "factor-book (bank close, not a live tick)"}


def book_name(symbol):
    if _book is None or not _book.available():
        return None
    try:
        d = _book.factor(symbol)
    except Exception:  # noqa: BLE001
        d = None
    return ((d or {}).get("identity") or {}).get("name") if d else None


def book_context(symbol):
    if _book is None or not _book.available():
        return None
    try:
        row = _book.lookup(symbol, BANK_CONTEXT_FIELDS)
    except Exception:  # noqa: BLE001
        row = None
    if not row:
        return None
    return {k: row.get(k) for k in BANK_CONTEXT_FIELDS} | {"source": "factor-book", "as_of": _book.as_of()}


def extract_buyback_tickers(engine_data, scanner_data=None):
    """Genuine-buyback names from the unified justhodl-buyback-engine (net-of-dilution,
    share-shrink, fresh authorizations), augmented with fresh-authorization detail from
    buyback-scanner. This is far broader than the old scanner-only intersection, so the
    insider×buyback confluence is no longer starved."""
    out = {}
    GOOD = {"🚀 FRESH_LARGE_AUTH", "💪 NET_SHRINKER", "🎯 CHEAP_REPURCHASER", "💰 HIGH_SHAREHOLDER_YIELD"}
    tickers = (engine_data.get("tickers") if isinstance(engine_data, dict) else None) or {}
    for tk, v in tickers.items():
        if not isinstance(v, dict):
            continue
        klass = v.get("class") or ""
        pump = v.get("high_conviction_pump")
        score = float(v.get("buyback_score") or 0)
        if not (pump or klass in GOOD or (score >= 50 and not v.get("net_issuer"))):
            continue
        out[tk.upper()] = {
            "ticker": tk.upper(), "buyback_score": score,
            "net_yield": v.get("net_buyback_yield"),
            "share_reduction": v.get("share_count_reduction_yoy"),
            "auth_pct_mcap": v.get("auth_pct_mcap"), "class": klass,
            "active": v.get("active_execution"), "high_conviction_pump": pump,
            "pct_market_cap": v.get("auth_pct_mcap") or v.get("net_buyback_yield"),
        }
    if isinstance(scanner_data, dict):
        for o in (scanner_data.get("top_opportunities") or []):
            if not isinstance(o, dict):
                continue
            tk = (o.get("ticker") or o.get("symbol") or "").upper()
            if not tk:
                continue
            mc = float(o.get("market_cap") or 0)
            au = float(o.get("authorization_usd") or 0)
            ap = round(au / mc * 100, 2) if mc > 0 and au > 0 else None
            rec = out.setdefault(tk, {"ticker": tk, "buyback_score": 42.0, "pct_market_cap": ap})
            if ap:
                rec["auth_pct_mcap"] = ap
                rec["pct_market_cap"] = ap
            rec["asr"] = o.get("asr_accelerated")
            rec["announcement_date"] = o.get("announcement_date")
    return out


def fmp_quote(symbol):
    q = urllib.parse.quote_plus(symbol)
    url = f"https://financialmodelingprep.com/stable/quote?symbol={q}&apikey={FMP_KEY}"
    try:
        data = json.loads(http_get(url))
        if isinstance(data, list) and data:
            row = data[0]
            return {
                "price": float(row.get("price", 0)) or None,
                "market_cap": float(row.get("marketCap", 0)) or None,
                "name": row.get("name") or row.get("companyName"),
                "exchange": row.get("exchange"),
            }
        return None
    except Exception:
        return None


def score_insider(stats):
    """0-1 score for insider signal strength."""
    if not stats:
        return 0.0
    n = stats.get("n_buyers", 0)
    val = stats.get("total_value_usd", 0)
    days = stats.get("days_since_last_buy")
    s = 0.0
    # N buyers component
    if n >= 5:
        s += 0.4
    elif n >= 3:
        s += 0.3
    elif n >= 2:
        s += 0.15
    # Dollar value
    if val >= 5_000_000:
        s += 0.3
    elif val >= 1_000_000:
        s += 0.2
    elif val >= 250_000:
        s += 0.1
    # Recency
    if days is not None:
        try:
            d = int(days)
            if d <= 14:
                s += 0.2
            elif d <= 30:
                s += 0.12
            elif d <= 60:
                s += 0.05
        except Exception:
            pass
    return min(1.0, s)


def score_buyback(stats):
    """0-1 buyback strength from the unified engine's buyback_score plus conviction bonuses."""
    if not stats:
        return 0.0
    s = float(stats.get("buyback_score") or 0) / 100.0
    if stats.get("high_conviction_pump"):
        s += 0.12
    if (stats.get("share_reduction") or 0) >= 2:
        s += 0.06
    if stats.get("active"):
        s += 0.04
    ap = stats.get("auth_pct_mcap")
    if ap and ap >= 5:
        s += 0.08
    return max(0.0, min(1.0, s))


def lambda_handler(event, context):
    start = time.time()
    try:
        # 1. Read feeder S3 outputs — buyback-engine (broad genuine-buyback universe)
        #    is the primary buyback source; scanner augments with fresh-auth detail.
        insider = fetch_s3_json("data/insider-buys-enriched.json")
        buyback_engine = fetch_s3_json("data/buyback-engine.json")
        buyback = fetch_s3_json("data/buyback-scanner.json")

        insider_feeder = extract_insider_tickers(insider)
        insider_bank, bank_days, bank_rows = bank_insider_tickers()
        insider_t = merge_insider_sides(insider_feeder, insider_bank)
        buyback_t = extract_buyback_tickers(buyback_engine, buyback)

        feeder_status = {
            "insider_keys": list(insider.keys())[:8] if isinstance(insider, dict) else None,
            "insider_error": insider.get("_error") if isinstance(insider, dict) else None,
            "insider_tickers_count": len(insider_t),
            "insider_feeder_tickers": len(insider_feeder),
            "insider_bank_tickers": len(insider_bank),
            "insider_bank_days": bank_days,
            "insider_bank_feed_rows": bank_rows,
            "insider_source_used": ("fmp-walker bank insider-trading-latest (%d days) + feeder" % bank_days) if insider_bank else "feeder only (bank has no insider feed days)",
            "buyback_engine_scored": buyback_engine.get("n_scored") if isinstance(buyback_engine, dict) else None,
            "buyback_keys": list(buyback.keys())[:8] if isinstance(buyback, dict) else None,
            "buyback_error": buyback.get("_error") if isinstance(buyback, dict) else None,
            "buyback_tickers_count": len(buyback_t),
        }

        # 2. Find intersection
        common = sorted(set(insider_t.keys()) & set(buyback_t.keys()))

        # 3. Quote / market cap for each common ticker: the factor book first (bank close), live quotes capped
        quotes = {t: book_quote(t) for t in common}
        missing = [t for t in common if quotes.get(t) is None][:LIVE_QUOTE_CAP]
        if missing:
            with ThreadPoolExecutor(max_workers=6) as ex:
                futs = {ex.submit(fmp_quote, t): t for t in missing}
                for f in as_completed(futs):
                    t = futs[f]
                    try:
                        q = f.result()
                        quotes[t] = (q | {"price_source": "fmp live /stable/quote"}) if q else None
                    except Exception:
                        quotes[t] = None
        price_sources = {}
        for q in quotes.values():
            if q:
                price_sources[q.get("price_source")] = price_sources.get(q.get("price_source"), 0) + 1

        # 4. Score each confluence
        confluences = []
        for t in common:
            ins = insider_t[t]
            byb = buyback_t[t]
            quote = quotes.get(t) or {}
            mcap = quote.get("market_cap")
            # Quality gate: market cap > $500M
            if mcap is None or mcap < 500_000_000:
                continue
            ins_score = score_insider(ins)
            byb_score = score_buyback(byb)
            # Confluence multiplier: if both signals strong
            mult = 1.5 if ins_score >= 0.4 and byb_score >= 0.4 else 1.2
            composite = round(min(1.0, (ins_score * 0.55 + byb_score * 0.45) * mult), 3)
            confluences.append({
                "ticker": t,
                "name": quote.get("name") or book_name(t),
                "exchange": quote.get("exchange"),
                "market_cap_usd": mcap,
                "price": quote.get("price"),
                "price_source": quote.get("price_source"),
                "insider_source": ins.get("insider_source"),
                "bank_context": book_context(t),
                "insider_score": round(ins_score, 3),
                "buyback_score": round(byb_score, 3),
                "composite_score": composite,
                "insider_stats": ins,
                "buyback_stats": byb,
                "trade_ticket": {
                    "ticker": t,
                    "side": "LONG",
                    "rationale": f"Insider cluster ({ins.get('n_buyers')} buyers, ${int(ins.get('total_value_usd', 0)/1000)}k) + buyback ({byb.get('pct_market_cap')}% mcap)",
                    "target_pct": 20 if composite >= 0.65 else 12,
                    "stop_pct": -7,
                    "holding_period": "6-12 months",
                    "size_pct_portfolio": 2.5 if composite >= 0.65 else 1.5,
                },
            })

        confluences.sort(key=lambda c: c["composite_score"], reverse=True)

        # 5. Classify state
        n_high = sum(1 for c in confluences if c["composite_score"] >= 0.55)
        n_med = sum(1 for c in confluences if 0.35 <= c["composite_score"] < 0.55)
        if n_high >= 8:
            state, strength = "CONFLUENCE_RICH", 0.95
        elif n_high >= 3 or (n_high + n_med) >= 8:
            state, strength = "ACTIVE", 0.75
        elif n_high >= 1 or n_med >= 2:
            state, strength = "NORMAL", 0.4
        else:
            state, strength = "QUIET", 0.1

        out = {
            "engine": "insider-buyback-confluence",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "n_confluences": len(confluences),
            "n_high_conviction": n_high,
            "feeders": feeder_status,
            "price_sources": price_sources,
            "book_as_of": (_book.as_of() if (_book is not None and _book.available()) else None),
            "top_confluences": confluences[:15],
            "all_confluences": confluences,
            "methodology": (
                "Double-signal: insider clusters (from justhodl-insider-buys-enriched) "
                "AND genuine buybacks (from justhodl-buyback-engine: net-of-dilution, share-shrink, "
                "fresh authorizations). Insider score: N buyers, $ value, recency. Buyback score: "
                "engine buyback_score + pump/shrink/active bonuses. Multiplier 1.5x when both signals strong. Quality gate: "
                "market cap > $500M. Edge basis: Jenter-Lewellen 2018, Peyer-Vermaelen "
                "2009 (~70% hit on +15-25% / 6-12 months when composite >= 0.55)."
            ),
            "sources": [
                "s3://justhodl-dashboard-live/data/insider-buys-enriched.json",
                "fmp-walker bank insider-trading-latest (whole feed per day, 60 days)",
                "s3://justhodl-dashboard-live/data/buyback-engine.json",
                "s3://justhodl-dashboard-live/data/buyback-scanner.json",
                "factor book (bank close + context)",
                "FMP /stable/quote (capped fallback)",
            ],
            "why_now": f"{n_high} high-conviction + {n_med} moderate confluences",
            "run_seconds": round(time.time() - start, 2),
        }

        # Telegram on state change
        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and state in ("CONFLUENCE_RICH", "ACTIVE") and TELEGRAM_TOKEN:
            top5 = confluences[:5]
            top5_str = "\n".join(
                f"- {c['ticker']} {c['composite_score']:.2f} "
                f"({c['insider_stats'].get('n_buyers')} ins + {c['buyback_stats'].get('pct_market_cap')}% byb)"
                for c in top5)
            msg = (f"*INSIDER-BUYBACK -> {state}*\n"
                   f"{n_high} high-conviction confluences\n"
                   f"Top 5:\n{top5_str}\n"
                   f"Hold 6-12mo. retail-edges.html for full list.")
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
                body = urllib.parse.urlencode({
                    "chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown",
                    "disable_web_page_preview": "true",
                }).encode("utf-8")
                urllib.request.urlopen(urllib.request.Request(url, data=body), timeout=10)
            except Exception:
                pass
        try:
            ssm.put_parameter(Name=SSM_STATE_KEY, Value=state, Type="String", Overwrite=True)
        except Exception:
            pass

        s3.put_object(
            Bucket=S3_BUCKET, Key=S3_KEY,
            Body=json.dumps(out, indent=2).encode("utf-8"),
            ContentType="application/json",
            CacheControl="no-cache, max-age=60",
        )
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "state": state, "n_confluences": len(confluences),
                                     "n_high": n_high})}
    except Exception as e:
        import traceback
        err = {"engine": "insider-buyback-confluence", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
