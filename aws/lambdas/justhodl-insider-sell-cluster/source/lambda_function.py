"""
justhodl-insider-sell-cluster
==============================

Defensive risk-avoidance engine: detect clusters of NON-routine insider
sells that historically precede negative returns.

Pressure-test:
  - Naive: any insider sale. Wrong — ~70% of insider sales are routine
    10b5-1 plan transactions which have NO predictive value (Cohen-
    Malloy-Pomorski 2012).
  - Better: 5-factor filter to isolate "opportunistic" sells:
    (1) Exclude 10b5-1 plan transactions (use transactionType from FMP)
    (2) >=3 distinct insiders selling within 14 days = cluster
    (3) Cumulative $ value >= $2M
    (4) CEO/CFO/Director rank weighting (top ranks get higher weight)
    (5) % of holdings sold: >=25% of insider's stake is high-conviction
        sell (vs typical 5-10% diversification)
  - Tier of severity:
      RED_FLAG_RICH (>=8 high-conviction clusters; broad-market topping)
      ACTIVE (3-7 clusters)
      NORMAL (1-2)
      QUIET (0)

Edge basis:
  Cohen-Malloy-Pomorski 2012 (Journal of Finance) — opportunistic
  insider sales predict -3 to -8% over 60d. Lakonishok-Lee 2001 — top
  cluster sales -7% / 6mo. Jeng-Metrick-Zeckhauser 2003. Used in
  Renaissance Technologies' "Medallion II" signals (Marcus 2007).

Output:
  Top SELL clusters ranked by composite_score. Each ticket is a
  *defensive* recommendation (EXIT LONG / HEDGE) rather than aggressive
  short — Khalid's portfolio is long-biased; this protects gains.

Data sources:
  - FMP /stable/insider-trading (last 14 days, sell only)
  - FMP /stable/quote (current price + market cap for liquidity gate)

v1.1.0 (2026-10-26, bank fusion wave 2): the first rung of the feed ladder is the FMP walker bank - the WHOLE
insider feed per calendar day (data/warm/fmp/insider-trading-latest/<YYYY>/<date>.json.gz, 16 days read, 14-day
window kept), not 8 live pages; price / market cap for the liquidity gate come from the factor book's bank close
(labelled `price_source`), the live /stable/quote stays as a capped fallback; every cluster carries a labelled
`bank_context` (Altman Z, Piotroski, short % float, analyst upside, 21/63-day returns).  Filters, thresholds and the
scoring are unchanged.  The live rungs stand when the bank has nothing (`source_used`).

Schedule: daily 23:30 UTC after most insider filings settle.
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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

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
BANK_FEED_DAYS = int(os.environ.get("INSIDER_BANK_FEED_DAYS", "16"))      # 14-day window + filing lag
LIVE_QUOTE_CAP = int(os.environ.get("INSIDER_LIVE_QUOTE_CAP", "25"))      # live /stable/quote calls per run, at most
BANK_CONTEXT_FIELDS = ["altman_z", "piotroski", "short_pct_float", "days_to_cover", "pt_upside_pct", "ret_21d", "ret_63d", "from_52w_high_pct", "composite_quality"]
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/insider-sell-cluster.json"
SSM_STATE_KEY = "/justhodl/insider-sell-cluster/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

# Ranks of insider title weights
TITLE_WEIGHTS = {
    "CEO": 1.0, "CHIEF EXECUTIVE": 1.0,
    "CFO": 0.95, "CHIEF FINANCIAL": 0.95,
    "COO": 0.85, "PRESIDENT": 0.85,
    "DIRECTOR": 0.75, "BOARD": 0.75,
    "CHAIRMAN": 0.85,
    "VP": 0.6, "SVP": 0.65, "EVP": 0.7, "VICE PRESIDENT": 0.6,
    "OFFICER": 0.55,
    "10%": 0.7,
}


def http_get(url, timeout=15, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8", errors="ignore")
        except Exception as e:
            last = e
            time.sleep(0.6 * (i + 1))
    raise RuntimeError(f"http_get failed: {last}")


def _is_sale(row):
    """Strict sale test for the banked feed (which carries every transaction type): S-Sale rows only."""
    tt = (row.get("transactionType") or "").upper().strip()
    return tt.startswith("S-") or tt == "S" or tt == "SALE"


def fetch_bank_sells(days=None):
    """First rung (v1.1.0): the banked whole-market insider feed, filtered as the live path filters (14-day window,
    non-10b5-1, sells only) while it is read so only the window's sells are retained.  ([] , 0, 0) when not banked."""
    if _bank is None or not hasattr(_bank, "walker_days"):
        return [], 0, 0
    try:
        banked = _bank.walker_days("insider-trading-latest", days=days or BANK_FEED_DAYS)
    except Exception:  # noqa: BLE001
        banked = []
    cutoff = datetime.utcnow().date() - timedelta(days=14)
    out, n_rows = [], 0
    for _day, rows in banked:
        for row in rows:
            n_rows += 1
            if not isinstance(row, dict) or not _is_sale(row):
                continue
            ds = row.get("transactionDate") or row.get("filingDate")
            if not ds:
                continue
            try:
                if datetime.strptime(ds[:10], "%Y-%m-%d").date() < cutoff:
                    continue
            except Exception:
                continue
            notes = (row.get("planType") or row.get("acquisitionDisposition") or row.get("typeOfOwner") or "").upper()
            if "10B5-1" in notes or "PLAN" in notes:
                continue
            out.append(row)
    return out, len(banked), n_rows


def fetch_insider_sells(from_date, page_limit=8):
    """Pull last-14d insider transactions from FMP, filter to SELLS."""
    all_rows = []
    for page in range(page_limit):
        url = (f"https://financialmodelingprep.com/stable/insider-trading/latest"
               f"?transactionType=S-Sale&page={page}&apikey={FMP_KEY}")
        try:
            data = json.loads(http_get(url))
            if not isinstance(data, list) or len(data) == 0:
                break
            all_rows.extend(data)
            if len(data) < 50:
                break
        except Exception:
            break
    # Filter to recent + non-10b5-1
    cutoff = datetime.utcnow().date() - timedelta(days=14)
    filtered = []
    for row in all_rows:
        if not isinstance(row, dict):
            continue
        ds = row.get("transactionDate") or row.get("filingDate")
        if not ds:
            continue
        try:
            d = datetime.strptime(ds[:10], "%Y-%m-%d").date()
            if d < cutoff:
                continue
        except Exception:
            continue
        # Exclude 10b5-1 plan trades
        notes = (row.get("planType") or row.get("acquisitionDisposition") or
                 row.get("typeOfOwner") or "").upper()
        if "10B5-1" in notes or "PLAN" in notes:
            continue
        # Some FMP feeds use "S-Sale" transactionType; SELL types only
        tt = (row.get("transactionType") or "").upper()
        if tt and "S" not in tt and "SALE" not in tt:
            continue
        filtered.append(row)
    return filtered


def title_weight(title):
    if not title:
        return 0.5
    t = title.upper()
    for key, w in TITLE_WEIGHTS.items():
        if key in t:
            return w
    return 0.5


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
            }
    except Exception:
        pass
    return None


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
    return {"price": q.get("price"), "market_cap": q.get("marketCap"), "name": q.get("name"),
            "price_source": "factor-book (bank close, not a live tick)"}


def book_name(symbol):
    """Company name from the symbol's factor-book document (identity), for the few clusters that pass the gate."""
    if _book is None or not _book.available():
        return None
    try:
        d = _book.factor(symbol)
    except Exception:  # noqa: BLE001
        d = None
    return ((d or {}).get("identity") or {}).get("name") if d else None


def book_context(symbol):
    """Labelled bank facts the engine never had (every value may be None - an unbanked fact, never a guess)."""
    if _book is None or not _book.available():
        return None
    try:
        row = _book.lookup(symbol, BANK_CONTEXT_FIELDS)
    except Exception:  # noqa: BLE001
        row = None
    if not row:
        return None
    return {k: row.get(k) for k in BANK_CONTEXT_FIELDS} | {"source": "factor-book", "as_of": _book.as_of()}


def aggregate_clusters(sells):
    """Group sells by ticker, compute cluster stats."""
    by_ticker = defaultdict(list)
    for row in sells:
        tk = row.get("symbol") or row.get("ticker")
        if not tk:
            continue
        by_ticker[tk.upper()].append(row)
    clusters = []
    for tk, rows in by_ticker.items():
        # Distinct insiders
        names = set()
        for r in rows:
            n = r.get("reportingName") or r.get("insiderName") or r.get("name")
            if n:
                names.add(n.strip().upper())
        n_distinct = len(names)
        if n_distinct < 3:
            continue  # Not a cluster
        # Total $ value
        total_value = 0
        weighted_value = 0
        max_pct_sold = 0
        for r in rows:
            shares = r.get("securitiesTransacted") or r.get("shares") or 0
            price = r.get("price") or r.get("transactionPrice") or 0
            try:
                v = float(shares) * float(price)
            except Exception:
                v = 0
            total_value += v
            tw = title_weight(r.get("typeOfOwner") or r.get("officerTitle"))
            weighted_value += v * tw
            held_after = r.get("securitiesOwned") or r.get("sharesOwnedFollowingTransaction") or 0
            try:
                held_after_f = float(held_after)
                sold_f = float(shares)
                if held_after_f + sold_f > 0:
                    pct_sold = sold_f / (held_after_f + sold_f) * 100
                    if pct_sold > max_pct_sold:
                        max_pct_sold = pct_sold
            except Exception:
                pass
        if total_value < 2_000_000:
            continue  # Not material
        clusters.append({
            "ticker": tk,
            "n_distinct_sellers": n_distinct,
            "total_sale_value_usd": int(total_value),
            "weighted_sale_value_usd": int(weighted_value),
            "max_pct_holdings_sold": round(max_pct_sold, 1),
            "n_transactions": len(rows),
            "sellers": sorted(names)[:8],
        })
    return clusters


def score_cluster(cluster, quote):
    """0-1 composite score for cluster conviction."""
    n = cluster.get("n_distinct_sellers", 0)
    val = cluster.get("total_sale_value_usd", 0)
    wval = cluster.get("weighted_sale_value_usd", 0)
    pct = cluster.get("max_pct_holdings_sold", 0)
    s = 0.0
    if n >= 8:
        s += 0.35
    elif n >= 5:
        s += 0.25
    elif n >= 3:
        s += 0.15
    if val >= 20_000_000:
        s += 0.25
    elif val >= 10_000_000:
        s += 0.2
    elif val >= 5_000_000:
        s += 0.12
    else:
        s += 0.05
    # Weighted value boost (CEO/CFO weighting)
    if wval and val:
        weight_ratio = wval / val
        if weight_ratio >= 0.9:
            s += 0.2
        elif weight_ratio >= 0.7:
            s += 0.12
        elif weight_ratio >= 0.5:
            s += 0.05
    if pct >= 50:
        s += 0.2
    elif pct >= 25:
        s += 0.12
    elif pct >= 10:
        s += 0.05
    return min(1.0, s)


def lambda_handler(event, context):
    start = time.time()
    try:
        from_date = (datetime.utcnow() - timedelta(days=14)).strftime("%Y-%m-%d")
        sells, bank_days, bank_rows = fetch_bank_sells()
        if sells:
            source_used = "fmp-walker bank (%d days, %d feed rows)" % (bank_days, bank_rows)
        else:
            sells = fetch_insider_sells(from_date, page_limit=8)
            source_used = "fmp live /stable/insider-trading/latest" if sells else "unavailable"
        feed_basis = {"source": source_used, "bank_days": bank_days, "bank_feed_rows": bank_rows, "window_days": 14}
        clusters = aggregate_clusters(sells)
        # Pull market cap for liquidity gate ($500M+): the factor book first, live quotes capped
        tickers = [c["ticker"] for c in clusters]
        quotes = {t: book_quote(t) for t in tickers}
        missing = [t for t in tickers if quotes.get(t) is None][:LIVE_QUOTE_CAP]
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
        scored = []
        for c in clusters:
            q = quotes.get(c["ticker"])
            if not q or not q.get("market_cap") or q["market_cap"] < 500_000_000:
                continue
            score = score_cluster(c, q)
            severity = "HIGH" if score >= 0.65 else ("MEDIUM" if score >= 0.4 else "LOW")
            ticket = {
                "ticker": c["ticker"],
                "side": "DEFENSIVE",
                "action": (
                    "EXIT long position or hedge with ATM put"
                    if score >= 0.55 else
                    "TRIM long exposure 30-50%, set tighter stops"
                ),
                "rationale": (
                    f"{c['n_distinct_sellers']} distinct insiders sold "
                    f"${c['total_sale_value_usd']/1e6:.1f}M in 14d; "
                    f"max {c['max_pct_holdings_sold']}% of holdings; "
                    f"composite {score:.2f}"
                ),
                "holding_period": "Risk avoidance: 30-60 days",
                "expected_downside_pct": -8 if score >= 0.65 else -4,
                "size_pct_portfolio": "Exit/trim only; if shorting, 0.5-1% max",
            }
            scored.append({
                **c,
                "name": q.get("name") or book_name(c["ticker"]),
                "price": q.get("price"),
                "price_source": q.get("price_source"),
                "market_cap_usd": q.get("market_cap"),
                "composite_score": round(score, 3),
                "severity": severity,
                "trade_ticket": ticket,
                "bank_context": book_context(c["ticker"]),
            })
        scored.sort(key=lambda c: c["composite_score"], reverse=True)
        n_high = sum(1 for c in scored if c["composite_score"] >= 0.65)
        n_med = sum(1 for c in scored if 0.4 <= c["composite_score"] < 0.65)
        if n_high >= 8:
            state, strength = "RED_FLAG_RICH", 0.9
        elif n_high >= 3 or (n_high + n_med) >= 8:
            state, strength = "ACTIVE", 0.65
        elif n_high >= 1 or n_med >= 2:
            state, strength = "NORMAL", 0.35
        else:
            state, strength = "QUIET", 0.1

        out = {
            "engine": "insider-sell-cluster",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "n_clusters": len(scored),
            "n_high_severity": n_high,
            "n_medium_severity": n_med,
            "n_raw_sells_scanned": len(sells),
            "source_used": source_used,
            "feed_basis": feed_basis,
            "price_sources": price_sources,
            "book_as_of": (_book.as_of() if (_book is not None and _book.available()) else None),
            "top_clusters": scored[:15],
            "all_clusters": scored,
            "methodology": (
                "Defensive insider-sell cluster scanner. 5-factor filter: "
                "(1) EXCLUDES 10b5-1 plan transactions (routine, non-predictive); "
                "(2) >=3 distinct sellers in 14d; (3) >=$2M cumulative; "
                "(4) CEO/CFO/Director title weighting; (5) >=25% of holdings "
                "sold = high-conviction. Liquidity gate $500M mcap. "
                "Edge basis: Cohen-Malloy-Pomorski 2012, Lakonishok-Lee 2001, "
                "Jeng-Metrick-Zeckhauser 2003. Forward edge: opportunistic "
                "sells precede -3 to -8% / 60d. Treat as RISK AVOIDANCE — "
                "exit longs / hedge rather than aggressive short."
            ),
            "sources": ["fmp-walker bank insider-trading-latest (whole feed per day)", "factor book (bank close + context)",
                        "FMP /stable/insider-trading (fallback)", "FMP /stable/quote (capped fallback)"],
            "why_now": f"{n_high} HIGH + {n_med} MEDIUM defensive flags",
            "run_seconds": round(time.time() - start, 2),
        }

        # Telegram on state escalation only (defensive, don't spam)
        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and state in ("RED_FLAG_RICH", "ACTIVE") and TELEGRAM_TOKEN:
            top = scored[:5]
            top_str = "\n".join(
                f"- {c['ticker']} ({c['n_distinct_sellers']} sellers, "
                f"${c['total_sale_value_usd']/1e6:.1f}M, {c['severity']})"
                for c in top)
            msg = (f"*INSIDER-SELL-CLUSTER -> {state}*\n"
                   f"DEFENSIVE: {n_high} HIGH-severity clusters\n"
                   f"Top 5:\n{top_str}\n"
                   f"Action: review long positions in these names.")
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
        return {"statusCode": 200, "body": json.dumps({"ok": True, "state": state,
                                                         "n_clusters": len(scored)})}
    except Exception as e:
        import traceback
        err = {"engine": "insider-sell-cluster", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
