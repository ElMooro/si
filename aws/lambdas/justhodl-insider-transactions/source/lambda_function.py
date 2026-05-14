"""
justhodl-insider-transactions — SEC Form 4 Insider Buying/Selling Engine (BUILD 12/15)

WHY THIS EXISTS (PIVOT FROM USPTO)
==================================
USPTO patent data is slow-moving and lower signal-value than insider
transactions. SEC Form 4 (executive officers' open-market trades) is the
single most predictive single-stock signal in academic literature:
  • Lakonishok & Lee (2001): Insider buys outperform market by ~11% / year
  • Cohen, Malloy & Pomorski (2012): Insider sales DON'T predict (often
    diversification or pre-set 10b5-1 plans), but CLUSTER BUYS do

Bloomberg's TICKR<EQUITY> page charges thousands for this. SEC EDGAR
provides it free with real-time updates via the full-text search index.

DATA SOURCE
===========
SEC EDGAR Submissions API:
  data.sec.gov/submissions/CIK{010d}.json
    Returns all recent filings with form types, accession, date

SEC EDGAR Form 4 XML:
  www.sec.gov/Archives/edgar/data/{cik}/{nodash_accession}/{primary}.xml
    Contains: reportingOwner, transactionCode, shares, price, ownership

UNIVERSE
========
Top 30 S&P 500 names by market cap.

PER-TICKER METRICS
==================
N Form 4 filings in last 7d, 14d, 30d, 90d
Z-score of 7d activity vs 90d baseline
For each recent filing:
  insider name + role (CEO, CFO, Director, 10% Owner)
  transaction code (P=buy, S=sell, A=grant, etc.)
  shares × price = $ value
Composite:
  buy_count, sell_count, buy_value_usd, sell_value_usd
  buy_sell_ratio (count), buy_sell_dollar_ratio
  cluster_flag: 3+ distinct insiders buying in 7d (strongest signal)

COMPOSITE REGIME
================
n_cluster_buys >= 3   INSIDER_BUYING_BROAD (rare; bullish signal)
n_cluster_buys >= 1   INSIDER_BUYING_PRESENT
mostly_sells          INSIDER_SELLING_REGIME (cautious, often noise)
balanced              INSIDER_NORMAL

SCHEDULE
========
cron(0 1 ? * MON-SAT *) — daily at 01:00 UTC (after EDGAR end-of-day batch)
"""
import io, json, os, re, time, urllib.request, urllib.error
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.1.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/insider-transactions.json"

TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 25
MAX_PARALLEL = 5  # SEC has 10 req/sec limit per IP — stay safe
LOOKBACK_DAYS = 90

# Ticker → CIK mapping (top 30 by market cap, hard-coded to avoid extra fetch)
UNIVERSE = [
    {"ticker": "AAPL",  "cik": "0000320193", "name": "Apple"},
    {"ticker": "MSFT",  "cik": "0000789019", "name": "Microsoft"},
    {"ticker": "GOOGL", "cik": "0001652044", "name": "Alphabet"},
    {"ticker": "AMZN",  "cik": "0001018724", "name": "Amazon"},
    {"ticker": "NVDA",  "cik": "0001045810", "name": "Nvidia"},
    {"ticker": "META",  "cik": "0001326801", "name": "Meta"},
    {"ticker": "TSLA",  "cik": "0001318605", "name": "Tesla"},
    {"ticker": "AVGO",  "cik": "0001730168", "name": "Broadcom"},
    {"ticker": "JPM",   "cik": "0000019617", "name": "JPMorgan Chase"},
    {"ticker": "WMT",   "cik": "0000104169", "name": "Walmart"},
    {"ticker": "LLY",   "cik": "0000059478", "name": "Eli Lilly"},
    {"ticker": "V",     "cik": "0001403161", "name": "Visa"},
    {"ticker": "UNH",   "cik": "0000731766", "name": "UnitedHealth"},
    {"ticker": "XOM",   "cik": "0000034088", "name": "ExxonMobil"},
    {"ticker": "MA",    "cik": "0001141391", "name": "Mastercard"},
    {"ticker": "PG",    "cik": "0000080424", "name": "P&G"},
    {"ticker": "JNJ",   "cik": "0000200406", "name": "Johnson & Johnson"},
    {"ticker": "HD",    "cik": "0000354950", "name": "Home Depot"},
    {"ticker": "COST",  "cik": "0000909832", "name": "Costco"},
    {"ticker": "ABBV",  "cik": "0001551152", "name": "AbbVie"},
    {"ticker": "BAC",   "cik": "0000070858", "name": "Bank of America"},
    {"ticker": "KO",    "cik": "0000021344", "name": "Coca-Cola"},
    {"ticker": "CVX",   "cik": "0000093410", "name": "Chevron"},
    {"ticker": "ORCL",  "cik": "0001341439", "name": "Oracle"},
    {"ticker": "MRK",   "cik": "0000310158", "name": "Merck"},
    {"ticker": "PEP",   "cik": "0000077476", "name": "PepsiCo"},
    {"ticker": "ADBE",  "cik": "0000796343", "name": "Adobe"},
    {"ticker": "CSCO",  "cik": "0000858877", "name": "Cisco"},
    {"ticker": "TMO",   "cik": "0000097745", "name": "Thermo Fisher"},
    {"ticker": "AMD",   "cik": "0000002488", "name": "AMD"},
]

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# HTTP / EDGAR
# ═══════════════════════════════════════════════════════════════════════════

def http_get(url, timeout=HTTP_TIMEOUT, parse_json=True):
    req = urllib.request.Request(url, headers={
        "User-Agent": "JustHodl.AI Research support@justhodl.ai",
        "Accept": "application/json, text/xml, application/xml, */*",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read().decode("utf-8", "replace")
    if parse_json:
        return json.loads(body)
    return body


def fetch_submissions(cik):
    """Returns SEC submissions JSON or None."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    try:
        return http_get(url)
    except Exception as e:
        print(f"  CIK {cik} submissions err: {str(e)[:80]}")
        return None


def fetch_form4_xml(cik, accession_no_dashes):
    """Fetch raw Form 4 XML. accession should be without dashes (e.g. 000114036126020871)."""
    # Look for primary doc URL — try standard pattern
    base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_no_dashes}"
    # Find the XML file via index
    try:
        idx_url = f"{base}/{accession_no_dashes[:18]}-index.html"
        # Easier: try common names
        for fn in ("form4.xml", "primary_doc.xml", "doc4.xml"):
            try:
                return http_get(f"{base}/{fn}", parse_json=False)
            except Exception: continue
        return None
    except Exception as e:
        return None


# ═══════════════════════════════════════════════════════════════════════════
# FORM 4 XML PARSING
# ═══════════════════════════════════════════════════════════════════════════

# Transaction codes per SEC Form 4 spec (most common)
TXN_CODE_MEANING = {
    "P": "BUY",                    # Open market or private purchase
    "S": "SELL",                   # Open market or private sale
    "A": "GRANT",                  # Grant, award (incl. ESPP)
    "F": "TAX_WITHHOLD",           # Payment of exercise price/tax via shares
    "D": "DISPOSE",                # Disposition (other)
    "M": "EXERCISE",               # Exercise/conversion of derivative
    "G": "GIFT",
    "C": "CONVERSION",
    "X": "EXERCISE_DERIVATIVE",
    "J": "OTHER_ACQUISITION",
    "K": "OTHER_DISPOSITION",
    "U": "TENDER",
    "V": "VOLUNTARY",
    "I": "DISCRETIONARY",
    "Z": "VOTING_TRUST",
    "W": "ACQUIRED_WILL",
}


def parse_form4_minimal(xml_text):
    """Extract reporter, txn code, shares, price from Form 4 XML.
    Returns list of transaction dicts (a single filing may have multiple)."""
    if not xml_text: return []
    txns = []

    # Extract reporter (insider) info
    reporter = re.search(r'<rptOwnerName>([^<]+)</rptOwnerName>', xml_text)
    insider_name = reporter.group(1).strip() if reporter else None

    role_dir = bool(re.search(r'<isDirector>(?:1|true)</isDirector>', xml_text))
    role_off = bool(re.search(r'<isOfficer>(?:1|true)</isOfficer>', xml_text))
    role_ten = bool(re.search(r'<isTenPercentOwner>(?:1|true)</isTenPercentOwner>', xml_text))
    title = re.search(r'<officerTitle>([^<]+)</officerTitle>', xml_text)
    role_title = title.group(1).strip() if title else None
    role_flags = []
    if role_ten: role_flags.append("10%_OWNER")
    if role_off: role_flags.append("OFFICER")
    if role_dir: role_flags.append("DIRECTOR")

    # Non-derivative transactions (most common — open market buys/sells)
    # Match transactionCode + shares + price within each transaction block
    txn_pattern = re.compile(
        r'<nonDerivativeTransaction>(.*?)</nonDerivativeTransaction>',
        re.DOTALL,
    )
    for m in txn_pattern.finditer(xml_text):
        block = m.group(1)
        code_m = re.search(r'<transactionCode>([^<]+)</transactionCode>', block)
        shares_m = re.search(r'<transactionShares>\s*<value>([\d.]+)</value>', block)
        price_m = re.search(r'<transactionPricePerShare>\s*<value>([\d.]+)</value>', block)
        date_m = re.search(r'<transactionDate>\s*<value>([\d\-]+)</value>', block)
        post_m = re.search(r'<sharesOwnedFollowingTransaction>\s*<value>([\d.]+)</value>', block)

        if not (code_m and shares_m): continue
        code = code_m.group(1).strip()
        shares = float(shares_m.group(1))
        price = float(price_m.group(1)) if price_m else None
        dollar = round(shares * price, 2) if price else None
        txns.append({
            "insider": insider_name,
            "role_title": role_title,
            "role_flags": role_flags,
            "code": code,
            "code_meaning": TXN_CODE_MEANING.get(code, code),
            "shares": shares,
            "price": price,
            "dollar_value": dollar,
            "date": date_m.group(1).strip() if date_m else None,
            "shares_after": float(post_m.group(1)) if post_m else None,
        })
    return txns


# ═══════════════════════════════════════════════════════════════════════════
# PER-TICKER ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_ticker(meta):
    ticker = meta["ticker"]
    cik = meta["cik"]
    result = {"ticker": ticker, "cik": cik, "name": meta["name"]}

    sub = fetch_submissions(cik)
    if not sub:
        result["err"] = "no submissions data"
        return result

    recent = (sub.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    dates = recent.get("filingDate") or []
    accessions = recent.get("accessionNumber") or []
    primary_docs = recent.get("primaryDocument") or []

    if not forms:
        result["err"] = "no filings"
        return result

    today = datetime.now(timezone.utc).date()
    cutoff_7d = today - timedelta(days=7)
    cutoff_14d = today - timedelta(days=14)
    cutoff_30d = today - timedelta(days=30)
    cutoff_90d = today - timedelta(days=LOOKBACK_DAYS)

    form4_indices = [i for i, f in enumerate(forms) if f == "4"]
    n_7d = n_14d = n_30d = n_90d = 0
    recent_filings_meta = []

    for i in form4_indices:
        try:
            d = datetime.strptime(dates[i], "%Y-%m-%d").date()
            if d < cutoff_90d: continue
            n_90d += 1
            if d >= cutoff_30d: n_30d += 1
            if d >= cutoff_14d: n_14d += 1
            if d >= cutoff_7d: n_7d += 1
            if d >= cutoff_30d:
                recent_filings_meta.append({
                    "date": dates[i],
                    "accession": accessions[i],
                    "primary": primary_docs[i] if i < len(primary_docs) else None,
                })
        except Exception: pass

    # Fetch + parse last 5 form 4 XMLs (don't overload)
    # CRITICAL: The submissions API gives us paths like "xslF345X06/form4.xml" which
    # is the HTML-rendered (XSL stylesheet) version of the filing. We need the RAW
    # XML which lives at the same accession folder but WITHOUT the xslF345X06/ prefix.
    # Always strip that prefix; also try common filename fallbacks.
    parsed_txns = []
    for fm in recent_filings_meta[:10]:
        try:
            acc = fm["accession"].replace("-", "")
            raw_primary = fm.get("primary") or "form4.xml"
            # Strip ANY xslF345X*/ prefix (xslF345X02 through X06 exist across years)
            primary = re.sub(r'^xslF345X\d+/', '', raw_primary)
            xml = None
            # Try the stripped primary path first
            candidates = [primary]
            # Fallback: bare form4.xml (the canonical name)
            if "form4.xml" not in candidates: candidates.append("form4.xml")
            if "ownership.xml" not in candidates: candidates.append("ownership.xml")
            base = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{acc}"
            for cand in candidates:
                try:
                    xml = http_get(f"{base}/{cand}", parse_json=False)
                    # quick sanity: real XML starts with <?xml or <ownership
                    if xml and ("<ownershipDocument" in xml[:1000] or "<?xml" in xml[:200]):
                        break
                    xml = None
                except Exception: continue
            if not xml: continue
            txns = parse_form4_minimal(xml)
            for t in txns:
                t["filing_date"] = fm["date"]
                t["accession"] = fm["accession"]
            parsed_txns.extend(txns)
        except Exception as e:
            pass

    # Aggregate buy/sell from parsed txns (last 30d)
    buys = [t for t in parsed_txns if t.get("code") == "P"]
    sells = [t for t in parsed_txns if t.get("code") == "S"]
    grants = [t for t in parsed_txns if t.get("code") == "A"]
    exercises = [t for t in parsed_txns if t.get("code") == "M"]
    withholdings = [t for t in parsed_txns if t.get("code") == "F"]
    distinct_buyers = set(t.get("insider") for t in buys if t.get("insider"))
    distinct_sellers = set(t.get("insider") for t in sells if t.get("insider"))

    buy_value = sum(t.get("dollar_value") or 0 for t in buys)
    sell_value = sum(t.get("dollar_value") or 0 for t in sells)
    grant_value = sum(t.get("dollar_value") or 0 for t in grants)

    # Code-distribution summary for transparency
    code_dist = {}
    for t in parsed_txns:
        c = t.get("code") or "?"
        code_dist[c] = code_dist.get(c, 0) + 1

    cluster_buy_flag = len(distinct_buyers) >= 3  # 3+ unique buyers in 30d
    cluster_sell_flag = len(distinct_sellers) >= 5

    result.update({
        "n_form4_7d": n_7d,
        "n_form4_14d": n_14d,
        "n_form4_30d": n_30d,
        "n_form4_90d": n_90d,
        "n_buys_30d": len(buys),
        "n_sells_30d": len(sells),
        "n_grants_30d": len(grants),
        "n_exercises_30d": len(exercises),
        "n_withholdings_30d": len(withholdings),
        "n_distinct_buyers_30d": len(distinct_buyers),
        "n_distinct_sellers_30d": len(distinct_sellers),
        "buy_value_30d_usd": round(buy_value, 0),
        "sell_value_30d_usd": round(sell_value, 0),
        "grant_value_30d_usd": round(grant_value, 0),
        "buy_sell_dollar_ratio": round(buy_value / max(sell_value, 1), 2),
        "code_distribution_30d": code_dist,
        "cluster_buy_flag": cluster_buy_flag,
        "cluster_sell_flag": cluster_sell_flag,
        "recent_txns_top_5": [
            {"date": t.get("filing_date"), "insider": t.get("insider"),
              "role": t.get("role_title") or " ".join(t.get("role_flags", [])),
              "code": t.get("code"), "code_meaning": t.get("code_meaning"),
              "shares": t.get("shares"), "price": t.get("price"),
              "dollar_value": t.get("dollar_value")}
            for t in sorted(parsed_txns,
                              key=lambda x: x.get("filing_date") or "",
                              reverse=True)[:5]
        ],
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

def classify_regime(results):
    valid = [r for r in results if not r.get("err")]
    if not valid:
        return "UNKNOWN", "No data loaded"

    n_cluster_buy = sum(1 for r in valid if r.get("cluster_buy_flag"))
    n_cluster_sell = sum(1 for r in valid if r.get("cluster_sell_flag"))
    total_buy_usd = sum(r.get("buy_value_30d_usd") or 0 for r in valid)
    total_sell_usd = sum(r.get("sell_value_30d_usd") or 0 for r in valid)
    n_with_buys = sum(1 for r in valid if (r.get("n_buys_30d") or 0) > 0)

    if n_cluster_buy >= 3:
        return "INSIDER_BUYING_BROAD", (
            f"{n_cluster_buy} tickers w/ 3+ distinct insider buyers in 30d — rare bullish signal")
    if n_cluster_buy >= 1:
        return "INSIDER_BUYING_PRESENT", (
            f"{n_cluster_buy} ticker(s) with cluster buying · "
            f"{n_with_buys} names with any insider buys · "
            f"${total_buy_usd/1e6:.1f}M buy vs ${total_sell_usd/1e6:.1f}M sell 30d")
    if total_sell_usd > total_buy_usd * 10 and total_sell_usd > 50e6:
        return "INSIDER_SELLING_REGIME", (
            f"Sells >10x buys (${total_sell_usd/1e6:.0f}M vs ${total_buy_usd/1e6:.0f}M); "
            "often diversification, but worth monitoring")
    return "INSIDER_NORMAL", (
        f"Balanced activity · ${total_buy_usd/1e6:.1f}M buys vs ${total_sell_usd/1e6:.1f}M sells 30d")


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
    print(f"=== insider-transactions v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    print(f"  universe: {len(UNIVERSE)} tickers")

    try:
        prior_payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = prior_payload.get("composite_regime")
    except Exception:
        prior_regime = None

    # Parallel fetch (low concurrency — SEC rate limit)
    results = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futures = {ex.submit(analyze_ticker, m): m["ticker"] for m in UNIVERSE}
        for f in as_completed(futures):
            r = f.result()
            if r.get("err"):
                print(f"  ✗ {r['ticker']}: {r['err']}")
            else:
                cluster = " CLUSTER_BUY" if r.get("cluster_buy_flag") else ""
                print(f"  ✓ {r['ticker']:6s} 7d:{r.get('n_form4_7d'):>2} 14d:{r.get('n_form4_14d'):>2} 30d:{r.get('n_form4_30d'):>2} "
                      f"buys30d:{r.get('n_buys_30d')} sells30d:{r.get('n_sells_30d')}{cluster}")
            results.append(r)

    valid = [r for r in results if not r.get("err")]
    n_with_data = len(valid)

    # Rankings
    by_buy_value = sorted(valid, key=lambda x: -(x.get("buy_value_30d_usd") or 0))
    by_sell_value = sorted(valid, key=lambda x: -(x.get("sell_value_30d_usd") or 0))
    by_n_buys = sorted(valid, key=lambda x: -(x.get("n_buys_30d") or 0))
    cluster_buys = [r for r in valid if r.get("cluster_buy_flag")]
    most_active = sorted(valid, key=lambda x: -(x.get("n_form4_14d") or 0))

    ranked = {
        "biggest_buy_dollars_30d": [
            {"ticker": r["ticker"], "buy_value_usd": r.get("buy_value_30d_usd"),
              "n_buys_30d": r.get("n_buys_30d"),
              "n_distinct_buyers": r.get("n_distinct_buyers_30d"),
              "cluster_buy": r.get("cluster_buy_flag")}
            for r in by_buy_value[:5] if (r.get("buy_value_30d_usd") or 0) > 0
        ],
        "biggest_sell_dollars_30d": [
            {"ticker": r["ticker"], "sell_value_usd": r.get("sell_value_30d_usd"),
              "n_sells_30d": r.get("n_sells_30d"),
              "n_distinct_sellers": r.get("n_distinct_sellers_30d")}
            for r in by_sell_value[:5] if (r.get("sell_value_30d_usd") or 0) > 0
        ],
        "most_n_buys_30d": [
            {"ticker": r["ticker"], "n_buys_30d": r.get("n_buys_30d"),
              "buy_value_usd": r.get("buy_value_30d_usd"),
              "cluster_buy": r.get("cluster_buy_flag")}
            for r in by_n_buys[:5] if (r.get("n_buys_30d") or 0) > 0
        ],
        "cluster_buys": [
            {"ticker": r["ticker"], "n_distinct_buyers": r.get("n_distinct_buyers_30d"),
              "buy_value_usd": r.get("buy_value_30d_usd")}
            for r in cluster_buys
        ],
        "most_active_14d": [
            {"ticker": r["ticker"], "n_form4_14d": r.get("n_form4_14d"),
              "n_form4_7d": r.get("n_form4_7d")}
            for r in most_active[:5] if (r.get("n_form4_14d") or 0) > 0
        ],
    }

    regime, signal = classify_regime(results)
    regime_changed = (prior_regime != regime) if prior_regime else False

    by_ticker = {r["ticker"]: {k: v for k, v in r.items() if k != "cik"}
                  for r in results}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "SEC EDGAR submissions API + Form 4 XML",
        "elapsed_seconds": round(time.time() - started, 1),
        "universe": [u["ticker"] for u in UNIVERSE],
        "n_tickers": len(UNIVERSE),
        "n_with_data": n_with_data,
        "n_with_err": len(results) - n_with_data,
        "n_cluster_buys": len(cluster_buys),
        "total_buy_value_30d_usd": sum(r.get("buy_value_30d_usd") or 0 for r in valid),
        "total_sell_value_30d_usd": sum(r.get("sell_value_30d_usd") or 0 for r in valid),
        "by_ticker": by_ticker,
        "ranked": ranked,
        "composite_regime": regime,
        "composite_signal": signal,
        "regime_changed_from_prior": regime_changed,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ insider-transactions.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Telegram on regime change or cluster buy detection
    alert_sent = False
    if regime_changed or cluster_buys:
        lines = [f"💼 *Insider Form 4 · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                  f"⚡ {regime}",
                  f"_{signal[:140]}_\n",
                  f"📊 30d: ${payload['total_buy_value_30d_usd']/1e6:.1f}M buys vs "
                  f"${payload['total_sell_value_30d_usd']/1e6:.1f}M sells"]
        if cluster_buys:
            lines.append("\n🚨 Cluster buying:")
            for cb in cluster_buys[:5]:
                lines.append(f"  • {cb['ticker']}: {cb.get('n_distinct_buyers_30d')} distinct insiders, "
                              f"${(cb.get('buy_value_30d_usd') or 0)/1e6:.1f}M")
        if prior_regime and prior_regime != regime:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_tickers": len(UNIVERSE), "n_with_data": n_with_data,
        "n_cluster_buys": len(cluster_buys),
        "total_buy_30d_usd": payload["total_buy_value_30d_usd"],
        "total_sell_30d_usd": payload["total_sell_value_30d_usd"],
        "regime": regime, "regime_changed": regime_changed,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
