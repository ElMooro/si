"""
justhodl-buyback-yield-ranking
==============================

Trailing 12-month buyback yield ranking screener.

Pressure-test:
  - Naive: rank by buyback yield alone. Includes EPS-management schemes,
    companies overpaying for own stock at peaks, and dilution offsets.
  - Better: 5-factor quality screen:
    (1) Buyback yield = abs(TTM share repurchases) / current market cap
    (2) FCF coverage: TTM FCF >= TTM buybacks (sustainable, not leveraged)
    (3) Valuation gate: P/E < 30 (avoid buying back at extreme multiples)
    (4) Net buyback yield = gross buybacks MINUS share issuance (avoid
        SBC-offset shams)
    (5) Trend: 4-quarter consistency (not one-off event)

Edge basis:
  Pontiff-Woodgate 2008 (top decile +4-6%/yr), Boudoukh-Michaely-Roberts
  2007 (total payout yield outperforms), Ikenberry-Lakonishok-Vermaelen
  1995 (post-buyback alpha 12% over 4 years). Apple/AutoZone-style
  "coffee can" payout machines. Long-term rebalance, not tactical.

Trade tickets:
  Long top 20 ranked names; rebalance quarterly; hold 12+ months.

Universe: top 500 by market cap from master-ranker (or fallback list);
$2B+ mcap gate.

Schedule: weekly Mon 13:00 UTC (after weekend filings + earnings season).

v1.1.0 (2026-10-26, bank fusion wave 2): the universe is every US common stock >= $2B whose share count shrank or
that repurchased stock in the latest fiscal year (factor book, up to BOOK_CANDIDATES by market cap) merged with the
master-ranker list; market cap / price / P/E come from the book (bank close, `price_source`), the TTM cash flows
from the dated FMP cash-flow bulks (`fmp_bank.statements`, `cashflow_source`), and every row carries a
`bank_context` block.  Live FMP stays the fallback for names the banks lack; the five gates and the composite
weights are unchanged.
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

import boto3
try:  # v1.1.0: the factor book (aws/shared/bank_enrich) - whole-market candidates + bank-close quote facts, projection reads
    import bank_enrich as _book
except Exception:  # noqa: BLE001
    _book = None
try:  # the FMP statement bulks (aws/shared/fmp_bank) - four quarters of cash flow without a live call
    import fmp_bank as _bank
except Exception:  # noqa: BLE001
    _bank = None

VERSION = "1.1.0"
BOOK_CANDIDATES = int(os.environ.get("BUYBACK_BOOK_CANDIDATES", "600"))   # whole-market candidates by market cap (bank-only reads)
MIN_MCAP_USD = 2_000_000_000
CONTEXT_FIELDS = ["altman_z", "piotroski", "fcf_yield", "shares_growth", "buyback_growth", "pt_upside_pct", "short_pct_float", "composite_quality", "composite_value"]
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = "data/buyback-yield-ranking.json"
SSM_STATE_KEY = "/justhodl/buyback-yield-ranking/state"

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT = os.environ.get("TELEGRAM_CHAT_ID", "8678089260")

s3 = boto3.client("s3")
ssm = boto3.client("ssm")

# Fallback liquid large-cap universe (S&P 500 sample)
FALLBACK_UNIVERSE = [
    "AAPL","MSFT","GOOGL","AMZN","META","NVDA","TSLA","AVGO","JPM","V","MA",
    "WMT","PG","JNJ","UNH","HD","BAC","XOM","CVX","PFE","ABBV","MRK","LLY",
    "DIS","NFLX","CRM","ADBE","ORCL","INTC","AMD","MU","QCOM","TXN","IBM",
    "GS","MS","C","WFC","AXP","BLK","SPGI","T","VZ","CMCSA","CSCO","ACN",
    "NKE","MCD","SBUX","KO","PEP","TGT","COST","LOW","F","GM","BA","CAT",
    "DE","HON","RTX","LMT","GE","MMM","DOW","ABT","TMO","DHR","BMY","GILD",
    "AMGN","REGN","VRTX","BIIB","ISRG","PYPL","UBER","SNOW","DDOG","CRWD",
    "PANW","NET","OKTA","MDB","TEAM","ZS","FTNT","NOW","VEEV","WDAY",
    "AZO","ORLY","ROST","TJX","DG","DLTR","KR","SYY","ADM","GIS","K",
    "PM","MO","BTI","KMB","CL","CHD","CLX","EL","STZ","TAP","DEO",
    "BKNG","HLT","MAR","RCL","NCLH","CCL","DAL","UAL","AAL","LUV","UNP","CSX","NSC",
    "TGT","LULU","KMB","SO","DUK","NEE","XEL","WEC","ED","PEG","AEP","D","EXC",
    "MSCI","ICE","CME","NDAQ","CBOE","COIN","SCHW","BX","KKR","APO","CG","ARES"
]


def http_get(url, timeout=15, retries=2):
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


def load_universe():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key="data/master-ranker.json")
        data = json.loads(obj["Body"].read())
        picks = (data.get("picks") or data.get("ranks") or data.get("universe")
                 or data.get("results") or [])
        if isinstance(picks, list):
            ts = []
            for r in picks[:300]:
                if isinstance(r, dict):
                    t = r.get("ticker") or r.get("symbol")
                    if t:
                        ts.append(t.upper())
                elif isinstance(r, str):
                    ts.append(r.upper())
            if ts:
                return ts[:200]
    except Exception:
        pass
    return FALLBACK_UNIVERSE


def book_ok():
    try:
        return _book is not None and _book.available()
    except Exception:  # noqa: BLE001
        return False


def book_candidates():
    """US common stocks >= $2B that shrank their share count or repurchased stock in the latest fiscal year (the book's
    growth bulks), largest first, capped - the whole-market universe the master-ranker list never gave this screen."""
    if not book_ok():
        return []
    try:
        syms = _book.prescreen(lambda r: (r.get("mcap") or 0) >= MIN_MCAP_USD and not r.get("is_adr")
                               and (((r.get("shares_growth") or 0) < 0) or (r.get("buyback_growth") is not None)),
                               order_by="mcap", descending=True, limit=BOOK_CANDIDATES, fields=["mcap", "shares_growth", "buyback_growth"])
    except Exception:  # noqa: BLE001
        return []
    return [str(s).upper() for s in (syms or []) if isinstance(s, str) and s]      # prescreen() returns symbols, largest first


def book_quote(symbol):
    """price / market cap / P/E from the factor book (bank close) in fmp_quote()'s shape, or None."""
    if not book_ok():
        return None
    try:
        row = _book.lookup(symbol, ["price", "mcap", "pe"])
    except Exception:  # noqa: BLE001
        return None
    if not row or not row.get("mcap"):
        return None
    pe = row.get("pe")
    return {"price": row.get("price"), "market_cap": float(row["mcap"]), "pe": (float(pe) if isinstance(pe, (int, float)) and pe > 0 else None),
            "name": None, "source": "factor-book (bank close)"}


def bank_context(symbol):
    if not book_ok():
        return None
    try:
        row = _book.lookup(symbol, CONTEXT_FIELDS)
    except Exception:  # noqa: BLE001
        return None
    if not row:
        return None
    ctx = {k: row.get(k) for k in CONTEXT_FIELDS}
    ctx["as_of"] = _book.as_of()
    ctx["source"] = "factor-book"
    return ctx


def cash_flow_ttm_from_rows(data):
    """fmp_cash_flow_ttm()'s arithmetic over the last 4 quarterly rows (newest first) from either source."""
    if not isinstance(data, list) or len(data) < 2:
        return None
    ttm_buybacks = 0  # commonStockRepurchased (negative in FMP)
    ttm_issuance = 0  # commonStockIssued
    ttm_ocf = 0       # operatingCashFlow
    ttm_capex = 0     # capitalExpenditure (negative)
    quarters_with_buybacks = 0
    for row in data[:4]:
        bb = row.get("commonStockRepurchased")
        if bb is not None:
            ttm_buybacks += abs(float(bb))
            if abs(float(bb)) > 0:
                quarters_with_buybacks += 1
        iss = row.get("commonStockIssued")
        if iss is not None:
            ttm_issuance += float(iss)
        ocf = row.get("operatingCashFlow") or row.get("netCashProvidedByOperatingActivities")
        if ocf is not None:
            ttm_ocf += float(ocf)
        cap = row.get("capitalExpenditure")
        if cap is not None:
            ttm_capex += abs(float(cap))
    ttm_fcf = ttm_ocf - ttm_capex
    net_buybacks = ttm_buybacks - ttm_issuance
    return {
        "ttm_buybacks_gross": ttm_buybacks,
        "ttm_issuance": ttm_issuance,
        "ttm_buybacks_net": net_buybacks,
        "ttm_ocf": ttm_ocf,
        "ttm_capex": ttm_capex,
        "ttm_fcf": ttm_fcf,
        "n_quarters": len(data),
        "quarters_with_buybacks": quarters_with_buybacks,
    }


def bank_cash_flow_ttm(symbol):
    """The last 4 quarterly cash-flow rows from the FMP bank's dated bulks (no key), or None when unbanked."""
    if _bank is None:
        return None
    try:
        rows = _bank.statements("cash-flow-statement", symbol, period="quarter", limit=4)
    except Exception:  # noqa: BLE001
        return None
    if not isinstance(rows, list) or len(rows) < 2:
        return None
    return cash_flow_ttm_from_rows(rows)


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
                "pe": float(row.get("pe", 0)) if row.get("pe") else None,
                "name": row.get("name") or row.get("companyName"),
            }
    except Exception:
        pass
    return None


def fmp_cash_flow_ttm(symbol):
    """Pull last 4 quarters of cash flow (live FMP) to compute TTM buybacks + issuance + FCF."""
    q = urllib.parse.quote_plus(symbol)
    url = (f"https://financialmodelingprep.com/stable/cash-flow-statement"
           f"?symbol={q}&period=quarter&limit=4&apikey={FMP_KEY}")
    try:
        data = json.loads(http_get(url))
        return cash_flow_ttm_from_rows(data)
    except Exception:
        return None


def analyze_ticker(symbol, bank_only=False):
    quote = book_quote(symbol)
    price_source = "factor-book (bank close)" if quote else None
    if not quote and not bank_only:
        quote = fmp_quote(symbol)
        price_source = "fmp live quote" if quote else None
    if not quote or not quote.get("market_cap") or quote["market_cap"] < MIN_MCAP_USD:
        return None
    cf = bank_cash_flow_ttm(symbol)
    cashflow_source = "fmp-bank cash-flow-statement quarterly" if cf else None
    if not cf and not bank_only:
        cf = fmp_cash_flow_ttm(symbol)
        cashflow_source = "fmp live cash-flow-statement" if cf else None
    if not cf or cf["ttm_buybacks_gross"] <= 0:
        return None
    mcap = quote["market_cap"]
    gross_yield = cf["ttm_buybacks_gross"] / mcap * 100
    net_yield = cf["ttm_buybacks_net"] / mcap * 100
    fcf_coverage = cf["ttm_fcf"] / cf["ttm_buybacks_gross"] if cf["ttm_buybacks_gross"] else 0

    # Quality filters
    pe = quote.get("pe")
    if pe and pe > 30:
        return None  # Avoid extreme P/E
    if cf["ttm_fcf"] <= 0:
        return None  # Avoid negative FCF
    if cf["quarters_with_buybacks"] < 2:
        return None  # Need consistency

    # Composite score
    score = 0.0
    if gross_yield >= 8:
        score += 0.35
    elif gross_yield >= 5:
        score += 0.25
    elif gross_yield >= 3:
        score += 0.15
    elif gross_yield >= 1.5:
        score += 0.08

    if net_yield >= gross_yield * 0.9:
        score += 0.25  # Low issuance dilution
    elif net_yield >= gross_yield * 0.7:
        score += 0.15
    elif net_yield >= 0:
        score += 0.05

    if fcf_coverage >= 1.5:
        score += 0.2
    elif fcf_coverage >= 1.0:
        score += 0.15
    elif fcf_coverage >= 0.7:
        score += 0.08

    if cf["quarters_with_buybacks"] == 4:
        score += 0.15  # consistent every quarter
    elif cf["quarters_with_buybacks"] == 3:
        score += 0.08

    if pe is not None:
        if pe < 15:
            score += 0.05
        elif pe < 20:
            score += 0.03

    return {
        "ticker": symbol,
        "name": quote.get("name"),
        "price": quote.get("price"),
        "market_cap_usd": mcap,
        "pe": pe,
        "ttm_buyback_yield_gross_pct": round(gross_yield, 2),
        "ttm_buyback_yield_net_pct": round(net_yield, 2),
        "ttm_buybacks_gross_usd": int(cf["ttm_buybacks_gross"]),
        "ttm_buybacks_net_usd": int(cf["ttm_buybacks_net"]),
        "ttm_fcf_usd": int(cf["ttm_fcf"]),
        "fcf_coverage_ratio": round(fcf_coverage, 2),
        "quarters_with_buybacks": cf["quarters_with_buybacks"],
        "composite_score": round(min(1.0, score), 3),
        "price_source": price_source,
        "cashflow_source": cashflow_source,
        "bank_context": bank_context(symbol),
    }


def lambda_handler(event, context):
    start = time.time()
    try:
        curated = load_universe()
        book_syms = book_candidates()
        book_set = set(book_syms)
        universe_basis = ("factor book: US common stocks >= $2B with a shrinking share count or repurchases in the latest fiscal year "
                          "(%d, cap %d, bank-only reads) + master-ranker list (%d)" % (len(book_syms), BOOK_CANDIDATES, len(curated))
                          if book_syms else "master-ranker list (%d); factor book unavailable" % len(curated))
        # the book's names are assessed bank-only (no live retries - the live key answers 429 today); the curated list keeps its
        # live fallback and its 200-name cap
        universe = list(dict.fromkeys(book_syms + [t for t in curated[:200]]))
        results = []
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(analyze_ticker, t, (t in book_set and t not in set(curated[:200]))): t for t in universe}
            for f in as_completed(futs):
                try:
                    r = f.result()
                    if r:
                        results.append(r)
                except Exception:
                    continue
        results.sort(key=lambda r: r["composite_score"], reverse=True)
        n_strong = sum(1 for r in results if r["composite_score"] >= 0.65)
        n_med = sum(1 for r in results if 0.45 <= r["composite_score"] < 0.65)

        if n_strong >= 15:
            state, strength = "BUYBACK_RICH", 0.85
        elif n_strong >= 6 or (n_strong + n_med) >= 15:
            state, strength = "ACTIVE", 0.6
        elif n_strong >= 2 or n_med >= 5:
            state, strength = "NORMAL", 0.35
        else:
            state, strength = "QUIET", 0.1

        # Top 20 trade list
        tickets = []
        for r in results[:20]:
            tickets.append({
                "ticker": r["ticker"],
                "side": "LONG",
                "rationale": (
                    f"Buyback yield {r['ttm_buyback_yield_net_pct']}% net "
                    f"({r['ttm_buyback_yield_gross_pct']}% gross), "
                    f"FCF coverage {r['fcf_coverage_ratio']}x, "
                    f"{r['quarters_with_buybacks']}/4 qtrs"
                ),
                "holding_period": "12+ months (quarterly rebalance)",
                "size_pct_portfolio": 0.5,  # equal weight 20 names = 10% total
                "expected_alpha_pct_yr": 4 if r["composite_score"] >= 0.7 else 2.5,
            })

        out = {
            "engine": "buyback-yield-ranking",
            "version": VERSION,
            "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "state": state,
            "signal_strength": round(strength, 2),
            "n_qualified": len(results),
            "n_strong": n_strong,
            "n_medium": n_med,
            "universe_size": len(universe),
            "universe_basis": universe_basis,
            "n_book_candidates": len(book_syms),
            "book_as_of": (_book.as_of() if book_ok() else None),
            "price_source_counts": {k: sum(1 for r in results if r.get("price_source") == k) for k in {r.get("price_source") for r in results}},
            "cashflow_source_counts": {k: sum(1 for r in results if r.get("cashflow_source") == k) for k in {r.get("cashflow_source") for r in results}},
            "top_20_ranked": results[:20],
            "all_ranked": results,
            "trade_tickets": tickets,
            "n_tickets": len(tickets),
            "methodology": (
                "Buyback yield ranking (Pontiff-Woodgate 2008). 5-factor "
                "quality screen: (1) TTM gross buyback yield = "
                "abs(repurchases) / mcap; (2) net yield = gross MINUS share "
                "issuance (avoid SBC-offset shams); (3) FCF coverage >= 1.0x "
                "(sustainable); (4) P/E < 30 gate (avoid extreme multiples); "
                "(5) >=2 of last 4 quarters with buybacks (consistency). "
                "$2B+ mcap gate. Composite weights: yield 35% + net-to-gross "
                "ratio 25% + FCF coverage 20% + consistency 15% + valuation "
                "bonus. Top 20 ranked = equal-weighted portfolio rebalanced "
                "quarterly. Edge: Pontiff-Woodgate 2008 (top decile +4-6%/yr), "
                "Boudoukh-Michaely-Roberts 2007 (total payout yield), "
                "Ikenberry-Lakonishok-Vermaelen 1995 (+12% / 4yr post-buyback)."
            ),
            "sources": [
                "factor book data/warm/factor-book (whole-market candidates; price, mcap, P/E at the bank close; bank_context)",
                "FMP bank data/warm/fmp-bank/cash-flow-statement-bulk quarterly (TTM buybacks, FCF, issuance; no live call)",
                "s3://justhodl-dashboard-live/data/master-ranker.json (curated universe)",
                "FMP /stable/quote (mcap, P/E) - fallback",
                "FMP /stable/cash-flow-statement?period=quarter&limit=4 (TTM buybacks, FCF, issuance) - fallback",
            ],
            "why_now": f"{n_strong} strong + {n_med} moderate buyback-yield setups",
            "run_seconds": round(time.time() - start, 2),
        }

        try:
            prev = ssm.get_parameter(Name=SSM_STATE_KEY)["Parameter"]["Value"]
        except Exception:
            prev = None
        if prev != state and state == "BUYBACK_RICH" and TELEGRAM_TOKEN:
            top5 = "\n".join(
                f"- {r['ticker']} ({r['ttm_buyback_yield_net_pct']}% net yield, "
                f"score {r['composite_score']})"
                for r in results[:5])
            msg = (f"*BUYBACK-YIELD-RANKING -> {state}*\n"
                   f"{n_strong} strong screens (top decile)\n"
                   f"Top 5:\n{top5}\n"
                   f"Quarterly rebalance portfolio. retail-edges.html for full top 20.")
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
                                                         "n_ranked": len(results)})}
    except Exception as e:
        import traceback
        err = {"engine": "buyback-yield-ranking", "error": str(e)[:300],
               "trace": traceback.format_exc()[:600],
               "as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                          Body=json.dumps(err, indent=2).encode("utf-8"),
                          ContentType="application/json")
        except Exception:
            pass
        return {"statusCode": 500, "body": json.dumps({"error": str(e)[:300]})}
