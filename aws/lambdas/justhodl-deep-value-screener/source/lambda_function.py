"""
justhodl-deep-value-screener — Modern Ben Graham net-net pattern.

Identifies stocks where:
  • (cash + investments - all debt) / market_cap >= NET_CASH_RATIO_THRESHOLD (default 0.50)
  • revenue_TTM / market_cap >= REV_RATIO_THRESHOLD (default 0.40)
  • operating_cash_flow positive in ≥2 of last 4 quarters (kills value traps)
  • market_cap >= MIN_MCAP (default $200M, kills illiquid microcap noise)

These are stocks trading at a discount to their cash + revenue base.
Historical pattern: in 2022-2023, baskets of these returned 80-150% over 18-30 months.

Inputs:
  • FMP /stable/profile + /stable/balance-sheet-statement + /stable/income-statement
  • Optional: existing screener output for ticker universe
  • S&P 500 list as primary universe

Output:
  s3://justhodl-dashboard-live/data/deep-value.json

Score (0-100):
  net_cash_pct (60%) + revenue_yield (25%) + cf_quality (15%)
  capped, then 1.2× multiplier if mcap_to_rev <= 1.0 (deep discount)
"""

try:  # bank-first FMP (aws/shared/fmp_bankfirst): /stable answered from the warehouse banks when fresh, else live
    from fmp_bankfirst import install as _fmp_bankfirst_install
    _fmp_bankfirst_install()
except Exception:  # noqa: BLE001 - the shim never blocks the engine
    pass
import io, json, os, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/deep-value.json")
FMP_KEY = managed_secret(('FMP_KEY', 'FMP_API_KEY'), ("/justhodl/fmp/api-key",))
N_WORKERS = int(os.environ.get("N_WORKERS", "8"))
MIN_MCAP = float(os.environ.get("MIN_MCAP", "200000000"))      # $200M
NET_CASH_RATIO = float(os.environ.get("NET_CASH_RATIO", "0.50"))
REV_RATIO = float(os.environ.get("REV_RATIO", "0.40"))
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "600"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "240"))

S3 = boto3.client("s3", region_name=REGION)

# v1.1 (2026-10-26, bank fusion): the universe starts from the whole US market in the factor book (justhodl-bank-factors)
# - every common stock >= MIN_MCAP whose bank numbers already look like net cash (cash/share vs price), a Graham
# net-current-asset discount or a sub-0.6x sales multiple - instead of a 600-name seed list; the exact cash + investments
# - debt test below still decides.  Prices / market caps / 52-week highs come from the book's bank close (labelled), so
# the screen does not depend on hundreds of live /quote calls; each row carries the book's Altman Z, Piotroski, short %
# float, analyst upside and value composite as context.
try:
    import bank_enrich as _book
except Exception:  # noqa: BLE001 - without the SDK the engine is the seed-list screen it was
    _book = None


def book_candidates():
    """Whole-market net-cash candidates from the factor book, best cash/mcap first; ([], None) when the book is absent."""
    if _book is None or not _book.available():
        return [], None

    def looks_cheap(r):
        # an OPERATING company (it has revenue: a positive sales multiple), US-domiciled common stock (no ADR), with the
        # bank's numbers already pointing at net cash; SPAC shells, warrants, preferreds, funds never enter (SDK flags).
        # first light 2026-10-26: without these two gates the 500 candidates were cash shells and foreign ADRs whose
        # statements the bulk does not hold -> 492 "no_balance"
        if (r.get("mcap") or 0) < MIN_MCAP or r.get("is_adr"):
            return False
        px, cps, ncav, ps = r.get("price"), r.get("cash_per_share"), r.get("net_current_asset_value"), r.get("ps")
        if ps is None or ps <= 0 or ps > 3.0:
            return False
        cash_pct = (cps / px) if (cps and px and px > 0) else 0.0
        ncav_pct = (ncav / r["mcap"]) if (ncav and r.get("mcap")) else 0.0
        return cash_pct >= 0.25 or ncav_pct >= 0.15 or ps <= 0.6
    rows = _book.screen(looks_cheap, fields=["price", "cash_per_share", "net_current_asset_value", "ps", "mcap", "is_adr"])
    rows.sort(key=lambda r: -((r.get("cash_per_share") or 0) / r["price"] if r.get("price") else 0))
    return [r["symbol"] for r in rows[:MAX_TICKERS]], _book.as_of()


def book_quote(symbol):
    """quote-shaped row (price, marketCap, yearHigh) from the factor book's bank close, with context; None when absent."""
    if _book is None or not _book.available():
        return None
    try:
        q = _book.quote(symbol)
        if not q or not q.get("price") or not q.get("marketCap"):
            return None
        q["bank_context"] = _book.lookup(symbol, ["altman_z", "piotroski", "composite_value", "composite_quality", "short_pct_float", "pt_upside_pct", "dcf_upside_pct"])
        return q
    except Exception:  # noqa: BLE001
        return None


# ─────────────────────────────────────────────────────────────────────
# UNIVERSE
# ─────────────────────────────────────────────────────────────────────
SP500_BACKUP = [
    # Common large/mid caps to seed scanning if S&P list is unavailable
    "AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","BRK-B","TSLA","AVGO","JPM","WMT","LLY","V","MA",
    "ORCL","XOM","UNH","COST","HD","PG","JNJ","NFLX","BAC","ABBV","CRM","KO","CVX","TMO","MRK","CSCO",
    "ACN","AMD","ADBE","PEP","LIN","WFC","ABT","DIS","TXN","INTC","NOW","MCD","ISRG","DHR","CMCSA",
    "VZ","PM","INTU","UNP","NEE","RTX","QCOM","SPGI","HON","BX","T","LOW","CAT","GS","BA","NKE","BLK",
    "AMAT","SCHW","SYK","UBER","C","BKNG","DE","TMUS","AXP","MDLZ","GILD","PFE","TJX","ETN","ADP",
    "MU","BSX","ADI","VRTX","PANW","PLD","REGN","SO","BMY","MO","ANET","KLAC","ZTS","SLB","CB","FI",
    "EQIX","DUK","ICE","CI","MMC","PGR","WM","CRWD","ELV","CDNS","NOC","AON","TGT","SHW","SNPS","CVS",
    "MCK","CL","PYPL","ORLY","TT","FCX","HCA","ITW","DELL","COF","FDX","CME","APD","CMG","EOG","USB",
    "MSI","WELL","BDX","MAR","PNC","ECL","EMR","TFC","NSC","ROP","MMM","GD","AJG","RSG","CARR","PSX",
    "AZO","CPRT","PCAR","SRE","AFL","FTNT","KMB","TRV","GM","ADSK","BK","SPG","KMI","DLR","PH","NEM",
    "GIS","HLT","D","FAST","ALL","WMB","O","KDP","KHC","STZ","HSY","COR","GWW","WBD","DOW","WBA",
    "CMI","CHTR","DXCM","PCG","ROST","MET","AEP","SYY","KR","AME","TEL","HUM","XEL","BKR","RCL","EXC",
    "TDG","NXPI","MNST","CTAS","JCI","HES","IDXX","VLO","OXY","FIS","FANG","STT","CTSH","OTIS","PWR",
    "MPC","EFX","PRU","DD","DG","VRSK","RJF","EW","EBAY","A","ON","BIIB","CNC","ANSS","KEYS","WTW",
    "GPN","AVB","DLTR","WDC","HIG","NDAQ","DHI","LEN","ETR","WAB","AIG","FE","MTD","DVN","WEC","ED",
    "GLW","HBAN","TSCO","STE","FITB","ACGL","PHM","NTAP","K","PPG","ZBH","BAX","ROK","ARES","CTRA",
    "EXPE","CPB","DTE","KEY","WMG","SBAC","RF","DRI","ESS","TYL","BLDR","STX","RMD","INVH","EIX",
    "CTVA","CNP","ATO","HEI","STLD","CHD","NUE","BR","CDW","HOLX","WAT","ARE","CAH","CAG","BALL",
    "LDOS","WST","CINF","CMS","CFG","WBA","LH","JBL","TXT","TRMB","AKAM","MOH","UAL","MGM","BG",
    "GEHC","HRL","DGX","KIM","IT","PNW","STT","GEN","INCY","FDS","DOV","MAS","DPZ","JBHT","UDR",
    "SBNY","NTRS","SWK","NRG","BBY","PFG","ALLE","NWSA","ZBRA","FFIV","CE","WRB","TXT","SWKS",
]


def _http_get_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-DV-Screener/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_universe():
    """Return up to MAX_TICKERS: the factor book's whole-market net-cash candidates first, then the unified universe
    (data/universe.json) and the older seeds fill the remainder."""
    universe = []
    book_syms, book_as_of = book_candidates()
    universe.extend(book_syms)
    get_universe.book = (len(book_syms), book_as_of)
    print(f"[deep-value] factor book candidates: {len(book_syms)} (bank as of {(book_as_of or {}).get('fmp-bank')})")

    # PRIMARY: pull from unified universe (master pool)
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/universe.json")
        ud = json.loads(obj["Body"].read())
        for s in ud.get("stocks", []):
            sym = (s.get("symbol") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] seeded {len(universe)} from data/universe.json (unified)")
    except Exception as e:
        print(f"[deep-value] unified universe failed, falling back: {e}")

    # FALLBACK: existing screener output
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] universe after screener fallback: {len(universe)}")
    except Exception as e:
        print(f"[deep-value] screener seed failed: {e}")

    # Try the asymmetric scorer output (mid-cap names)
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="data/asymmetric-scorer.json")
        d = json.loads(obj["Body"].read())
        for c in d.get("candidates", []) or d.get("setups", []) or []:
            sym = (c.get("symbol") or c.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[deep-value] universe after asymmetric: {len(universe)}")
    except Exception:
        pass

    # Add SP500 backup
    for s in SP500_BACKUP:
        if s not in universe:
            universe.append(s)
    print(f"[deep-value] universe after SP500 backup: {len(universe)}")

    return universe[:MAX_TICKERS]


def fetch_profile(symbol):
    """FMP /stable/profile/{symbol} → {marketCap, sector, industry, price, name}."""
    url = f"https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list) and r:
            return r[0]
    except Exception as e:
        pass
    return None


def fetch_balance(symbol):
    """FMP /stable/balance-sheet-statement (most recent quarter)."""
    url = f"https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={symbol}&period=quarter&limit=1&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list) and r:
            return r[0]
    except Exception:
        pass
    return None


def fetch_income(symbol):
    """FMP /stable/income-statement TTM."""
    url = f"https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&period=annual&limit=1&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list) and r:
            return r[0]
    except Exception:
        pass
    return None


def fetch_cashflow(symbol):
    """FMP /stable/cash-flow-statement quarterly, last 4 → for CF positivity check."""
    url = f"https://financialmodelingprep.com/stable/cash-flow-statement?symbol={symbol}&period=quarter&limit=4&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list):
            return r
    except Exception:
        pass
    return []


def fetch_quote(symbol):
    """FMP /stable/quote → {price, marketCap, yearHigh}."""
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list) and r:
            return r[0]
    except Exception:
        pass
    return None


def evaluate_ticker(symbol, deadline_at):
    """Return dict with all relevant fields, or None if can't qualify."""
    if time.time() > deadline_at:
        return {"symbol": symbol, "status": "deadline_skip"}

    # Quote first — the factor book's bank close (labelled) when it holds the name, else the live /quote
    q = book_quote(symbol)
    price_source = "factor-book close as of %s" % (((q or {}).get("as_of") or {}).get("fmp-bank") or "")[:10] if q else "live quote"
    if not q:
        q = fetch_quote(symbol)
    if not q:
        return {"symbol": symbol, "status": "no_quote"}
    mcap = q.get("marketCap") or 0
    if mcap < MIN_MCAP:
        return {"symbol": symbol, "status": "below_min_mcap", "mcap": mcap}
    price = q.get("price") or 0
    yhigh = q.get("yearHigh") or price
    pct_from_52h = ((price - yhigh) / yhigh * 100.0) if yhigh else 0.0

    # Balance sheet for cash + debt
    b = fetch_balance(symbol)
    if not b:
        return {"symbol": symbol, "status": "no_balance"}
    cash = (b.get("cashAndCashEquivalents") or 0) + (b.get("shortTermInvestments") or 0)
    longinv = b.get("longTermInvestments") or 0
    total_debt = b.get("totalDebt") or 0
    net_cash = cash + longinv - total_debt
    net_cash_pct = net_cash / mcap if mcap else 0

    # Quick early-out: needs at least 15% net cash to be worth checking
    if net_cash_pct < 0.15:
        return {"symbol": symbol, "status": "below_min_net_cash", "net_cash_pct": round(net_cash_pct, 3)}

    # Income for revenue
    inc = fetch_income(symbol)
    if not inc:
        return {"symbol": symbol, "status": "no_income"}
    rev = inc.get("revenue") or 0
    rev_yield = rev / mcap if mcap else 0

    # Cash flow check
    cf = fetch_cashflow(symbol)
    cf_positive_q = sum(1 for q_ in cf if (q_.get("operatingCashFlow") or 0) > 0)
    cf_total_q = len(cf)
    cf_quality = cf_positive_q / max(cf_total_q, 1)

    # Score
    nc_score = min(net_cash_pct / 0.8, 1.0) * 60
    rev_score = min(rev_yield / 1.0, 1.0) * 25
    cf_score = cf_quality * 15
    score = nc_score + rev_score + cf_score
    # Deep discount multiplier — but cap at 100
    mcap_to_rev = mcap / rev if rev else 999
    if mcap_to_rev <= 1.0:
        score = min(score * 1.2, 100)
    # Drawdown bonus — contrarian buying into 25%+ drawdown
    if pct_from_52h <= -25:
        score = min(score + 5, 100)
    if pct_from_52h <= -50:
        score = min(score + 10, 100)

    # Sector + industry — FMP /quote often has these blank, so always pull /profile
    profile = fetch_profile(symbol)
    sector = (profile or {}).get("sector") or q.get("sector") or ""
    industry = (profile or {}).get("industry") or q.get("industry") or ""
    company = (profile or {}).get("companyName") or q.get("name") or symbol

    # Sector adjustments — insurance/banking carry massive investment books
    # that aren't discretionary "net cash". Score them differently.
    sector_lower = (sector or "").lower()
    industry_lower = (industry or "").lower()
    company_lower = (company or "").lower()

    # Comprehensive financial-book detection across sector + industry + company name
    fin_keywords = [
        "insurance", "bank", "banking", "investment management",
        "asset management", "capital markets", "financial conglomerate",
        "credit services", "diversified financial", "financial services",
        "savings", "thrift", "mortgage", "broker", "exchange",
    ]
    fin_company_keywords = [
        "bancorp", "bancshares", "financial corp", "insurance",
        "ins co", "& co.", "capital", "wells fargo", "bank of america",
        "jpmorgan", "morgan stanley", "goldman", "citigroup", "blackrock",
        "blackstone", "kkr", "apollo global", "carlyle", "ares capital",
    ]
    is_financial_book = (
        any(k in sector_lower for k in fin_keywords)
        or any(k in industry_lower for k in fin_keywords)
        or any(k in company_lower for k in fin_company_keywords)
    )

    is_reit = (
        "reit" in industry_lower
        or sector_lower == "real estate"
        or "real estate trust" in company_lower
        or "trust" in industry_lower and "estate" in industry_lower
    )

    flag = "MONITOR"
    if is_financial_book:
        flag = "FINANCIAL_BOOK_EXCLUDED"
        score = score * 0.2
    elif is_reit:
        flag = "REIT_EXCLUDED"
        score = score * 0.2
    elif net_cash_pct >= NET_CASH_RATIO and rev_yield >= REV_RATIO and cf_quality >= 0.5:
        flag = "DEEP_VALUE_TIER_A"
    elif net_cash_pct >= 0.35 and rev_yield >= 0.25 and cf_quality >= 0.25:
        flag = "DEEP_VALUE_TIER_B"
    elif net_cash_pct >= 0.20 and cf_quality >= 0.25:
        flag = "NET_CASH_WATCH"
    else:
        flag = "MARGINAL"

    return {
        "symbol": symbol,
        "company": company,
        "score": round(min(score, 100), 1),
        "flag": flag,
        "status": "ok",
        "price_source": price_source,
        "bank_context": q.get("bank_context"),
        "fundamentals": {
            "market_cap": mcap,
            "price": price,
            "year_high": yhigh,
            "pct_from_52w_high": round(pct_from_52h, 1),
            "cash": cash,
            "long_investments": longinv,
            "total_debt": total_debt,
            "net_cash": net_cash,
            "net_cash_pct_of_mcap": round(net_cash_pct, 3),
            "revenue_ttm": rev,
            "revenue_yield_of_mcap": round(rev_yield, 3),
            "mcap_to_rev": round(mcap_to_rev, 2),
            "operating_cf_positive_quarters": cf_positive_q,
            "operating_cf_total_quarters": cf_total_q,
            "sector": sector,
            "industry": industry,
        },
        "rationale": _build_rationale(symbol, mcap, net_cash_pct, rev_yield, mcap_to_rev, pct_from_52h, cf_positive_q, cf_total_q, sector),
    }


def _build_rationale(sym, mcap, nc_pct, rev_yield, m2r, ph, cf_pos, cf_total, sector):
    parts = [f"{sym} ({sector})"]
    parts.append(f"trades at ${mcap/1e9:.2f}B mcap with {nc_pct*100:.0f}% net cash")
    parts.append(f"and {rev_yield*100:.0f}% revenue yield")
    parts.append(f"(mcap/rev = {m2r:.2f}×)")
    if ph <= -25:
        parts.append(f"— stock down {abs(ph):.0f}% from 52w high")
    parts.append(f"— OCF positive {cf_pos}/{cf_total} quarters")
    return " ".join(parts) + "."


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[deep-value] starting v1.1 (bank fusion), max_tickers={MAX_TICKERS}, budget={TIMEOUT_BUDGET_S}s")

    universe = get_universe()
    print(f"[deep-value] universe size: {len(universe)}")

    results = []
    statuses = {"ok": 0, "no_quote": 0, "below_min_mcap": 0, "no_balance": 0,
                 "below_min_net_cash": 0, "no_income": 0, "deadline_skip": 0}

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate_ticker, s, deadline_at): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
            except Exception as e:
                continue
            if r:
                statuses[r.get("status", "ok")] = statuses.get(r.get("status", "ok"), 0) + 1
                if r.get("status") == "ok":
                    results.append(r)

    print(f"[deep-value] evaluated {len(universe)}, OK: {len(results)}, statuses: {statuses}")

    # Sort and slice
    results.sort(key=lambda x: x["score"], reverse=True)
    tier_a = [r for r in results if r["flag"] == "DEEP_VALUE_TIER_A"]
    excluded = [r for r in results if r["flag"] in ("FINANCIAL_BOOK_EXCLUDED", "REIT_EXCLUDED")]
    tier_b = [r for r in results if r["flag"] == "DEEP_VALUE_TIER_B"]
    watch = [r for r in results if r["flag"] == "NET_CASH_WATCH"]

    # Top by drawdown — contrarian deep value
    contrarian = sorted(
        [r for r in results if (r["fundamentals"].get("pct_from_52w_high") or 0) <= -25],
        key=lambda x: x["score"], reverse=True
    )[:25]

    out = {
        "schema_version": 1,
        "method": "deep_value_screener_v1.1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_book_candidates": getattr(get_universe, "book", (0, None))[0],
            "book_as_of": getattr(get_universe, "book", (0, None))[1],
            "universe_basis": ("factor book whole-market net-cash candidates (%d) + unified universe / seeds" % getattr(get_universe, "book", (0, None))[0]) if getattr(get_universe, "book", (0, None))[0] else "unified universe / seeds (factor book unavailable)",
            "n_qualifying": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "n_watch": len(watch),
            "n_contrarian": len(contrarian),
            "statuses": statuses,
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "company": r["company"],
                    "score": r["score"],
                    "flag": r["flag"],
                    "net_cash_pct": r["fundamentals"]["net_cash_pct_of_mcap"],
                    "rev_yield": r["fundamentals"]["revenue_yield_of_mcap"],
                    "mcap_to_rev": r["fundamentals"]["mcap_to_rev"],
                    "pct_from_52w_high": r["fundamentals"]["pct_from_52w_high"],
                    "sector": r["fundamentals"]["sector"],
                }
                for r in [x for x in results if x["flag"] not in ("FINANCIAL_BOOK_EXCLUDED", "REIT_EXCLUDED")][:25]
            ],
            "top_25_excluded_financials": [
                {
                    "symbol": r["symbol"],
                    "score": r["score"],
                    "flag": r["flag"],
                    "sector": r["fundamentals"]["sector"],
                }
                for r in [x for x in results if x["flag"] in ("FINANCIAL_BOOK_EXCLUDED", "REIT_EXCLUDED")][:25]
            ],
            "tier_a": [r["symbol"] for r in tier_a],
            "contrarian_top": [r["symbol"] for r in contrarian[:15]],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[deep-value] wrote {len(body)}b to {S3_KEY}")
    print(f"[deep-value] tier_a={len(tier_a)} tier_b={len(tier_b)} watch={len(watch)} contrarian={len(contrarian)}")
    if results[:8]:
        print(f"[deep-value] TOP: {[(r['symbol'], r['score'], r['flag']) for r in results[:8]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_universe": len(universe),
            "n_qualifying": len(results),
            "n_tier_a": len(tier_a),
            "duration_s": round(time.time() - started, 1),
        }),
    }
