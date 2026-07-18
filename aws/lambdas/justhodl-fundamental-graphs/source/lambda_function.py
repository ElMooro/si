"""justhodl-fundamental-graphs v1.0.0 (ops 3462)
MARKER: FUNDGRAPH_V1_OPS3462

TradingView-class "Fundamental Graphs" API for fundamental-graphs.html.
One call per symbol+period returns ~150 aligned time series over 10+ years:

  - Raw statement lines (income / balance / cash-flow) straight from
    FMP /stable (same field vocabulary the Beneish + share-flows engines
    already parse in production — zero field-name roulette).
  - Every ratio/margin/valuation COMPUTED IN-HOUSE from the statements +
    dilluted share count + the price tape (mcap_t = close(t) x shares_t),
    TTM-proper, period-aligned. No dependence on FMP's ratio endpoints.
  - Forensic scores per period: Altman Z, Piotroski F, Beneish M,
    Sloan accruals — consistent with the fleet's forensic-screen doctrine.
  - Analyst estimates (history + future) for the Forecasts tab.
  - Weekly close series for the "Show price charts" overlay.

Real data only. S3-cached 20h (data/fundgraph/cache/). Served through a
public Function URL (CORS *), gzip-encoded when the client accepts it.

Invoke shapes:
  Function URL GET  ?symbol=AAPL&period=quarter|annual[&refresh=1]
  Direct/Event      {"symbol":"AAPL","period":"quarter"}
  Warm (Event)      {"warm":["AAPL","CHTR"],"periods":["quarter","annual"]}
"""

import base64
import gzip
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_KEY = os.environ.get("FMP_KEY", "")
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
CACHE_PREFIX = "data/fundgraph/cache/"
CACHE_TTL_SEC = int(os.environ.get("CACHE_TTL_SEC", 20 * 3600))
MAX_Q = 44          # ~11y of quarters served
MAX_A = 12          # annuals served
FETCH_Q = 50        # fetch a little extra for TTM/YoY lookbacks
FETCH_A = 14
UA = {"User-Agent": "Mozilla/5.0 (justhodl-fundamental-graphs/1.0)"}
SLEEP = 0.22

_s3 = boto3.client("s3")


# ── tiny utils ───────────────────────────────────────────────────────────────
def num(x):
    try:
        if x is None or isinstance(x, bool):
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except (TypeError, ValueError):
        return None


def g(row, *names):
    """First non-null numeric among alias field names."""
    for n in names:
        v = num(row.get(n))
        if v is not None:
            return v
    return None


def _http(url, timeout=25):
    last = None
    for att in range(3):
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, headers=UA), timeout=timeout
            ) as r:
                return json.loads(r.read().decode("utf-8", "replace"))
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(0.6 * (att + 1))
    raise RuntimeError(f"fetch failed: {str(last)[:160]}")


def _fmp(path_qs):
    sep = "&" if "?" in path_qs else "?"
    data = _http(f"{FMP_BASE}/{path_qs}{sep}apikey={FMP_KEY}")
    time.sleep(SLEEP)
    return data


def rnd(v, p=6):
    if v is None:
        return None
    try:
        return round(float(v), p)
    except (TypeError, ValueError):
        return None


def clamp(v, lo, hi):
    if v is None:
        return None
    return max(lo, min(hi, v))


# ── FMP fetchers (field names mirror in-fleet consumers) ─────────────────────
def fetch_statements(sym, period):
    lim = FETCH_Q if period == "quarter" else FETCH_A
    pq = f"symbol={urllib.parse.quote(sym)}&period={period}&limit={lim}"
    inc = _fmp(f"income-statement?{pq}")
    bal = _fmp(f"balance-sheet-statement?{pq}")
    cf = _fmp(f"cash-flow-statement?{pq}")
    for name, x in (("income", inc), ("balance", bal), ("cashflow", cf)):
        if not isinstance(x, list):
            raise RuntimeError(f"{name} statement not a list for {sym}")
    return inc, bal, cf


def fetch_estimates(sym, period):
    pq = f"symbol={urllib.parse.quote(sym)}&period={period}&limit=60"
    try:
        est = _fmp(f"analyst-estimates?{pq}")
        if not (isinstance(est, list) and est):
            est = _fmp(f"analyst-estimates?symbol={urllib.parse.quote(sym)}&limit=40")
    except Exception:  # noqa: BLE001
        est = []
    return est if isinstance(est, list) else []


def fetch_profile(sym):
    try:
        p = _fmp(f"profile?symbol={urllib.parse.quote(sym)}")
        row = p[0] if isinstance(p, list) and p else (p if isinstance(p, dict) else {})
        return {
            "name": row.get("companyName") or row.get("name") or sym,
            "sector": row.get("sector") or "",
            "industry": row.get("industry") or "",
            "currency": row.get("currency") or "USD",
            "mktCap": g(row, "mktCap", "marketCap"),
            "price": g(row, "price"),
            "exchange": row.get("exchangeShortName") or row.get("exchange") or "",
        }
    except Exception:  # noqa: BLE001
        return {"name": sym, "sector": "", "industry": "", "currency": "USD",
                "mktCap": None, "price": None, "exchange": ""}


def fetch_price(sym):
    """Daily closes ~10.5y. Returns (daily list[(date,close)] asc, weekly list)."""
    frm = (datetime.now(timezone.utc) - timedelta(days=3860)).strftime("%Y-%m-%d")
    try:
        data = _fmp(
            f"historical-price-eod/light?symbol={urllib.parse.quote(sym)}&from={frm}"
        )
    except Exception:  # noqa: BLE001
        data = []
    hist = data.get("historical") or data.get("data") or [] if isinstance(data, dict) else data
    rows = []
    for r in hist if isinstance(hist, list) else []:
        d = r.get("date")
        c = g(r, "close", "price", "adjClose")
        if d and c is not None and c > 0:
            rows.append((str(d)[:10], c))
    rows.sort(key=lambda t: t[0])
    weekly, last_wk = [], None
    for d, c in rows:
        try:
            wk = datetime.strptime(d, "%Y-%m-%d").isocalendar()[:2]
        except ValueError:
            continue
        if wk != last_wk:
            weekly.append([d, rnd(c, 4)])
            last_wk = wk
        else:
            weekly[-1] = [d, rnd(c, 4)]
    return rows, weekly


def price_at(daily, date):
    """Nearest close on/before date (binary search)."""
    if not daily:
        return None
    lo, hi = 0, len(daily) - 1
    if date < daily[0][0]:
        return None
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if daily[mid][0] <= date:
            lo = mid
        else:
            hi = mid - 1
    return daily[lo][1]


# ── series assembly ──────────────────────────────────────────────────────────
def by_date(rows):
    out = {}
    for r in rows:
        d = str(r.get("date") or "")[:10]
        if len(d) == 10:
            out[d] = r
    return out


def nearest(dmap, date, tol_days=48):
    if date in dmap:
        return dmap[date]
    try:
        t0 = datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        return {}
    best, bestdiff = {}, tol_days + 1
    for d, r in dmap.items():
        try:
            diff = abs((datetime.strptime(d, "%Y-%m-%d") - t0).days)
        except ValueError:
            continue
        if diff < bestdiff:
            best, bestdiff = r, diff
    return best if bestdiff <= tol_days else {}


def build_doc(sym, period):
    inc, bal, cf = fetch_statements(sym, period)
    if not inc:
        raise RuntimeError(f"no income data for {sym}")
    est_rows = fetch_estimates(sym, period)
    profile = fetch_profile(sym)
    daily_px, weekly_px = fetch_price(sym)

    bmap, cmap = by_date(bal), by_date(cf)
    inc_sorted = sorted(
        [r for r in inc if str(r.get("date") or "")[:10]],
        key=lambda r: str(r["date"])[:10],
    )
    frames = []
    for r in inc_sorted:
        d = str(r["date"])[:10]
        frames.append({"date": d, "inc": r, "bal": nearest(bmap, d), "cf": nearest(cmap, d)})
    n = len(frames)
    lb = 4 if period == "quarter" else 1  # YoY lookback in periods

    # raw extract per frame ---------------------------------------------------
    R = []
    for f in frames:
        i, b, c = f["inc"], f["bal"], f["cf"]
        row = {
            "date": f["date"],
            # income
            "revenue": g(i, "revenue"),
            "costOfRevenue": g(i, "costOfRevenue"),
            "grossProfit": g(i, "grossProfit"),
            "rnd": g(i, "researchAndDevelopmentExpenses"),
            "sgna": g(i, "sellingGeneralAndAdministrativeExpenses",
                      "generalAndAdministrativeExpenses"),
            "opex": g(i, "operatingExpenses"),
            "operatingIncome": g(i, "operatingIncome"),
            "ebitda": g(i, "ebitda"),
            "da_is": g(i, "depreciationAndAmortization"),
            "interestExpense": g(i, "interestExpense"),
            "interestIncome": g(i, "interestIncome"),
            "pretaxIncome": g(i, "incomeBeforeTax"),
            "taxExpense": g(i, "incomeTaxExpense"),
            "netIncome": g(i, "netIncome"),
            "eps": g(i, "eps"),
            "epsDiluted": g(i, "epsDiluted", "epsdiluted"),
            "shs": g(i, "weightedAverageShsOut"),
            "shsDil": g(i, "weightedAverageShsOutDil"),
            # balance
            "cash": g(b, "cashAndCashEquivalents"),
            "sti": g(b, "shortTermInvestments"),
            "cashSTI": g(b, "cashAndShortTermInvestments"),
            "receivables": g(b, "netReceivables"),
            "inventory": g(b, "inventory"),
            "totalCurrentAssets": g(b, "totalCurrentAssets"),
            "ppeNet": g(b, "propertyPlantEquipmentNet"),
            "goodwill": g(b, "goodwill"),
            "intangibles": g(b, "intangibleAssets"),
            "gwIntang": g(b, "goodwillAndIntangibleAssets"),
            "ltInvestments": g(b, "longTermInvestments"),
            "totalAssets": g(b, "totalAssets"),
            "accountsPayable": g(b, "accountPayables"),
            "shortTermDebt": g(b, "shortTermDebt"),
            "deferredRevenue": g(b, "deferredRevenue"),
            "totalCurrentLiabilities": g(b, "totalCurrentLiabilities"),
            "longTermDebt": g(b, "longTermDebt"),
            "totalLiabilities": g(b, "totalLiabilities"),
            "retainedEarnings": g(b, "retainedEarnings"),
            "equity": g(b, "totalStockholdersEquity", "totalEquity"),
            "totalDebt": g(b, "totalDebt"),
            "netDebt": g(b, "netDebt"),
            "minorityInterest": g(b, "minorityInterest"),
            # cash flow
            "cfo": g(c, "netCashProvidedByOperatingActivities",
                     "operatingCashFlow"),
            "da_cf": g(c, "depreciationAndAmortization"),
            "sbc": g(c, "stockBasedCompensation"),
            "dWorkingCapital": g(c, "changeInWorkingCapital"),
            "cfi": g(c, "netCashProvidedByInvestingActivities",
                     "netCashUsedForInvestingActivites",
                     "netCashUsedForInvestingActivities"),
            "capex": g(c, "capitalExpenditure",
                       "investmentsInPropertyPlantAndEquipment"),
            "acquisitions": g(c, "acquisitionsNet"),
            "purchInvest": g(c, "purchasesOfInvestments"),
            "saleInvest": g(c, "salesMaturitiesOfInvestments"),
            "cff": g(c, "netCashProvidedByFinancingActivities",
                     "netCashUsedProvidedByFinancingActivities"),
            "debtRepayment": g(c, "debtRepayment", "netDebtIssuance"),
            "stockIssued": g(c, "commonStockIssued", "commonStockIssuance"),
            "stockRepurchased": g(c, "commonStockRepurchased",
                                  "commonStockRepurchase"),
            "dividendsPaid": g(c, "dividendsPaid", "netDividendsPaid",
                               "commonDividendsPaid"),
            "fcf": g(c, "freeCashFlow"),
        }
        if row["cashSTI"] is None:
            cs = [x for x in (row["cash"], row["sti"]) if x is not None]
            row["cashSTI"] = sum(cs) if cs else None
        if row["gwIntang"] is None:
            gi = [x for x in (row["goodwill"], row["intangibles"]) if x is not None]
            row["gwIntang"] = sum(gi) if gi else None
        if row["fcf"] is None and row["cfo"] is not None and row["capex"] is not None:
            row["fcf"] = row["cfo"] + row["capex"]  # capex reported negative
        if row["ebitda"] is None and row["operatingIncome"] is not None:
            row["ebitda"] = row["operatingIncome"] + (row["da_is"] or row["da_cf"] or 0)
        R.append(row)

    def col(key):
        return [r.get(key) for r in R]

    def ttm(vals):
        """Rolling sum of the trailing `lb` periods (annual passthrough)."""
        out = [None] * n
        if lb == 1:
            return list(vals)
        for i in range(n):
            if i >= lb - 1:
                w = vals[i - lb + 1: i + 1]
                if all(v is not None for v in w):
                    out[i] = sum(w)
        return out

    rev_t = ttm(col("revenue"))
    cogs_t = ttm(col("costOfRevenue"))
    gp_t = ttm(col("grossProfit"))
    ebitda_t = ttm(col("ebitda"))
    ebit_t = ttm(col("operatingIncome"))
    ni_t = ttm(col("netIncome"))
    pretax_t = ttm(col("pretaxIncome"))
    tax_t = ttm(col("taxExpense"))
    cfo_t = ttm(col("cfo"))
    cfi_t = ttm(col("cfi"))
    capex_t = ttm(col("capex"))
    fcf_t = ttm(col("fcf"))
    sbc_t = ttm(col("sbc"))
    rnd_t = ttm(col("rnd"))
    sga_t = ttm(col("sgna"))
    da_t = ttm([r.get("da_cf") if r.get("da_cf") is not None else r.get("da_is") for r in R])
    intexp_t = ttm(col("interestExpense"))
    div_t = ttm(col("dividendsPaid"))
    buyb_t = ttm(col("stockRepurchased"))
    eps_t = ttm(col("epsDiluted")) if lb > 1 else col("epsDiluted")

    # market cap / EV per period ---------------------------------------------
    mcap, ev = [None] * n, [None] * n
    for i, r in enumerate(R):
        sh = r.get("shsDil") or r.get("shs")
        px = price_at(daily_px, r["date"])
        if sh and px:
            mcap[i] = sh * px
            debt = r.get("totalDebt")
            if debt is None:
                parts = [x for x in (r.get("shortTermDebt"), r.get("longTermDebt"))
                         if x is not None]
                debt = sum(parts) if parts else 0.0
            ev[i] = mcap[i] + (debt or 0) - (r.get("cashSTI") or 0)

    def div_(a, b, mult=1.0, pos_denom=True):
        if a is None or b in (None, 0):
            return None
        if pos_denom and b <= 0:
            return None
        return a / b * mult

    P = {}

    def put(key, i, val, prec=6):
        v = rnd(val, prec)
        if v is None:
            return
        P.setdefault(key, []).append([R[i]["date"], v])

    RAW_KEYS = [
        "revenue", "costOfRevenue", "grossProfit", "rnd", "sgna", "opex",
        "operatingIncome", "ebitda", "interestExpense", "interestIncome",
        "pretaxIncome", "taxExpense", "netIncome", "eps", "epsDiluted",
        "shs", "shsDil",
        "cash", "sti", "cashSTI", "receivables", "inventory",
        "totalCurrentAssets", "ppeNet", "goodwill", "intangibles", "gwIntang",
        "ltInvestments", "totalAssets", "accountsPayable", "shortTermDebt",
        "deferredRevenue", "totalCurrentLiabilities", "longTermDebt",
        "totalLiabilities", "retainedEarnings", "equity", "totalDebt",
        "netDebt", "minorityInterest",
        "cfo", "sbc", "dWorkingCapital", "cfi", "capex", "acquisitions",
        "purchInvest", "saleInvest", "cff", "debtRepayment", "stockIssued",
        "stockRepurchased", "dividendsPaid", "fcf",
    ]

    keep_from = max(0, n - (MAX_Q if period == "quarter" else MAX_A))
    for i in range(keep_from, n):
        r = R[i]
        for k in RAW_KEYS:
            put(k, i, r.get(k), 2)
        put("da", i, r.get("da_cf") if r.get("da_cf") is not None else r.get("da_is"), 2)

        TA, TL, EQ = r.get("totalAssets"), r.get("totalLiabilities"), r.get("equity")
        sh = r.get("shsDil") or r.get("shs")
        mc, evv = mcap[i], ev[i]
        wc = (None if (r.get("totalCurrentAssets") is None or
                       r.get("totalCurrentLiabilities") is None)
              else r["totalCurrentAssets"] - r["totalCurrentLiabilities"])
        nd = r.get("netDebt")
        if nd is None and r.get("totalDebt") is not None:
            nd = r["totalDebt"] - (r.get("cashSTI") or 0)
        tang_eq = (None if EQ is None else EQ - (r.get("gwIntang") or 0))

        # TTM flow snapshots
        put("revenue_ttm", i, rev_t[i], 2)
        put("ebitda_ttm", i, ebitda_t[i], 2)
        put("ebit_ttm", i, ebit_t[i], 2)
        put("net_income_ttm", i, ni_t[i], 2)
        put("cfo_ttm", i, cfo_t[i], 2)
        put("fcf_ttm", i, fcf_t[i], 2)
        put("gross_profit_ttm", i, gp_t[i], 2)

        # market layer
        put("mcap", i, mc, 2)
        put("ev", i, evv, 2)
        put("working_capital", i, wc, 2)
        put("tangible_equity", i, tang_eq, 2)
        put("net_debt_calc", i, nd, 2)

        # margins %
        put("gross_margin_pct", i, div_(gp_t[i], rev_t[i], 100), 3)
        put("operating_margin_pct", i, div_(ebit_t[i], rev_t[i], 100), 3)
        put("ebitda_margin_pct", i, div_(ebitda_t[i], rev_t[i], 100), 3)
        put("net_margin_pct", i, div_(ni_t[i], rev_t[i], 100, pos_denom=True), 3)
        put("fcf_margin_pct", i, div_(fcf_t[i], rev_t[i], 100), 3)

        # returns %
        eq_prev = R[i - lb]["equity"] if i - lb >= 0 else None
        eq_avg = (EQ + eq_prev) / 2 if (EQ is not None and eq_prev is not None) else EQ
        ta_prev = R[i - lb]["totalAssets"] if i - lb >= 0 else None
        ta_avg = (TA + ta_prev) / 2 if (TA is not None and ta_prev is not None) else TA
        put("roe_pct", i, div_(ni_t[i], eq_avg, 100), 3)
        put("roa_pct", i, div_(ni_t[i], ta_avg, 100), 3)
        nopat = None
        if ebit_t[i] is not None:
            tr = div_(tax_t[i], pretax_t[i]) if (tax_t[i] is not None and
                                                 pretax_t[i] and pretax_t[i] > 0) else 0.21
            tr = clamp(tr, 0.0, 0.5)
            nopat = ebit_t[i] * (1 - tr)
        ic = None
        if EQ is not None:
            ic = EQ + (r.get("totalDebt") or 0) - (r.get("cashSTI") or 0)
        put("roic_pct", i, div_(nopat, ic, 100), 3)
        rota_base = (None if TA is None else TA - (r.get("gwIntang") or 0))
        put("rota_pct", i, div_(ni_t[i], rota_base, 100), 3)

        # valuation
        put("pe_ttm", i, div_(mc, ni_t[i]), 3)
        put("ps_ttm", i, div_(mc, rev_t[i]), 3)
        put("pb", i, div_(mc, EQ), 3)
        put("ptb", i, div_(mc, tang_eq), 3)
        put("p_fcf_ttm", i, div_(mc, fcf_t[i]), 3)
        put("p_cfo_ttm", i, div_(mc, cfo_t[i]), 3)
        put("ev_ebitda_ttm", i, div_(evv, ebitda_t[i]), 3)
        put("ev_ebit_ttm", i, div_(evv, ebit_t[i]), 3)
        put("ev_sales_ttm", i, div_(evv, rev_t[i]), 3)
        put("ev_fcf_ttm", i, div_(evv, fcf_t[i]), 3)
        put("earnings_yield_pct", i, div_(ni_t[i], mc, 100), 3)
        put("fcf_yield_pct", i, div_(fcf_t[i], mc, 100), 3)
        dy = div_(-div_t[i] if div_t[i] is not None else None, mc, 100)
        by = div_(-buyb_t[i] if buyb_t[i] is not None else None, mc, 100)
        put("dividend_yield_pct", i, dy, 3)
        put("buyback_yield_pct", i, by, 3)
        if dy is not None or by is not None:
            put("shareholder_yield_pct", i, (dy or 0) + (by or 0), 3)
        gn = None
        if eps_t[i] is not None and EQ is not None and sh and eps_t[i] > 0:
            bvps_ = EQ / sh
            if bvps_ > 0:
                gn = math.sqrt(22.5 * eps_t[i] * bvps_)
        put("graham_number", i, gn, 3)

        # leverage / liquidity
        put("debt_to_equity", i, div_(r.get("totalDebt"), EQ), 3)
        put("debt_to_assets", i, div_(r.get("totalDebt"), TA), 3)
        put("equity_to_assets", i, div_(EQ, TA), 3)
        put("liab_to_assets", i, div_(TL, TA), 3)
        put("netdebt_to_ebitda_ttm", i,
            (None if (nd is None or ebitda_t[i] in (None, 0) or ebitda_t[i] <= 0)
             else nd / ebitda_t[i]), 3)
        put("interest_coverage_ttm", i,
            (None if (ebit_t[i] is None or intexp_t[i] in (None, 0) or intexp_t[i] <= 0)
             else ebit_t[i] / intexp_t[i]), 3)
        put("current_ratio", i, div_(r.get("totalCurrentAssets"),
                                     r.get("totalCurrentLiabilities")), 3)
        qa = (None if r.get("totalCurrentAssets") is None
              else r["totalCurrentAssets"] - (r.get("inventory") or 0))
        put("quick_ratio", i, div_(qa, r.get("totalCurrentLiabilities")), 3)
        put("cash_ratio", i, div_(r.get("cashSTI"),
                                  r.get("totalCurrentLiabilities")), 3)

        # efficiency / quality
        put("asset_turnover_ttm", i, div_(rev_t[i], ta_avg), 3)
        put("inventory_turnover_ttm", i, div_(cogs_t[i], r.get("inventory")), 3)
        put("dso_days", i, div_(r.get("receivables"), rev_t[i], 365), 2)
        dio = div_(r.get("inventory"), cogs_t[i], 365)
        dpo = div_(r.get("accountsPayable"), cogs_t[i], 365)
        put("dio_days", i, dio, 2)
        put("dpo_days", i, dpo, 2)
        dso = div_(r.get("receivables"), rev_t[i], 365)
        if dso is not None and dio is not None and dpo is not None:
            put("ccc_days", i, dso + dio - dpo, 2)
        put("income_quality", i,
            (None if (ni_t[i] in (None, 0) or ni_t[i] <= 0 or cfo_t[i] is None)
             else cfo_t[i] / ni_t[i]), 3)
        put("sbc_to_revenue_pct", i, div_(sbc_t[i], rev_t[i], 100), 3)
        put("capex_to_revenue_pct", i,
            div_(-capex_t[i] if capex_t[i] is not None else None, rev_t[i], 100), 3)
        put("rnd_to_revenue_pct", i, div_(rnd_t[i], rev_t[i], 100), 3)
        put("sga_to_revenue_pct", i, div_(sga_t[i], rev_t[i], 100), 3)
        put("effective_tax_rate_pct", i,
            (None if (tax_t[i] is None or pretax_t[i] in (None, 0) or pretax_t[i] <= 0)
             else clamp(tax_t[i] / pretax_t[i] * 100, -50, 100)), 3)
        if ni_t[i] is not None and cfo_t[i] is not None and cfi_t[i] is not None and TA:
            put("sloan_accruals_pct", i, (ni_t[i] - cfo_t[i] - cfi_t[i]) / TA * 100, 3)

        # per-share
        put("eps_ttm", i, div_(ni_t[i], sh, pos_denom=True), 4)
        put("fcf_ps_ttm", i, div_(fcf_t[i], sh), 4)
        put("cfo_ps_ttm", i, div_(cfo_t[i], sh), 4)
        put("revenue_ps_ttm", i, div_(rev_t[i], sh), 4)
        put("book_value_ps", i, div_(EQ, sh), 4)
        put("tangible_bv_ps", i, div_(tang_eq, sh), 4)
        put("dps_ttm", i, div_(-div_t[i] if div_t[i] is not None else None, sh), 4)
        put("cash_ps", i, div_(r.get("cashSTI"), sh), 4)
        put("payout_ratio_pct", i,
            (None if (div_t[i] is None or ni_t[i] in (None, 0) or ni_t[i] <= 0)
             else clamp(-div_t[i] / ni_t[i] * 100, 0, 400)), 3)
        sh_prev = (R[i - lb].get("shsDil") or R[i - lb].get("shs")) if i - lb >= 0 else None
        if sh and sh_prev:
            put("share_count_yoy_pct", i, (sh / sh_prev - 1) * 100, 3)

        # ── forensic scores ─────────────────────────────────────────────────
        # Altman Z (classic manufacturing form — fleet standard)
        if TA and TA > 0 and TL and TL > 0:
            z_parts = [
                1.2 * (wc / TA) if wc is not None else None,
                1.4 * (r["retainedEarnings"] / TA) if r.get("retainedEarnings") is not None else None,
                3.3 * (ebit_t[i] / TA) if ebit_t[i] is not None else None,
                0.6 * (mc / TL) if mc is not None else None,
                0.999 * (rev_t[i] / TA) if rev_t[i] is not None else None,
            ]
            if all(p is not None for p in z_parts):
                put("altman_z", i, sum(z_parts), 3)

        # Piotroski F (TTM vs prior-year TTM)
        j = i - lb
        if j >= 0:
            rp = R[j]
            checks, avail = 0, 0

            def chk(cond):
                nonlocal checks, avail
                if cond is None:
                    return
                avail += 1
                if cond:
                    checks += 1

            roa_now = div_(ni_t[i], TA)
            roa_prev = div_(ni_t[j], rp.get("totalAssets"))
            chk(None if roa_now is None else roa_now > 0)
            chk(None if cfo_t[i] is None else cfo_t[i] > 0)
            chk(None if (roa_now is None or roa_prev is None) else roa_now > roa_prev)
            chk(None if (cfo_t[i] is None or ni_t[i] is None) else cfo_t[i] > ni_t[i])
            lev_now = div_(r.get("longTermDebt"), TA)
            lev_prev = div_(rp.get("longTermDebt"), rp.get("totalAssets"))
            chk(None if (lev_now is None or lev_prev is None) else lev_now <= lev_prev)
            cr_now = div_(r.get("totalCurrentAssets"), r.get("totalCurrentLiabilities"))
            cr_prev = div_(rp.get("totalCurrentAssets"), rp.get("totalCurrentLiabilities"))
            chk(None if (cr_now is None or cr_prev is None) else cr_now > cr_prev)
            chk(None if (sh is None or sh_prev is None) else sh <= sh_prev * 1.005)
            gm_now = div_(gp_t[i], rev_t[i])
            gm_prev = div_(gp_t[j], rev_t[j])
            chk(None if (gm_now is None or gm_prev is None) else gm_now > gm_prev)
            at_now = div_(rev_t[i], TA)
            at_prev = div_(rev_t[j], rp.get("totalAssets"))
            chk(None if (at_now is None or at_prev is None) else at_now > at_prev)
            if avail >= 7:
                put("piotroski_f", i, checks, 0)

            # Beneish M-score (8-variable), TTM vs prior-year TTM
            def _ix(a, b):
                v = div_(a, b)
                return clamp(v, 0.05, 20.0)

            TAp = rp.get("totalAssets")
            dsri = _ix(div_(r.get("receivables"), rev_t[i]),
                       div_(rp.get("receivables"), rev_t[j]))
            gmi = _ix(gm_prev, gm_now)
            aq_now = (None if TA in (None, 0) else
                      1 - ((r.get("totalCurrentAssets") or 0) + (r.get("ppeNet") or 0)) / TA)
            aq_prev = (None if TAp in (None, 0) else
                       1 - ((rp.get("totalCurrentAssets") or 0) + (rp.get("ppeNet") or 0)) / TAp)
            aqi = _ix(aq_now, aq_prev) if (aq_now and aq_prev and aq_now > 0 and aq_prev > 0) else 1.0
            sgi = _ix(rev_t[i], rev_t[j])
            dep_now = da_t[i]
            dep_prev = da_t[j]
            depi = None
            if all(v is not None for v in (dep_now, dep_prev, r.get("ppeNet"), rp.get("ppeNet"))):
                a = dep_prev / (dep_prev + rp["ppeNet"]) if (dep_prev + rp["ppeNet"]) > 0 else None
                b2 = dep_now / (dep_now + r["ppeNet"]) if (dep_now + r["ppeNet"]) > 0 else None
                depi = _ix(a, b2)
            sgai = _ix(div_(sga_t[i], rev_t[i]), div_(sga_t[j], rev_t[j]))
            tata = (None if (ni_t[i] is None or cfo_t[i] is None or not TA)
                    else (ni_t[i] - cfo_t[i]) / TA)
            lv_now = (None if not TA else
                      ((r.get("longTermDebt") or 0) + (r.get("totalCurrentLiabilities") or 0)) / TA)
            lv_prev = (None if not TAp else
                       ((rp.get("longTermDebt") or 0) + (rp.get("totalCurrentLiabilities") or 0)) / TAp)
            lvgi = _ix(lv_now, lv_prev)
            need = (dsri, gmi, aqi, sgi, sgai, tata, lvgi)
            if all(v is not None for v in need):
                m = (-4.84 + 0.92 * dsri + 0.528 * gmi + 0.404 * aqi + 0.892 * sgi
                     + 0.115 * (depi if depi is not None else 1.0)
                     - 0.172 * sgai + 4.679 * clamp(tata, -1, 1) - 0.327 * lvgi)
                put("beneish_m", i, m, 3)

    # ── forecasts (history + future) ─────────────────────────────────────────
    EST_ALIASES = {
        "est_revenue_avg": ("revenueAvg", "estimatedRevenueAvg"),
        "est_revenue_low": ("revenueLow", "estimatedRevenueLow"),
        "est_revenue_high": ("revenueHigh", "estimatedRevenueHigh"),
        "est_eps_avg": ("epsAvg", "estimatedEpsAvg"),
        "est_eps_low": ("epsLow", "estimatedEpsLow"),
        "est_eps_high": ("epsHigh", "estimatedEpsHigh"),
        "est_ebitda_avg": ("ebitdaAvg", "estimatedEbitdaAvg"),
        "est_ebit_avg": ("ebitAvg", "estimatedEbitAvg"),
        "est_net_income_avg": ("netIncomeAvg", "estimatedNetIncomeAvg"),
        "est_sga_avg": ("sgaExpenseAvg", "estimatedSgaExpenseAvg"),
        "est_num_analysts": ("numAnalystsRevenue", "numberAnalystEstimatedRevenue",
                             "numberAnalystsEstimatedRevenue", "numAnalystsEps"),
    }
    est_sorted = sorted(
        [e for e in est_rows if str(e.get("date") or "")[:10]],
        key=lambda e: str(e["date"])[:10],
    )
    cutoff = (datetime.now(timezone.utc) - timedelta(days=3900)).strftime("%Y-%m-%d")
    for e in est_sorted:
        d = str(e["date"])[:10]
        if d < cutoff:
            continue
        for out_k, names in EST_ALIASES.items():
            v = g(e, *names)
            if v is not None:
                P.setdefault(out_k, []).append([d, rnd(v, 4)])

    for k in list(P.keys()):
        P[k].sort(key=lambda t: t[0])

    doc = {
        "ok": True,
        "engine": "fundamental-graphs",
        "version": "1.0.2",
        "marker": "FUNDGRAPH_V1_OPS3462",
        "symbol": sym,
        "period": period,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "profile": profile,
        "n_periods": min(n, MAX_Q if period == "quarter" else MAX_A),
        "points": P,
        "price": weekly_px,
        "sources": [
            "FMP /stable income-statement / balance-sheet-statement / cash-flow-statement",
            "FMP /stable analyst-estimates (Forecasts tab)",
            "FMP /stable historical-price-eod/light (mcap_t = close_t x diluted shares_t)",
            "Altman Z / Piotroski F / Beneish M / Sloan derived in-engine per period",
        ],
    }
    return doc


# ── cache + handler ──────────────────────────────────────────────────────────
def cache_key(sym, period):
    return f"{CACHE_PREFIX}{sym}_{period}.json"


def load_cache(sym, period):
    try:
        obj = _s3.get_object(Bucket=S3_BUCKET, Key=cache_key(sym, period))
        doc = json.loads(obj["Body"].read())
        ts = datetime.fromisoformat(doc["generated_at"].replace("Z", "+00:00"))
        if (datetime.now(timezone.utc) - ts).total_seconds() < CACHE_TTL_SEC:
            return doc
    except Exception:  # noqa: BLE001
        pass
    return None


def save_cache(doc):
    try:
        _s3.put_object(
            Bucket=S3_BUCKET,
            Key=cache_key(doc["symbol"], doc["period"]),
            Body=json.dumps(doc, separators=(",", ":")).encode(),
            ContentType="application/json",
            CacheControl="public, max-age=900",
        )
    except Exception as e:  # noqa: BLE001
        print(f"cache write failed: {e}")


def get_doc(sym, period, refresh=False):
    if not refresh:
        cached = load_cache(sym, period)
        if cached:
            cached["cached"] = True
            return cached
    doc = build_doc(sym, period)
    save_cache(doc)
    doc["cached"] = False
    return doc


def _resp(status, doc, headers_in):
    body = json.dumps(doc, separators=(",", ":"))
    hdrs = {
        "Content-Type": "application/json",
        "Cache-Control": "public, max-age=600",
        # NOTE: no CORS headers here — the Function URL's Cors config is the
        # single authority. Emitting ACAO from the function too produces
        # duplicate headers ("*, *") which browsers reject (ops 3464).
    }
    ae = ""
    if isinstance(headers_in, dict):
        low = {str(k).lower(): str(v) for k, v in headers_in.items()}
        ae = low.get("accept-encoding", "")
        if low.get("x-gz") == "1":
            ae = "gzip"
    if "gzip" in ae.lower() and len(body) > 1400:
        gz = gzip.compress(body.encode())
        hdrs["Content-Encoding"] = "gzip"
        return {"statusCode": status, "headers": hdrs,
                "body": base64.b64encode(gz).decode(), "isBase64Encoded": True}
    return {"statusCode": status, "headers": hdrs, "body": body}


def _valid_symbol(s):
    s = (s or "").strip().upper()
    if 0 < len(s) <= 12 and all(c.isalnum() or c in ".-^" for c in s):
        return s
    return None


def lambda_handler(event, context):  # noqa: ARG001
    event = event or {}
    if not FMP_KEY:
        return _resp(500, {"ok": False, "error": "FMP_KEY not set"}, {})

    # warm mode (Event invokes / ops)
    if isinstance(event, dict) and event.get("warm"):
        out = {}
        periods = event.get("periods") or ["quarter"]
        for s in event["warm"][:12]:
            sym = _valid_symbol(s)
            if not sym:
                continue
            for p in periods:
                if p not in ("quarter", "annual"):
                    continue
                try:
                    d = get_doc(sym, p, refresh=bool(event.get("refresh")))
                    out[f"{sym}_{p}"] = {"ok": True, "n": d.get("n_periods"),
                                         "keys": len(d.get("points", {}))}
                except Exception as e:  # noqa: BLE001
                    out[f"{sym}_{p}"] = {"ok": False, "error": str(e)[:180]}
        return {"ok": True, "warmed": out, "marker": "FUNDGRAPH_V1_OPS3462"}

    qp = event.get("queryStringParameters") or {}
    if not qp and event.get("rawQueryString"):
        qp = dict(urllib.parse.parse_qsl(event["rawQueryString"]))
    if not qp and event.get("symbol"):
        qp = {"symbol": event.get("symbol"), "period": event.get("period", "quarter"),
              "refresh": "1" if event.get("refresh") else ""}
    headers_in = event.get("headers") or {}

    sym = _valid_symbol(qp.get("symbol") or qp.get("s"))
    if not sym:
        return _resp(400, {"ok": False,
                           "error": "symbol required, e.g. ?symbol=AAPL&period=quarter",
                           "marker": "FUNDGRAPH_V1_OPS3462"}, headers_in)
    period = (qp.get("period") or "quarter").lower()
    if period == "ttm":
        period = "quarter"  # client derives TTM from quarters
    if period not in ("quarter", "annual"):
        return _resp(400, {"ok": False, "error": "period must be quarter|annual"},
                     headers_in)
    if str(qp.get("gz") or "") == "1":
        headers_in = dict(headers_in or {})
        headers_in["x-gz"] = "1"
    try:
        doc = get_doc(sym, period, refresh=str(qp.get("refresh") or "") in ("1", "true"))
        return _resp(200, doc, headers_in)
    except Exception as e:  # noqa: BLE001
        return _resp(502, {"ok": False, "symbol": sym, "period": period,
                           "error": str(e)[:300]}, headers_in)
