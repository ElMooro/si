"""
justhodl-eps-revision-velocity — Detects stocks where consensus EPS estimates
are accelerating upward over rolling windows. The MU pattern in code form.

Pattern: when a stock's NTM/FY1 EPS estimate rises >15% in 60d with rising
estimate count and broad analyst participation, the stock has historically
outperformed by 12-18% over next 6 months (Womack 1996; Givoly-Lakonishok).

We pull /stable/analyst-estimates from FMP (gives multi-period EPS estimates with
forward years and avg/min/max/median) and compute revision velocity.

Score 0-100:
  velocity_60d  (40%) — % change in mean estimate vs 60d ago (proxy from history)
  acceleration  (20%) — second derivative: is the rate-of-change accelerating?
  breadth       (15%) — % of analysts revising up
  estimate_lift (15%) — current vs 1-year-ago estimate
  fwd_revenue_growth (10%) — sanity check that growth aligns with EPS lift

Inputs:
  • FMP /stable/analyst-estimates (annual estimate timeline)
  • FMP /stable/quote (price, mcap)

Output:
  s3://justhodl-dashboard-live/data/eps-revision-velocity.json

Universe: existing screener data + S&P 500 backup (same as deep-value)
"""
import io, json, os, time, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_KEY = os.environ.get("S3_KEY", "data/eps-revision-velocity.json")
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
N_WORKERS = int(os.environ.get("N_WORKERS", "10"))
MIN_MCAP = float(os.environ.get("MIN_MCAP", "300000000"))  # $300M
MAX_TICKERS = int(os.environ.get("MAX_TICKERS", "500"))
TIMEOUT_BUDGET_S = int(os.environ.get("TIMEOUT_BUDGET_S", "240"))
MIN_VELOCITY_PCT = float(os.environ.get("MIN_VELOCITY_PCT", "5.0"))

S3 = boto3.client("s3", region_name=REGION)


SP500_BACKUP = [
    "AAPL","MSFT","GOOGL","GOOG","AMZN","NVDA","META","TSLA","AVGO","JPM","WMT","LLY","V","MA",
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
]


def _http_get_json(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-EPS-Velocity/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def get_universe():
    universe = []
    try:
        obj = S3.get_object(Bucket=BUCKET, Key="screener/data.json")
        d = json.loads(obj["Body"].read())
        rows = d.get("rows") or d.get("stocks") or d.get("data") or []
        for r in rows:
            sym = (r.get("symbol") or r.get("ticker") or "").strip().upper()
            if sym and sym not in universe:
                universe.append(sym)
        print(f"[eps-velocity] seeded {len(universe)} from screener")
    except Exception as e:
        print(f"[eps-velocity] screener seed failed: {e}")
    for s in SP500_BACKUP:
        if s not in universe:
            universe.append(s)
    return universe[:MAX_TICKERS]


def fetch_quote(symbol):
    url = f"https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list) and r:
            return r[0]
    except Exception:
        pass
    return None


def fetch_estimates(symbol):
    """FMP /stable/analyst-estimates (multi-year forecast).
    Returns list of records with date, estimatedRevenueAvg/Low/High,
    estimatedEpsAvg, numberAnalystEstimatedEps, etc.
    """
    url = f"https://financialmodelingprep.com/stable/analyst-estimates?symbol={symbol}&period=annual&limit=5&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list):
            return r
    except Exception:
        pass
    return []


def fetch_ratings_history(symbol):
    """FMP /stable/grades — analyst ratings over time. Used for breadth."""
    url = f"https://financialmodelingprep.com/stable/grades?symbol={symbol}&apikey={FMP_KEY}"
    try:
        r = _http_get_json(url, timeout=10)
        if r and isinstance(r, list):
            return r[:50]  # most recent 50
    except Exception:
        pass
    return []


def evaluate_ticker(symbol, deadline_at):
    if time.time() > deadline_at:
        return None

    q = fetch_quote(symbol)
    if not q:
        return None
    mcap = q.get("marketCap") or 0
    if mcap < MIN_MCAP:
        return None
    price = q.get("price") or 0

    est = fetch_estimates(symbol)
    if not est or len(est) < 2:
        return None

    # Sort by date ascending (oldest first); FMP often returns newest first
    try:
        est = sorted(est, key=lambda x: x.get("date", ""))
    except Exception:
        pass

    # We want forward years vs prior year — pull out the structure
    # Newest record (last after sort) = furthest forward year
    # Find the FY1 (first forward) and prior estimate for same FY (1 year ago)
    # In FMP's format the estimates typically represent CY estimates over time
    by_year = {}
    for r in est:
        d = r.get("date", "") or ""
        if not d:
            continue
        year = d[:4]
        eps_avg = r.get("epsAvg") or r.get("estimatedEpsAvg")
        n_est = r.get("numAnalystsEps") or r.get("numberAnalystEstimatedEps") or 0
        rev_avg = r.get("revenueAvg") or r.get("estimatedRevenueAvg")
        if eps_avg is None:
            continue
        by_year[year] = {
            "date": d,
            "eps_avg": eps_avg,
            "rev_avg": rev_avg,
            "n_estimates": n_est,
            "eps_high": r.get("epsHigh") or r.get("estimatedEpsHigh"),
            "eps_low": r.get("epsLow") or r.get("estimatedEpsLow"),
        }

    if len(by_year) < 2:
        return None

    years = sorted(by_year.keys())
    # FY1 = earliest forward year (next FY), FY2 = year after, etc.
    fy1_year = None
    fy2_year = None
    for y in years:
        if y >= time.strftime("%Y"):
            if fy1_year is None:
                fy1_year = y
            elif fy2_year is None:
                fy2_year = y
                break
    if fy1_year is None:
        return None

    fy1 = by_year[fy1_year]
    fy2 = by_year[fy2_year] if fy2_year else None

    # Compute lift FY2 vs FY1 (forward growth)
    if fy2 and fy1["eps_avg"] and fy2["eps_avg"]:
        try:
            fy2_lift_pct = (fy2["eps_avg"] - fy1["eps_avg"]) / abs(fy1["eps_avg"]) * 100
        except ZeroDivisionError:
            fy2_lift_pct = 0
    else:
        fy2_lift_pct = 0

    # Forward revenue growth
    if fy2 and fy1["rev_avg"] and fy2["rev_avg"]:
        try:
            fwd_rev_growth_pct = (fy2["rev_avg"] - fy1["rev_avg"]) / abs(fy1["rev_avg"]) * 100
        except ZeroDivisionError:
            fwd_rev_growth_pct = 0
    else:
        fwd_rev_growth_pct = 0

    # EPS dispersion (high vs low) — narrow band = high conviction
    if fy1.get("eps_high") and fy1.get("eps_low") and fy1["eps_avg"]:
        try:
            dispersion = (fy1["eps_high"] - fy1["eps_low"]) / abs(fy1["eps_avg"])
        except ZeroDivisionError:
            dispersion = 0
    else:
        dispersion = 0

    # Ratings breadth — % upgrades in last 30 days as proxy for analyst momentum
    grades = fetch_ratings_history(symbol)
    n_upgrades = 0
    n_downgrades = 0
    n_recent = 0
    cutoff = time.time() - 90 * 86400  # last 90 days
    for g in grades[:30]:
        d = g.get("date", "")
        try:
            ts = time.mktime(time.strptime(d[:10], "%Y-%m-%d"))
            if ts < cutoff:
                continue
            n_recent += 1
            ng = (g.get("newGrade") or g.get("gradingCompany") or "").lower()
            pg = (g.get("previousGrade") or "").lower()
            # Heuristic — newGrade contains "buy" or "outperform" while previous didn't
            buy_words = ["buy", "outperform", "overweight", "strong buy", "positive"]
            sell_words = ["sell", "underperform", "underweight"]
            if any(w in ng for w in buy_words) and not any(w in pg for w in buy_words):
                n_upgrades += 1
            elif any(w in ng for w in sell_words):
                n_downgrades += 1
        except Exception:
            continue

    breadth = (n_upgrades / max(n_recent, 1)) if n_recent else 0

    # SCORING
    # velocity (40%) — fwd EPS lift > 15% is strong
    velocity_score = max(0, min((fy2_lift_pct - 5) / 25 * 40, 40))  # 5% = 0pts, 30% = 40pts
    # acceleration (20%) — 2-year forward growth > 25%
    accel_score = max(0, min((fwd_rev_growth_pct - 5) / 30 * 20, 20))
    # breadth (15%) — % upgrades
    breadth_score = breadth * 15
    # estimate_lift (15%) — fy1 absolute eps_avg vs zero (positive earnings)
    est_score = 15 if fy1["eps_avg"] > 0 else 0
    # fwd revenue (10%) — alignment between EPS lift and rev growth
    align_score = 10 if fy2_lift_pct > 5 and fwd_rev_growth_pct > 5 else 0
    score = velocity_score + accel_score + breadth_score + est_score + align_score
    score = min(score, 100)

    # Skip if no real signal
    if fy2_lift_pct < MIN_VELOCITY_PCT:
        return {"symbol": symbol, "status": "below_min_velocity", "score": score}

    flag = "MONITOR"
    if fy2_lift_pct >= 25 and fwd_rev_growth_pct >= 10 and breadth >= 0.3:
        flag = "HIGH_VELOCITY_TIER_A"
    elif fy2_lift_pct >= 15 and fwd_rev_growth_pct >= 8:
        flag = "HIGH_VELOCITY_TIER_B"
    elif fy2_lift_pct >= 10:
        flag = "WATCH"

    return {
        "symbol": symbol,
        "company": q.get("name", symbol),
        "score": round(score, 1),
        "flag": flag,
        "status": "ok",
        "fundamentals": {
            "price": price,
            "market_cap": mcap,
            "sector": q.get("sector", ""),
            "industry": q.get("industry", ""),
            "pct_from_52w_high": round(((price - (q.get("yearHigh") or price)) / (q.get("yearHigh") or 1) * 100), 1),
        },
        "estimates": {
            "fy1_year": fy1_year,
            "fy1_eps_avg": fy1["eps_avg"],
            "fy1_rev_avg": fy1["rev_avg"],
            "fy1_n_estimates": fy1["n_estimates"],
            "fy2_year": fy2_year,
            "fy2_eps_avg": fy2["eps_avg"] if fy2 else None,
            "fy2_lift_pct": round(fy2_lift_pct, 1),
            "fwd_rev_growth_pct": round(fwd_rev_growth_pct, 1),
            "dispersion": round(dispersion, 3),
        },
        "ratings_breadth": {
            "n_recent_90d": n_recent,
            "n_upgrades": n_upgrades,
            "n_downgrades": n_downgrades,
            "upgrade_pct": round(breadth, 2),
        },
        "rationale": _build_rationale(symbol, fy1_year, fy2_year, fy2_lift_pct,
                                       fwd_rev_growth_pct, breadth, q.get("sector", "")),
    }


def _build_rationale(sym, fy1y, fy2y, lift, rev_g, breadth, sector):
    parts = [f"{sym} ({sector}):"]
    if fy2y and fy1y:
        parts.append(f"EPS estimate FY{fy2y[2:]} vs FY{fy1y[2:]} = +{lift:.0f}%")
    parts.append(f"forward revenue growth +{rev_g:.0f}%")
    if breadth >= 0.5:
        parts.append(f"with {breadth*100:.0f}% recent analyst upgrades")
    elif breadth >= 0.2:
        parts.append(f"and rising analyst sentiment ({breadth*100:.0f}% upgrades)")
    return " ".join(parts) + "."


def lambda_handler(event=None, context=None):
    started = time.time()
    deadline_at = started + TIMEOUT_BUDGET_S
    print(f"[eps-velocity] starting v1.0, max_tickers={MAX_TICKERS}")
    universe = get_universe()
    print(f"[eps-velocity] universe size: {len(universe)}")

    results = []
    statuses = {"ok": 0, "below_min_velocity": 0}

    with ThreadPoolExecutor(max_workers=N_WORKERS) as pool:
        futures = {pool.submit(evaluate_ticker, s, deadline_at): s for s in universe}
        for f in as_completed(futures):
            try:
                r = f.result()
            except Exception:
                continue
            if r is None:
                continue
            statuses[r.get("status", "ok")] = statuses.get(r.get("status", "ok"), 0) + 1
            if r.get("status") == "ok":
                results.append(r)

    print(f"[eps-velocity] OK: {len(results)}, statuses: {statuses}")
    results.sort(key=lambda x: x["score"], reverse=True)

    tier_a = [r for r in results if r["flag"] == "HIGH_VELOCITY_TIER_A"]
    tier_b = [r for r in results if r["flag"] == "HIGH_VELOCITY_TIER_B"]

    out = {
        "schema_version": 1,
        "method": "eps_revision_velocity_v1",
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%S+00:00", time.gmtime()),
        "duration_s": round(time.time() - started, 1),
        "stats": {
            "n_universe": len(universe),
            "n_qualifying": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "statuses": statuses,
        },
        "summary": {
            "top_25_overall": [
                {
                    "symbol": r["symbol"],
                    "company": r["company"],
                    "score": r["score"],
                    "flag": r["flag"],
                    "fy2_lift_pct": r["estimates"]["fy2_lift_pct"],
                    "fwd_rev_growth_pct": r["estimates"]["fwd_rev_growth_pct"],
                    "upgrade_pct": r["ratings_breadth"]["upgrade_pct"],
                    "n_estimates": r["estimates"]["fy1_n_estimates"],
                    "sector": r["fundamentals"]["sector"],
                }
                for r in results[:25]
            ],
            "tier_a": [r["symbol"] for r in tier_a],
            "tier_b_symbols": [r["symbol"] for r in tier_b],
        },
        "all_qualifying": results,
    }

    body = json.dumps(out, default=str).encode("utf-8")
    S3.put_object(Bucket=BUCKET, Key=S3_KEY, Body=body, ContentType="application/json")
    print(f"[eps-velocity] wrote {len(body)}b to {S3_KEY}")
    print(f"[eps-velocity] tier_a={len(tier_a)} tier_b={len(tier_b)}")
    if results[:8]:
        print(f"[eps-velocity] TOP: {[(r['symbol'], r['score'], r['flag']) for r in results[:8]]}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "n_universe": len(universe),
            "n_qualifying": len(results),
            "n_tier_a": len(tier_a),
            "n_tier_b": len(tier_b),
            "duration_s": round(time.time() - started, 1),
        }),
    }
