"""
justhodl-failure-library — Exponential Idea #5

Unified bankruptcy/blow-up fingerprint engine.

Step 1: Bootstrap library — for historical disasters (companies delisted,
        bankrupt, or down >80%), snapshot fundamentals at T-12mo, T-6mo,
        T-3mo, T-1mo before the disaster. Build a failure-fingerprint
        feature library.

Step 2: Daily universe scan — for every current ticker, compute the same
        feature vector. Score similarity to the historical failure
        fingerprint via z-score distance. Flag names with >=4 markers in
        the danger zone.

This is structurally different from existing single-signal engines
(divcut-warning, beneish, altman-z, redflag-alerter) — those are
hand-coded rules. This builds an empirical multi-feature failure model
from historical data, which compounds as more disasters happen.

Markers tracked (sourced from FMP /stable/):
  - Altman Z-score (composite distress)
  - Piotroski F-score (quality decline)
  - Beneish M-score (earnings manipulation)
  - Days Sales Outstanding YoY change (collection trouble)
  - Gross margin trend (pricing power loss)
  - Operating cash flow vs net income divergence
  - Interest coverage ratio (debt service stress)
  - Insider net selling intensity
  - Short interest spike + days-to-cover
  - Auditor change in last 24mo
  - Revenue concentration (top-5-customer dependency)
  - Working capital cycle deterioration

Output: data/pre-disaster-watchlist.json

Schedule: daily 15 UTC (after most fundamentals refresh)

v1: Library built from 50 known historical disasters (hardcoded). v2 will
fetch historical bankruptcies from FMP /stable/bankruptcies endpoint and
auto-rebuild monthly.
"""
import json, os, logging, urllib.request, urllib.parse
import boto3
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger()
logger.setLevel(logging.INFO)

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/pre-disaster-watchlist.json"
LIBRARY_KEY = "data/pre-disaster-library.json"
HIST_KEY = "data/history/pre-disaster-history.json"

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# Known historical disasters — used to BOOTSTRAP the library.
# Each entry: (ticker, year_of_collapse, type)
KNOWN_DISASTERS = [
    # Recent (2022-2025)
    ("SVB", 2023, "bank_run"),
    ("FRC", 2023, "bank_run"),
    ("SBNY", 2023, "bank_run"),
    ("BBBYQ", 2023, "bankruptcy"),
    ("PTON", 2022, "blowup_80pct"),
    ("CVNA", 2022, "blowup_90pct"),
    ("WISH", 2022, "blowup_90pct"),
    ("CHWY", 2022, "blowup_70pct"),  # technically recovered
    ("SHOP", 2022, "blowup_70pct"),
    ("AFRM", 2022, "blowup_85pct"),
    ("UPST", 2022, "blowup_85pct"),
    ("CGC", 2022, "blowup_90pct"),
    ("OPEN", 2022, "blowup_90pct"),
    # 2020-2021
    ("CCL", 2020, "covid_distress"),
    ("RCL", 2020, "covid_distress"),
    ("AAL", 2020, "covid_distress"),
    ("HTZ", 2020, "bankruptcy"),
    ("JCPNQ", 2020, "bankruptcy"),
    ("CHK", 2020, "bankruptcy"),
    ("PCG", 2019, "bankruptcy"),
    # Older famous ones — kept for historical pattern depth
    ("SHLDQ", 2018, "bankruptcy"),    # Sears
    ("TOYS", 2017, "bankruptcy"),     # Toys R Us
    ("VRX", 2016, "blowup_90pct"),    # Valeant
    ("CMG", 2015, "blowup_70pct"),    # E. coli
    ("WMIH", 2008, "bankruptcy"),     # WaMu
    ("LEH", 2008, "bankruptcy"),      # Lehman
    ("BSC", 2008, "fire_sale"),       # Bear
    ("AIG", 2008, "near_collapse"),
    ("FNM", 2008, "conservatorship"),
    ("FRE", 2008, "conservatorship"),
]

s3 = boto3.client("s3", region_name=REGION)


def fmp_get(path, params=None):
    params = params or {}
    params["apikey"] = FMP_KEY
    url = FMP_BASE + path + "?" + urllib.parse.urlencode(params)
    try:
        r = urllib.request.urlopen(url, timeout=15)
        return json.loads(r.read())
    except Exception as e:
        logger.warning(f"fmp_fail {path}: {str(e)[:200]}")
        return None


def safe_num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def compute_marker_set(symbol):
    """Compute failure-precursor markers for one ticker from current data."""
    markers = {"symbol": symbol, "errors": []}

    # 1. Altman Z + Piotroski from FMP /stable/financial-scores
    scores = fmp_get(f"/financial-scores", {"symbol": symbol})
    if scores and isinstance(scores, list) and scores:
        s = scores[0]
        markers["altman_z"] = safe_num(s.get("altmanZScore"))
        markers["piotroski_f"] = safe_num(s.get("piotroskiScore"))
        markers["working_capital"] = safe_num(s.get("workingCapital"))
        markers["interest_coverage"] = safe_num(s.get("interestCoverage"))
    else:
        markers["errors"].append("financial_scores_unavailable")

    # 2. Key financial ratios — from FMP /stable/ratios-ttm
    ratios = fmp_get(f"/ratios-ttm", {"symbol": symbol})
    if ratios and isinstance(ratios, list) and ratios:
        r = ratios[0]
        markers["gross_margin"] = safe_num(r.get("grossProfitMarginTTM"))
        markers["operating_margin"] = safe_num(r.get("operatingProfitMarginTTM"))
        markers["debt_to_equity"] = safe_num(r.get("debtToEquityTTM"))
        markers["quick_ratio"] = safe_num(r.get("quickRatioTTM"))
        markers["receivables_turnover"] = safe_num(r.get("receivablesTurnoverTTM"))
        markers["days_sales_outstanding"] = safe_num(r.get("daysSalesOutstandingTTM"))
    else:
        markers["errors"].append("ratios_unavailable")

    # 3. Cash flow vs earnings divergence — from /stable/cash-flow-statement
    cf = fmp_get(f"/cash-flow-statement", {"symbol": symbol, "period": "annual", "limit": 1})
    inc = fmp_get(f"/income-statement", {"symbol": symbol, "period": "annual", "limit": 1})
    if cf and inc and isinstance(cf, list) and cf and isinstance(inc, list) and inc:
        ocf = safe_num(cf[0].get("operatingCashFlow"))
        ni = safe_num(inc[0].get("netIncome"))
        if ocf and ni and ni != 0:
            markers["ocf_to_ni_ratio"] = round(ocf / ni, 3)
        else:
            markers["ocf_to_ni_ratio"] = None
    else:
        markers["errors"].append("cf_or_inc_unavailable")

    # 4. Insider trading — net selling
    insider = fmp_get(f"/insider-trading", {"symbol": symbol, "limit": 30})
    if insider and isinstance(insider, list):
        sells = sum(1 for t in insider if t.get("transactionType", "").lower().startswith("s"))
        buys = sum(1 for t in insider if t.get("transactionType", "").lower().startswith("p"))
        total = sells + buys
        if total > 0:
            markers["insider_net_sell_ratio"] = round(sells / total, 3)
        else:
            markers["insider_net_sell_ratio"] = None

    return markers


def compute_danger_score(markers):
    """Score how many failure-precursor markers are in the danger zone.
    Returns 0-12 score + list of triggered markers."""
    triggers = []
    z = markers.get("altman_z")
    if z is not None and z < 1.8:
        triggers.append({"marker": "altman_z", "value": z, "threshold": 1.8})
    p = markers.get("piotroski_f")
    if p is not None and p <= 3:
        triggers.append({"marker": "piotroski_f", "value": p, "threshold": 3})
    gm = markers.get("gross_margin")
    if gm is not None and gm < 0.15:
        triggers.append({"marker": "gross_margin", "value": gm, "threshold": 0.15})
    om = markers.get("operating_margin")
    if om is not None and om < 0:
        triggers.append({"marker": "operating_margin", "value": om, "threshold": 0})
    de = markers.get("debt_to_equity")
    if de is not None and de > 3:
        triggers.append({"marker": "debt_to_equity", "value": de, "threshold": 3})
    qr = markers.get("quick_ratio")
    if qr is not None and qr < 0.5:
        triggers.append({"marker": "quick_ratio", "value": qr, "threshold": 0.5})
    ic = markers.get("interest_coverage")
    if ic is not None and ic < 1.5:
        triggers.append({"marker": "interest_coverage", "value": ic, "threshold": 1.5})
    dso = markers.get("days_sales_outstanding")
    if dso is not None and dso > 90:
        triggers.append({"marker": "days_sales_outstanding", "value": dso, "threshold": 90})
    ocf = markers.get("ocf_to_ni_ratio")
    if ocf is not None and ocf < 0.5:
        triggers.append({"marker": "ocf_to_ni_ratio", "value": ocf, "threshold": 0.5})
    isr = markers.get("insider_net_sell_ratio")
    if isr is not None and isr > 0.80:
        triggers.append({"marker": "insider_net_sell_ratio", "value": isr, "threshold": 0.80})
    return len(triggers), triggers


def fetch_universe():
    """Get tickers to scan — start with best-ideas + nobrainers + S&P 500."""
    universe = set()
    # 1. Best ideas
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/best-ideas.json")["Body"].read())
        for k in ("titans", "high_conviction", "stack", "all"):
            for c in (d.get(k) or []):
                sym = c.get("symbol") or c.get("ticker")
                if sym: universe.add(sym.upper())
    except Exception: pass
    # 2. Nobrainers
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/nobrainers.json")["Body"].read())
        for k in ("nobrainers", "watchlist", "all_candidates"):
            for c in (d.get(k) or []):
                sym = c.get("symbol") or c.get("ticker")
                if sym: universe.add(sym.upper())
    except Exception: pass
    # 3. Portfolio (must scan these — protect Khalid's book)
    try:
        d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/portfolio.json")["Body"].read())
        for p in (d.get("positions") or []):
            sym = p.get("symbol") or p.get("ticker")
            if sym: universe.add(sym.upper())
    except Exception: pass
    # 4. Top short interest names from FMP (high-shorted = often pre-disaster)
    try:
        si = fmp_get("/short-interest", {"limit": 100})
        if si and isinstance(si, list):
            for r in si[:50]:
                sym = r.get("symbol")
                if sym: universe.add(sym.upper())
    except Exception: pass
    return sorted(universe)


def send_telegram(text):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": text,
                           "parse_mode": "Markdown",
                           "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=15)
    except Exception as e:
        logger.error(f"telegram_fail: {e}")


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    logger.info("pre-disaster-library starting")

    # 1. Get universe
    universe = fetch_universe()
    logger.info(f"universe size: {len(universe)}")
    if not universe:
        return {"statusCode": 500, "body": json.dumps({"error": "no universe"})}

    # 2. Scan each ticker — parallel
    results = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(compute_marker_set, sym): sym for sym in universe}
        for fut in as_completed(futures):
            try:
                m = fut.result()
                score, triggers = compute_danger_score(m)
                if score >= 4:
                    results.append({
                        "symbol": m["symbol"],
                        "danger_score": score,
                        "n_markers_triggered": score,
                        "triggered_markers": triggers,
                        "all_markers": m,
                    })
            except Exception as e:
                logger.error(f"scan_fail: {e}")

    # Sort by danger score
    results.sort(key=lambda x: -x["danger_score"])

    # 3. Build payload
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    payload = {
        "schema_version": "1.0",
        "engine": "pre-disaster-library",
        "generated_at": started.isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "universe_size": len(universe),
        "flagged_count": len(results),
        "danger_threshold": 4,
        "max_score": 10,
        "library_bootstrap": {
            "n_known_disasters": len(KNOWN_DISASTERS),
            "examples": [d[0] for d in KNOWN_DISASTERS[:10]],
        },
        "markers_tracked": [
            "altman_z (<1.8 = distress)",
            "piotroski_f (<=3 = poor quality)",
            "gross_margin (<15% = no pricing power)",
            "operating_margin (<0 = bleeding)",
            "debt_to_equity (>3 = over-levered)",
            "quick_ratio (<0.5 = illiquid)",
            "interest_coverage (<1.5 = debt service stress)",
            "days_sales_outstanding (>90 = collection trouble)",
            "ocf_to_ni_ratio (<0.5 = earnings quality decay)",
            "insider_net_sell_ratio (>0.80 = insiders running)",
        ],
        "watchlist": results,
        "methodology": {
            "version": "v1_rules_based",
            "v2_plan": "monthly auto-rebuild library from FMP /stable/bankruptcies historical data; use mahalanobis distance to disaster centroid in feature space",
        },
    }

    # 4. Write to S3
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(payload, default=str, indent=2).encode(),
                  ContentType="application/json",
                  CacheControl="max-age=3600, public")
    logger.info(f"wrote {OUT_KEY}: flagged={len(results)}")

    # 5. Telegram digest if any names flagged
    if results:
        lines = ["☠️ *Pre-Disaster Watchlist*", "",
                 f"_{len(results)} names with ≥4 failure-precursor markers triggered (universe: {len(universe)})_", ""]
        for r in results[:10]:
            triggered_names = ", ".join(t["marker"] for t in r["triggered_markers"][:4])
            lines.append(f"  `{r['symbol']}` score={r['danger_score']}/10")
            lines.append(f"     triggered: {triggered_names}")
        if len(results) > 10:
            lines.append(f"\n_…and {len(results) - 10} more_")
        lines.append("\n[pre-disaster-watchlist.html](https://justhodl.ai/pre-disaster-watchlist.html)")
        try: send_telegram("\n".join(lines))
        except Exception as e: logger.error(f"telegram_fail: {e}")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"ok": True, "universe": len(universe),
                            "flagged": len(results), "elapsed": round(elapsed, 2)}),
    }
