"""ops/838 - preflight for the next opportunity engine: an Index
Reconstitution / Forced-Flow Desk.

Thesis (the documented edge): index funds are price-insensitive forced
buyers and sellers. When FTSE Russell reconstitutes the Russell 1000 /
2000 at the end of June, every passive fund tracking those indices must
mechanically buy the additions and sell the deletions on the same day.
The methodology is transparent and rules-based - rank the eligible US
equity universe by total market cap on rank day; the cutoff between the
1000 and the 2000 is forecastable weeks ahead. Names crossing the
boundary upward (small-cap graduating to large-cap) or being added /
deleted are front-runnable. This is one of the best-documented market
anomalies and it is retail-accessible (you just buy the projected add).
The Russell 2026 reconstitution is effective end of June - the window
is open right now.

To build it well I need the FULL eligible US universe ranked by market
cap, not just the ~S&P names already in screener/data.json. This
preflight establishes the data foundation:
  1. how big / how complete screener/data.json's universe is
  2. whether FMP /stable/company-screener can deliver a 3000+ name US
     universe with market cap in a few paginated calls (the clean way)
  3. a short-interest endpoint probe, kept as a fallback lane
No engine is built here - this only proves the data path.

[re-dispatch: first push landed inside a merge commit so the run-ops
HEAD^..HEAD diff missed it; this clean tip commit re-arms the trigger.]
"""
import json, os, time, urllib.request, urllib.parse
from datetime import datetime, timezone
import boto3

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
report = {"ops": 838, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Preflight - Index Reconstitution / Forced-Flow Desk"}


def get(url, timeout=40):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


# --- 1. existing screener universe -------------------------------------
uni = {}
try:
    sd = json.loads(s3.get_object(
        Bucket=BUCKET, Key="screener/data.json")["Body"].read())
    stocks = sd.get("stocks") or []
    caps = [s.get("marketCap") for s in stocks
            if isinstance(s.get("marketCap"), (int, float))
            and s.get("marketCap") > 0]
    caps.sort(reverse=True)
    uni = {
        "n_stocks": len(stocks),
        "n_with_marketcap": len(caps),
        "largest_cap_bil": round(caps[0] / 1e9, 1) if caps else None,
        "smallest_cap_mil": round(caps[-1] / 1e6, 1) if caps else None,
        "cap_at_rank_1000_bil": (round(caps[999] / 1e9, 2)
                                 if len(caps) > 999 else None),
        "reaches_russell_boundary": len(caps) > 1000,
    }
except Exception as e:
    uni = {"error": str(e)[:200]}
report["screener_universe"] = uni

# --- 2. FMP /stable/company-screener as a full-universe source ---------
probes = {}
# pull a market-cap band; check page size + that marketCap is present
try:
    qs = urllib.parse.urlencode({
        "marketCapMoreThan": 200_000_000, "marketCapLowerThan": 50_000_000_000,
        "isEtf": "false", "isFund": "false", "isActivelyTrading": "true",
        "exchange": "NASDAQ", "limit": 1000, "apikey": FMP})
    st, body = get(f"https://financialmodelingprep.com/stable/"
                   f"company-screener?{qs}")
    rows = json.loads(body) if st == 200 else []
    sample = rows[0] if rows else {}
    probes["company_screener"] = {
        "status": st, "rows_returned": len(rows),
        "has_marketCap": "marketCap" in sample,
        "has_exchange": "exchange" in sample or "exchangeShortName" in sample,
        "sample_keys": sorted(sample.keys())[:25],
        "sample_row": {k: sample.get(k) for k in
                       ("symbol", "companyName", "marketCap",
                        "exchangeShortName", "sector", "country",
                        "isEtf", "price")},
    }
except Exception as e:
    probes["company_screener"] = {"error": str(e)[:200]}

# count of the investable US universe across the three major exchanges
try:
    total = 0
    by_exch = {}
    for exch in ("NYSE", "NASDAQ", "AMEX"):
        qs = urllib.parse.urlencode({
            "marketCapMoreThan": 30_000_000, "isEtf": "false",
            "isFund": "false", "isActivelyTrading": "true",
            "country": "US", "exchange": exch, "limit": 3000,
            "apikey": FMP})
        st, body = get(f"https://financialmodelingprep.com/stable/"
                       f"company-screener?{qs}", timeout=50)
        n = len(json.loads(body)) if st == 200 else 0
        by_exch[exch] = n
        total += n
        time.sleep(0.4)
    probes["us_universe_count"] = {
        "by_exchange": by_exch, "total": total,
        "enough_for_russell": total > 2500}
except Exception as e:
    probes["us_universe_count"] = {"error": str(e)[:200]}

# --- 3. short-interest fallback probe ----------------------------------
try:
    st, body = get(f"https://financialmodelingprep.com/stable/"
                   f"shares-float?symbol=AAPL&apikey={FMP}")
    j = json.loads(body) if st == 200 else None
    row = (j[0] if isinstance(j, list) and j else j) or {}
    probes["shares_float"] = {
        "status": st, "keys": sorted(row.keys()) if row else [],
        "has_short_data": any("short" in k.lower() for k in row)}
except Exception as e:
    probes["shares_float"] = {"error": str(e)[:200]}

report["fmp_probes"] = probes

# --- verdict / build guidance ------------------------------------------
cs = probes.get("company_screener", {})
uc = probes.get("us_universe_count", {})
data_ready = bool(cs.get("has_marketCap")) and bool(uc.get("enough_for_russell"))
report["build_guidance"] = {
    "data_ready_for_index_recon": data_ready,
    "universe_source": ("FMP /stable/company-screener, paginated by "
                        "exchange + market-cap band - returns marketCap "
                        "inline, no per-name profile calls needed"),
    "next_engine": ("justhodl-index-recon - Index Reconstitution / "
                    "Forced-Flow Desk: rank the full US universe by "
                    "market cap, project the Russell 1000/2000 boundary "
                    "and the FTSE banding zone, flag projected adds / "
                    "deletes / up-migrations, score each on the forced-"
                    "flow edge (passive AUM impact vs ADV, days-to-"
                    "absorb, distance into the band)."
                    if data_ready else
                    "FALL BACK to short-squeeze radar - see "
                    "shares_float probe for short-interest availability"),
}
report["verdict"] = (
    "DATA READY - build the Index Reconstitution / Forced-Flow Desk."
    if data_ready else
    "Index-recon data path incomplete - review fmp_probes, consider "
    "the short-squeeze fallback.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/838_index_recon_preflight.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/838_index_recon_preflight.json")
