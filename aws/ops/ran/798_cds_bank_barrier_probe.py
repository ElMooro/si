"""ops/798 — diagnose the cds-monitor bank false-alarm.

CreditGrades is showing every G-SIB at distance-to-default ~1-2 (a false
distress signal). The suspect: FMP's `totalDebt` for banks folds in
customer deposits, which are NOT run-prone default-triggering debt. This
probe dumps the real FMP balance-sheet liability fields for five major
banks and, for each candidate default-barrier definition, computes what
distance-to-default and synthetic 5Y CDS the CreditGrades model would
produce — so the fix uses the structurally-correct liability figure, not
a guessed fudge factor.

No deploy — pure diagnostic. Reads FMP only.
"""
import json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# CreditGrades standard set — must match the engine exactly.
CG_LBAR, CG_LAMBDA, CG_R, CG_T = 0.50, 0.30, 0.50, 5.0
BANKS = {"JPM": "JPMorgan", "BAC": "Bank of America", "C": "Citigroup",
         "GS": "Goldman Sachs", "DB": "Deutsche Bank"}
# rough real-world 5Y CDS (bp) for sanity comparison only — not used in math
REAL_CDS_HINT = {"JPM": 42, "BAC": 48, "C": 60, "GS": 58, "DB": 100}


def _get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "jh-probe/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def fmp(path, params):
    p = {**params, "apikey": FMP_KEY}
    url = (f"https://financialmodelingprep.com/stable/{path}"
           f"?{urllib.parse.urlencode(p)}")
    return json.loads(_get(url))


def fred_latest(series_id):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           "&sort_order=desc&limit=12")
    d = json.loads(_get(url))
    for o in d.get("observations", []):
        v = o.get("value")
        if v not in (None, ".", ""):
            try:
                return float(v)
            except ValueError:
                pass
    return None


def ncdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def credit_grades(S, sigma_E, debt_per_share, r,
                  t=CG_T, lbar=CG_LBAR, lam=CG_LAMBDA, recovery=CG_R):
    """Exact replica of the engine's CreditGrades pricing."""
    if S <= 0 or sigma_E <= 0 or debt_per_share <= 0:
        return None
    barrier = lbar * debt_per_share
    if barrier <= 0:
        return None
    sigma = sigma_E * S / (S + barrier)
    if sigma <= 1e-5:
        return None
    d = (S + barrier) / barrier * math.exp(lam * lam)
    d = max(d, 1.0001)
    lnd = math.log(d)
    a2 = sigma * sigma * t + lam * lam
    a = math.sqrt(a2)
    p_t = ncdf(-a / 2 + lnd / a) - d * ncdf(-a / 2 - lnd / a)
    p_t = min(max(p_t, 1e-6), 0.999999)
    dd = lnd / a - a / 2.0
    hazard = -math.log(p_t) / t
    spread_bp = hazard * (1.0 - recovery) * 1e4
    return {"dd": round(dd, 2), "cds_bp": round(spread_bp, 1),
            "asset_vol_pct": round(sigma * 100, 1),
            "pd_5y_pct": round((1 - p_t) * 100, 2)}


def equity_vol(prices):
    px = [p for p in prices if p and p > 0]
    if len(px) < 30:
        return None
    rets = [math.log(px[i] / px[i + 1]) for i in range(len(px) - 1)]
    n = len(rets)
    m = sum(rets) / n
    var = sum((x - m) ** 2 for x in rets) / (n - 1)
    return math.sqrt(var) * math.sqrt(252.0)


report = {"ops": 798, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe FMP bank balance sheets — fix cds-monitor "
                     "false alarm"}

r = fred_latest("DGS1")
r = (r / 100.0) if r is not None else 0.04
report["risk_free_1y"] = r

# liability-ish fields worth inspecting on a bank balance sheet
LIAB_FIELDS = ["totalDebt", "longTermDebt", "shortTermDebt",
               "totalLiabilities", "totalCurrentLiabilities",
               "otherCurrentLiabilities", "otherLiabilities",
               "deposits", "totalDeposits", "netDebt", "capitalLeaseObligations",
               "totalNonCurrentLiabilities", "accountPayables"]

results = {}
for tk, nm in BANKS.items():
    row = {"name": nm}
    try:
        q = fmp("quote", {"symbol": tk})
        q = q[0] if isinstance(q, list) and q else (q or {})
        price = q.get("price")
        mcap = q.get("marketCap")
        row["price"] = price
        row["market_cap_bn"] = round(mcap / 1e9, 1) if mcap else None
        shares = mcap / price if (price and mcap) else None
        row["shares_bn"] = round(shares / 1e9, 2) if shares else None

        bs = fmp("balance-sheet-statement", {"symbol": tk, "limit": 1})
        bs = bs[0] if isinstance(bs, list) and bs else (bs or {})
        row["balance_sheet_date"] = bs.get("date")
        row["bs_all_keys"] = sorted(bs.keys())
        row["liabilities"] = {k: bs.get(k) for k in LIAB_FIELDS
                              if bs.get(k) is not None}

        hp = fmp("historical-price-eod/light", {"symbol": tk})
        rows = hp if isinstance(hp, list) else (hp or {}).get("historical",
                                                              [])
        prices = [rw.get("price") or rw.get("close")
                  for rw in rows if isinstance(rw, dict)]
        sigma_E = equity_vol(prices)
        row["equity_vol_pct"] = round(sigma_E * 100, 1) if sigma_E else None

        # ── candidate default-barrier debt definitions ──
        ltd = bs.get("longTermDebt") or 0
        std = bs.get("shortTermDebt") or 0
        tot_debt = bs.get("totalDebt") or 0
        tot_liab = bs.get("totalLiabilities") or 0
        deposits = (bs.get("deposits") or bs.get("totalDeposits") or 0)

        candidates = {
            "A_totalDebt_current": tot_debt,
            "B_longTermDebt": ltd,
            "C_LT_plus_ST": ltd + std,
            "D_issued_plus_15pct_deposits": (
                ltd + std + 0.15 * deposits) if deposits else None,
            "E_22pct_totalLiabilities": (
                0.22 * tot_liab) if tot_liab else None,
            "F_issued_plus_20pct_noncore": (
                ltd + std + 0.20 * max(tot_liab - ltd - std - (mcap or 0),
                                       0)) if tot_liab else None,
        }
        priced = {}
        if price and shares and sigma_E:
            for label, debt in candidates.items():
                if not debt or debt <= 0:
                    priced[label] = None
                    continue
                dps = debt / shares
                cg = credit_grades(float(price), sigma_E, dps, r)
                priced[label] = {
                    "barrier_debt_bn": round(debt / 1e9, 1),
                    **(cg or {"error": "solve failed"}),
                }
        row["barrier_tests"] = priced
        row["real_cds_hint_bp"] = REAL_CDS_HINT.get(tk)
    except Exception as e:
        row["error"] = f"{type(e).__name__}: {str(e)[:160]}"
    results[tk] = row

report["banks"] = results
report["note"] = ("For each bank compare barrier_tests[*].cds_bp against "
                   "real_cds_hint_bp. The definition whose CDS lands "
                   "closest to the hints across all five banks is the "
                   "structurally-correct bank barrier.")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/798_cds_bank_barrier_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/798_cds_bank_barrier_probe.json")
