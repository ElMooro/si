"""ops/801 — probe FMP bank balance-sheet structure for the cds-monitor
bank-barrier fix.

cds-monitor's CreditGrades model uses FMP `totalDebt` as the structural
default barrier. For banks that overstates default risk badly — banks are
structurally leveraged and most of their debt is stable deposit / secured
funding, not run-prone default-triggering debt. Result: every G-SIB prices
at distance-to-default ~1-2 and the alarm board false-fires.

This probe pulls the real balance-sheet fields for the 12-bank universe so
the fix uses the right default-point definition (CDS references the senior
bond stack), not a guessed fudge factor. It also snapshots the current live
cds-monitor.json so the before/after is on record. No deploy — read only.
"""
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3", region_name="us-east-1")
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BANKS = {"JPM": "JPMorgan Chase", "BAC": "Bank of America", "C": "Citigroup",
         "WFC": "Wells Fargo", "GS": "Goldman Sachs", "MS": "Morgan Stanley",
         "USB": "U.S. Bancorp", "PNC": "PNC Financial", "DB": "Deutsche Bank",
         "UBS": "UBS Group", "BCS": "Barclays", "HSBC": "HSBC Holdings"}

report = {"ops": 801, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe FMP bank balance sheets for cds-monitor "
                     "barrier fix"}


def _get(url, timeout=25):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-ops801/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(1.0 * (attempt + 1))
    raise last or RuntimeError("fetch failed")


def fmp(path, params):
    p = {**params, "apikey": FMP_KEY}
    url = (f"https://financialmodelingprep.com/stable/{path}"
           f"?{urllib.parse.urlencode(p)}")
    return json.loads(_get(url))


# ── current live cds-monitor.json snapshot ──
try:
    cm = json.loads(s3.get_object(Bucket="justhodl-dashboard-live",
                    Key="data/cds-monitor.json")["Body"].read())
    report["live_cds_monitor"] = {
        "generated_at": cm.get("generated_at"),
        "schema": cm.get("schema_version"),
        "composite": (cm.get("global_credit_stress") or {}).get("score_0_100"),
        "regime": (cm.get("global_credit_stress") or {}).get("regime"),
        "alarm_status": (cm.get("alarm_board") or {}).get("status"),
        "n_alarms": (cm.get("alarm_board") or {}).get("n_active"),
        "alarms": [f"{a.get('level')}: {a.get('signal')}"
                   for a in (cm.get("alarm_board") or {}).get("alarms", [])],
        "banks": [{"t": b.get("ticker"), "cds_bp": b.get("synthetic_cds_bp"),
                   "dd": b.get("distance_to_default"),
                   "regime": b.get("regime")}
                  for b in (cm.get("banks") or [])],
    }
except Exception as e:
    report["live_cds_monitor"] = {"error": str(e)[:200]}

# ── balance-sheet probe ──
FIELDS = ["totalDebt", "longTermDebt", "shortTermDebt", "totalLiabilities",
          "totalAssets", "totalStockholdersEquity", "totalEquity",
          "totalCurrentLiabilities", "otherLiabilities", "netDebt",
          "cashAndCashEquivalents", "deposits", "depositLiabilities"]
rows = []
bs_keys_sample = None
for tk, nm in BANKS.items():
    rec = {"ticker": tk, "name": nm}
    try:
        q = fmp("quote", {"symbol": tk})
        q = q[0] if isinstance(q, list) and q else (q or {})
        price = q.get("price")
        mcap = q.get("marketCap")
        rec["price"] = price
        rec["market_cap_bn"] = round(mcap / 1e9, 1) if mcap else None
        shares = (mcap / price) if (price and mcap and price > 0) else None
        rec["shares_bn"] = round(shares / 1e9, 3) if shares else None

        bs = fmp("balance-sheet-statement", {"symbol": tk, "limit": 1})
        bs = bs[0] if isinstance(bs, list) and bs else (bs or {})
        if bs_keys_sample is None and bs:
            bs_keys_sample = sorted(bs.keys())
        for f in FIELDS:
            v = bs.get(f)
            rec[f] = round(v / 1e9, 1) if isinstance(v, (int, float)) else v
        rec["fiscal_date"] = bs.get("date") or bs.get("fillingDate")

        td = bs.get("totalDebt") or 0
        ltd = bs.get("longTermDebt") or 0
        if shares and price and price > 0:
            rec["dps_totalDebt"] = round(td / shares, 2)
            rec["dps_longTermDebt"] = round(ltd / shares, 2)
            rec["barrier_S_ratio_totalDebt"] = (
                round(0.50 * td / shares / price, 3))
            rec["barrier_S_ratio_longTermDebt"] = (
                round(0.50 * ltd / shares / price, 3))
        rec["ltd_to_totalDebt"] = (round(ltd / td, 3)
                                   if td else None)
    except Exception as e:
        rec["error"] = str(e)[:120]
    rows.append(rec)
    time.sleep(0.2)

report["balance_sheet_keys_available"] = bs_keys_sample
report["banks"] = rows

# quick read: how leveraged does each barrier choice make the universe
ratios_td = [r.get("barrier_S_ratio_totalDebt") for r in rows
             if isinstance(r.get("barrier_S_ratio_totalDebt"), (int, float))]
ratios_ltd = [r.get("barrier_S_ratio_longTermDebt") for r in rows
              if isinstance(r.get("barrier_S_ratio_longTermDebt"),
                            (int, float))]
report["summary"] = {
    "barrier_S_ratio_totalDebt_range": ([min(ratios_td), max(ratios_td)]
                                        if ratios_td else None),
    "barrier_S_ratio_longTermDebt_range": ([min(ratios_ltd), max(ratios_ltd)]
                                           if ratios_ltd else None),
    "note": "barrier/S ratio drives the CreditGrades distance-to-default; "
            "for healthy IG names it should sit well below 1 (a smaller "
            "barrier relative to share price = a safer firm).",
}

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/801_bank_balance_sheet_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/801_bank_balance_sheet_probe.json")
