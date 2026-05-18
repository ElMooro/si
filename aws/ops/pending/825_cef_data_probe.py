"""ops/825 - definitive FMP closed-end-fund (CEF) data probe.

The opportunity stack has ZERO fund-level coverage. The genuine institutional
gap is a discount-to-NAV engine (Saba / Bulldog style): CEFs have a fixed
share count, so price drifts to wide premiums/discounts vs net asset value -
and that gap mean-reverts. ops 816's CEF probe never produced a report, so
the build question is still open: does FMP serve a usable, fresh NAV for
real closed-end funds? This probes a 12-name CEF basket across every
candidate endpoint and returns a hard verdict so the engine is built on
real fields, not assumptions (audit-before-build doctrine).
"""
import json, urllib.request, urllib.error
from datetime import datetime, timezone

FMP = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
BASE = "https://financialmodelingprep.com/stable"

# 12 real, liquid CEFs across categories (equity / bond / credit / sector)
CEFS = {
    "ADX": "Adams Diversified Equity (equity)",
    "USA": "Liberty All-Star Equity (equity)",
    "RVT": "Royce Value Trust (small-cap equity)",
    "PDI": "PIMCO Dynamic Income (multisector bond)",
    "PTY": "PIMCO Corp & Income Opportunity (bond)",
    "PDO": "PIMCO Dynamic Income Opportunities (bond)",
    "GOF": "Guggenheim Strategic Opportunities (multi-asset)",
    "ECC": "Eagle Point Credit (CLO equity)",
    "UTF": "Cohen & Steers Infrastructure (infra)",
    "BST": "BlackRock Science & Technology (tech)",
    "BME": "BlackRock Health Sciences (healthcare)",
    "QQQX": "Nuveen NASDAQ 100 Dynamic Overwrite (equity)",
}


def get(url):
    sep = "&" if "?" in url else "?"
    try:
        req = urllib.request.Request(url + f"{sep}apikey={FMP}",
                                     headers={"User-Agent": "justhodl-ops"})
        with urllib.request.urlopen(req, timeout=25) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"_err": f"HTTP {e.code}"}
    except Exception as e:
        return {"_err": f"{type(e).__name__}: {str(e)[:120]}"}


def first(d):
    if isinstance(d, list):
        return d[0] if d else {}
    return d if isinstance(d, dict) else {}


report = {"ops": 825, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "FMP closed-end-fund NAV / discount data probe",
          "basket": CEFS, "per_cef": {}}

nav_ok = 0
quote_ok = 0
hist_ok = 0
div_yield_ok = 0
discount_samples = []

for sym, desc in CEFS.items():
    rec = {"desc": desc}

    # --- etf/info : the NAV candidate ---
    info = first(get(f"{BASE}/etf/info?symbol={sym}"))
    if "_err" in info:
        rec["info"] = info
    else:
        nav = info.get("nav")
        rec["info"] = {"keys": sorted(info.keys()),
                       "nav": nav, "navCurrency": info.get("navCurrency"),
                       "updatedAt": info.get("updatedAt"),
                       "name": info.get("name"),
                       "expenseRatio": info.get("expenseRatio")}
        if isinstance(nav, (int, float)) and nav > 0:
            nav_ok += 1

    # --- quote : current market price ---
    q = first(get(f"{BASE}/quote?symbol={sym}"))
    if "_err" in q:
        rec["quote"] = q
    else:
        px = q.get("price")
        rec["quote"] = {"price": px, "yield": q.get("yield"),
                        "volume": q.get("volume"),
                        "name": q.get("name"),
                        "marketCap": q.get("marketCap"),
                        "all_keys": sorted(q.keys())}
        if isinstance(px, (int, float)) and px > 0:
            quote_ok += 1

    # --- discount = (price - nav) / nav ---
    nav = (rec.get("info") or {}).get("nav")
    px = (rec.get("quote") or {}).get("price")
    if isinstance(nav, (int, float)) and nav > 0 and \
       isinstance(px, (int, float)) and px > 0:
        disc = (px - nav) / nav * 100.0
        rec["discount_pct"] = round(disc, 2)
        discount_samples.append((sym, round(disc, 2)))

    # --- historical price : engine needs >=120 bars for its own baseline ---
    h = get(f"{BASE}/historical-price-eod/light?symbol={sym}")
    if isinstance(h, list):
        rec["hist_bars"] = len(h)
        if len(h) >= 120:
            hist_ok += 1
        if h:
            rec["hist_latest"] = h[0]
    else:
        rec["hist_bars"] = h

    # --- dividends : trailing distribution yield ---
    dv = get(f"{BASE}/dividends?symbol={sym}")
    if isinstance(dv, list) and dv:
        ttm = sum(x.get("dividend", 0) or 0 for x in dv[:12])
        rec["dividends"] = {"n": len(dv),
                            "latest": dv[0],
                            "ttm_sum": round(ttm, 4)}
        if isinstance(px, (int, float)) and px > 0 and ttm > 0:
            rec["dist_yield_pct"] = round(ttm / px * 100.0, 2)
            div_yield_ok += 1
    else:
        rec["dividends"] = dv if isinstance(dv, dict) else {"n": 0}

    report["per_cef"][sym] = rec

n = len(CEFS)
report["summary"] = {
    "basket_size": n,
    "nav_available": f"{nav_ok}/{n}",
    "quote_available": f"{quote_ok}/{n}",
    "hist_120bars_available": f"{hist_ok}/{n}",
    "dist_yield_computable": f"{div_yield_ok}/{n}",
    "discount_samples": discount_samples,
}

# hard verdict for the build decision
nav_rate = nav_ok / n
if nav_rate >= 0.66:
    report["verdict"] = ("BUILD - FMP serves a usable NAV for the CEF basket; "
                         "discount-to-NAV engine is viable on real data.")
    report["build"] = "GREEN"
elif nav_rate >= 0.34:
    report["verdict"] = ("PARTIAL - NAV only for some CEFs; engine viable but "
                         "must hard-drop names with no NAV (no estimation).")
    report["build"] = "AMBER"
else:
    report["verdict"] = ("BLOCKED - FMP does not serve CEF NAV; a real "
                         "discount engine is not buildable on this data. "
                         "Pivot to a price-data-only engine.")
    report["build"] = "RED"

out = "aws/ops/reports/825_cef_data_probe.json"
with open(out, "w") as f:
    json.dump(report, f, indent=1)
print("WROTE", out)
print("BUILD:", report["build"], "|", report["verdict"])
print("NAV:", report["summary"]["nav_available"],
      "HIST:", report["summary"]["hist_120bars_available"],
      "DIST_YIELD:", report["summary"]["dist_yield_computable"])
print("discounts:", discount_samples)
