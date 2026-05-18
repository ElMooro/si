"""
justhodl-cds-monitor — Global Credit Default & Stress Monitor.

The platform proxies sovereign credit risk from bond yields (cds-proxy) and
tracks corporate bond spreads (credit-stress), but it has no SINGLE-NAME
credit-default read on the big banks and big companies — the names whose
CDS blowing out is the actual systemic event (Lehman 2008, Credit Suisse
2023). Real single-name CDS feeds cost $20k+/yr and are not on any free
API, so this engine builds the hedge-fund-standard alternative:

  THE MERTON STRUCTURAL MODEL — the same framework behind Moody's
  Analytics CreditEdge / the old KMV model. It treats a firm's equity as a
  call option on its assets, then backs out, by iteration, the unobservable
  asset value and asset volatility from observable inputs:
      E  — equity market value          (FMP, real-time)
      sE — equity volatility            (FMP price history)
      D  — total debt / default point   (FMP balance sheet)
      r  — risk-free rate               (FRED 1Y)
  From those it derives the DISTANCE TO DEFAULT, a market-implied
  one-year default probability, and a synthetic 5Y-equivalent CDS spread
  in basis points — a genuine, daily, market-based credit signal per name.

It computes this for a universe of global systemically-important banks and
large corporates, then fuses the result with the platform's existing
sovereign-CDS proxy, corporate bond stress, ECB systemic stress and the
ex-US canary grid into one consolidated global credit-stress composite and
an ALARM BOARD.

OUTPUT: data/cds-monitor.json   SCHEDULE: daily 13:00 UTC
Real data only — FMP + FRED + platform engines. Not investment advice.
"""
import json
import math
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cds-monitor.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# global systemically-important banks
BANKS = {
    "JPM": "JPMorgan Chase", "BAC": "Bank of America", "C": "Citigroup",
    "WFC": "Wells Fargo", "GS": "Goldman Sachs", "MS": "Morgan Stanley",
    "USB": "U.S. Bancorp", "PNC": "PNC Financial", "DB": "Deutsche Bank",
    "UBS": "UBS Group", "BCS": "Barclays", "HSBC": "HSBC Holdings",
}
# large corporates — mega-cap benchmarks + credit-sensitive names
CORPORATES = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet",
    "AMZN": "Amazon", "META": "Meta Platforms", "NVDA": "NVIDIA",
    "F": "Ford Motor", "GM": "General Motors", "T": "AT&T",
    "VZ": "Verizon", "BA": "Boeing", "INTC": "Intel",
    "ORCL": "Oracle", "DIS": "Walt Disney",
}
LGD = 0.60   # loss given default assumption for the synthetic spread


# ───────────────────────── data fetch ─────────────────────────
def _get(url, timeout=20):
    last = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "justhodl-cds-monitor/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:
            last = e
            if attempt < 2:
                time.sleep(0.8 * (attempt + 1))
    raise last or RuntimeError(f"fetch failed: {url}")


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
                continue
    return None


# ───────────────────────── Merton model ─────────────────────────
def ncdf(x):
    """Standard normal CDF."""
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def _solve_V(E, sV, D, r, T):
    """Solve E = V*N(d1) - D*e^{-rT}*N(d2) for asset value V (monotone)."""
    def f(V):
        if V <= 0:
            return -E
        d1 = (math.log(V / D) + (r + 0.5 * sV * sV) * T) / (sV * math.sqrt(T))
        d2 = d1 - sV * math.sqrt(T)
        return V * ncdf(d1) - D * math.exp(-r * T) * ncdf(d2) - E
    lo, hi = D * math.exp(-r * T) + 1e-6, (E + D) * 12.0
    if f(lo) > 0:
        return lo
    for _ in range(120):
        mid = (lo + hi) / 2
        fm = f(mid)
        if abs(fm) < E * 1e-8 or (hi - lo) < 1e-7 * hi:
            return mid
        if fm > 0:
            hi = mid
        else:
            lo = mid
    return (lo + hi) / 2


def merton(E, sigma_E, D, r, T=1.0):
    """Iterative KMV-style solve -> (asset_vol, distance_to_default, PD)."""
    if E <= 0 or D <= 0 or sigma_E <= 0:
        return None
    sV = max(sigma_E * E / (E + D), 0.03)
    for _ in range(50):
        V = _solve_V(E, sV, D, r, T)
        d1 = (math.log(V / D) + (r + 0.5 * sV * sV) * T) / (sV * math.sqrt(T))
        nd1 = ncdf(d1)
        if nd1 <= 1e-9 or V <= 0:
            break
        new_sV = sigma_E * E / (nd1 * V)
        new_sV = min(max(new_sV, 0.01), 3.0)
        if abs(new_sV - sV) < 1e-6:
            sV = new_sV
            break
        sV = new_sV
    V = _solve_V(E, sV, D, r, T)
    d1 = (math.log(V / D) + (r + 0.5 * sV * sV) * T) / (sV * math.sqrt(T))
    d2 = d1 - sV * math.sqrt(T)
    pd = ncdf(-d2)
    return {"asset_vol": sV, "distance_to_default": d2, "default_prob_1y": pd}


def synthetic_cds_bp(pd, T=1.0):
    """Map a risk-neutral 1y default probability to a CDS spread in bp."""
    pd = min(max(pd, 0.0), 0.9999)
    hazard = -math.log(1.0 - pd) / T
    return round(hazard * LGD * 10000.0, 1)


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def equity_vol(prices):
    """Annualised volatility from a price series (newest- or oldest-first)."""
    px = [p for p in prices if p and p > 0]
    if len(px) < 30:
        return None
    rets = [math.log(px[i] / px[i + 1]) for i in range(len(px) - 1)]
    rets = rets[:180] if len(rets) > 180 else rets
    n = len(rets)
    if n < 20:
        return None
    m = sum(rets) / n
    var = sum((x - m) ** 2 for x in rets) / (n - 1)
    return math.sqrt(var) * math.sqrt(252.0)


def name_regime(cds_bp):
    if cds_bp is None:
        return "UNKNOWN"
    if cds_bp < 40:
        return "VERY SAFE"
    if cds_bp < 100:
        return "INVESTMENT GRADE"
    if cds_bp < 250:
        return "ELEVATED"
    if cds_bp < 600:
        return "STRESSED"
    return "DISTRESSED"


def read_existing(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return None


# ───────────────────────── single-name pricing ─────────────────────────
def price_name(ticker, name, group, r):
    """Compute the Merton synthetic CDS for one firm."""
    try:
        q = fmp("quote", {"symbol": ticker})
        q = q[0] if isinstance(q, list) and q else (q or {})
        mcap = q.get("marketCap")
        if not mcap or mcap <= 0:
            return None, f"{ticker}: no market cap"

        bs = fmp("balance-sheet-statement", {"symbol": ticker, "limit": 1})
        bs = bs[0] if isinstance(bs, list) and bs else (bs or {})
        debt = bs.get("totalDebt")
        if not debt or debt <= 0:
            # banks sometimes report debt under other tags; fall back
            debt = bs.get("longTermDebt") or 0
            std = bs.get("shortTermDebt") or 0
            debt = (debt or 0) + (std or 0)
        if not debt or debt <= 0:
            return None, f"{ticker}: no debt figure"

        hp = fmp("historical-price-eod/light", {"symbol": ticker})
        rows = hp if isinstance(hp, list) else (hp or {}).get("historical", [])
        prices = [row.get("price") or row.get("close")
                  for row in rows if isinstance(row, dict)]
        sigma_E = equity_vol(prices)
        if sigma_E is None:
            return None, f"{ticker}: insufficient price history"

        m = merton(float(mcap), sigma_E, float(debt), r)
        if not m:
            return None, f"{ticker}: merton solve failed"
        cds = synthetic_cds_bp(m["default_prob_1y"])
        return {
            "ticker": ticker, "name": name, "group": group,
            "market_cap_usd_bn": round(mcap / 1e9, 1),
            "total_debt_usd_bn": round(debt / 1e9, 1),
            "equity_vol_pct": round(sigma_E * 100, 1),
            "asset_vol_pct": round(m["asset_vol"] * 100, 1),
            "distance_to_default": round(m["distance_to_default"], 2),
            "default_prob_1y_pct": round(m["default_prob_1y"] * 100, 3),
            "synthetic_cds_bp": cds,
            "regime": name_regime(cds),
        }, None
    except Exception as e:
        return None, f"{ticker}: {str(e)[:60]}"


# ───────────────────────── handler ─────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors = []

    r = fred_latest("DGS1")
    r = (r / 100.0) if r is not None else 0.04   # 1y risk-free, decimal

    # ── 1. single-name Merton CDS ──
    banks, corporates = [], []
    for tk, nm in BANKS.items():
        row, err = price_name(tk, nm, "bank", r)
        if row:
            banks.append(row)
        elif err:
            errors.append(err)
        time.sleep(0.15)
    for tk, nm in CORPORATES.items():
        row, err = price_name(tk, nm, "corporate", r)
        if row:
            corporates.append(row)
        elif err:
            errors.append(err)
        time.sleep(0.15)

    banks.sort(key=lambda x: x["synthetic_cds_bp"], reverse=True)
    corporates.sort(key=lambda x: x["synthetic_cds_bp"], reverse=True)

    def avg(rows):
        v = [x["synthetic_cds_bp"] for x in rows]
        return round(sum(v) / len(v), 1) if v else None

    bank_avg = avg(banks)
    corp_avg = avg(corporates)
    bank_worst = banks[0] if banks else None
    corp_worst = corporates[0] if corporates else None
    sn_read = (
        f"Banks: average synthetic CDS {bank_avg:.0f}bp"
        + (f", widest {bank_worst['name']} {bank_worst['synthetic_cds_bp']:.0f}bp "
           f"({bank_worst['regime'].lower()})" if bank_worst else "")
        + f". Corporates: average {corp_avg:.0f}bp"
        + (f", widest {corp_worst['name']} {corp_worst['synthetic_cds_bp']:.0f}bp"
           if corp_worst else "")
        + ". Bank readings run structurally higher — banks are leveraged by "
        "design — so read the rank and the moves, not the absolute level."
        if bank_avg is not None and corp_avg is not None
        else "Single-name pricing partial.")

    # ── 2. cross-reference the platform's credit engines ──
    cdsp = read_existing("data/cds-proxy.json") or {}
    crst = read_existing("data/credit-stress.json") or {}
    cgrid = read_existing("data/canary-grid.json") or {}
    ssj = read_existing("data/systemic-stress.json") or {}
    bstr = read_existing("data/bank-stress.json") or {}

    sovereign = {
        "proxy_composite_0_100": cdsp.get("composite_credit_risk"),
        "regime": cdsp.get("regime"),
        "sovereigns": cdsp.get("sovereigns"),
        "source": "cds-proxy (10Y sovereign spread proxy)",
    }
    hy_oas = (crst.get("hy_oas") or cdsp.get("hy_oas")
              or (crst.get("spreads") or {}).get("hy_oas"))
    ig_oas = (crst.get("ig_oas") or cdsp.get("ig_oas")
              or (crst.get("spreads") or {}).get("ig_oas"))
    bond_stress = {
        "hy_oas": hy_oas, "ig_oas": ig_oas,
        "regime": crst.get("regime") or crst.get("status"),
        "source": "credit-stress / cds-proxy (ICE BofA OAS)",
    }
    canary = {
        "state": (cgrid.get("state") or cgrid.get("regime")
                  or cgrid.get("posture") or cgrid.get("grid_state")),
        "score": (cgrid.get("score") or cgrid.get("composite")
                  or cgrid.get("grid_score")),
        "tripped": (cgrid.get("tripped") or cgrid.get("alarms")
                    or cgrid.get("warnings")),
        "source": "canary-grid (ex-US early-warning grid)",
    }
    systemic = {
        "ciss_composite": (ssj.get("composite") or {}).get("score_0_100"),
        "regime": (ssj.get("composite") or {}).get("regime"),
        "source": "systemic-stress (ECB CISS/SovCISS)",
    }

    # ── 3. CONSOLIDATED GLOBAL CREDIT-STRESS COMPOSITE (0-100) ──
    parts, wts = [], []
    if bank_avg is not None:
        parts.append(clamp((bank_avg - 40) / 360, 0, 1) * 100)
        wts.append(0.28)
    if isinstance(sovereign["proxy_composite_0_100"], (int, float)):
        parts.append(float(sovereign["proxy_composite_0_100"]))
        wts.append(0.20)
    if isinstance(hy_oas, (int, float)):
        # HY OAS: ~3% benign, ~6% stressed, ~9%+ crisis
        parts.append(clamp((float(hy_oas) - 3.0) / 6.0, 0, 1) * 100)
        wts.append(0.24)
    if isinstance(systemic["ciss_composite"], (int, float)):
        parts.append(float(systemic["ciss_composite"]))
        wts.append(0.16)
    if isinstance(canary["score"], (int, float)):
        cs = float(canary["score"])
        parts.append(cs if cs <= 100 else clamp(cs, 0, 100))
        wts.append(0.12)
    composite = (round(sum(p * w for p, w in zip(parts, wts)) / sum(wts), 1)
                 if parts else None)
    if composite is None:
        regime = "UNKNOWN"
    elif composite >= 80:
        regime = "CRISIS"
    elif composite >= 62:
        regime = "STRESSED"
    elif composite >= 42:
        regime = "ELEVATED"
    elif composite >= 25:
        regime = "WATCH"
    else:
        regime = "CALM"

    # ── 4. ALARM BOARD ──
    alarms = []

    def add(level, signal, detail):
        alarms.append({"level": level, "signal": signal, "detail": detail})

    for b in banks:
        if b["synthetic_cds_bp"] >= 600:
            add("ALERT", f"Bank credit distress — {b['name']}",
                f"synthetic CDS {b['synthetic_cds_bp']:.0f}bp, "
                f"1y default prob {b['default_prob_1y_pct']:.2f}%")
        elif b["synthetic_cds_bp"] >= 250:
            add("WATCH", f"Bank credit elevated — {b['name']}",
                f"synthetic CDS {b['synthetic_cds_bp']:.0f}bp")
    for c in corporates:
        if c["synthetic_cds_bp"] >= 600:
            add("ALERT", f"Corporate credit distress — {c['name']}",
                f"synthetic CDS {c['synthetic_cds_bp']:.0f}bp")
        elif c["synthetic_cds_bp"] >= 300:
            add("WATCH", f"Corporate credit elevated — {c['name']}",
                f"synthetic CDS {c['synthetic_cds_bp']:.0f}bp")
    if isinstance(hy_oas, (int, float)) and hy_oas >= 5.0:
        add("WATCH" if hy_oas < 7 else "ALERT", "High-yield credit stress",
            f"HY OAS {hy_oas:.2f}%")
    if (sovereign["regime"] or "").upper() in ("STRESS", "CRISIS"):
        add("ALERT", "Sovereign credit stress",
            f"cds-proxy regime {sovereign['regime']}")
    if isinstance(systemic["ciss_composite"], (int, float)) \
            and systemic["ciss_composite"] >= 55:
        add("WATCH", "Systemic stress elevated",
            f"CISS composite {systemic['ciss_composite']:.0f}/100")
    if (canary["state"] or "").upper() in ("WARNING", "ALERT", "RED",
                                           "ELEVATED"):
        add("WATCH", "Canary grid tripped",
            f"ex-US early-warning state {canary['state']}")
    if not alarms:
        add("INFO", "No active credit alarms",
            "all monitored credit signals within normal ranges")
    alarm_levels = [a["level"] for a in alarms]
    board_status = ("ALERT" if "ALERT" in alarm_levels else
                    "WATCH" if "WATCH" in alarm_levels else "CLEAR")

    comp_read = (
        f"Global credit-default stress {composite:.0f}/100 — {regime.lower()}. "
        + ("Credit markets are pricing meaningful default risk; the alarm "
           "board is active — reduce credit and high-beta exposure."
           if composite is not None and composite >= 42 else
           "Credit conditions are contained; single-name, sovereign and "
           "systemic credit signals are not corroborating acute stress.")
        if composite is not None else "Composite unavailable.")

    headline = (
        f"Global credit stress {regime}. Composite {composite:.0f}/100; bank "
        f"synthetic CDS avg {bank_avg:.0f}bp; alarm board {board_status}."
        if composite is not None and bank_avg is not None
        else "Global credit default & stress monitor: partial data.")

    core_ok = len(banks) >= 6 and len(corporates) >= 6
    out = {
        "schema_version": "1.0",
        "method": "merton_structural_credit_plus_platform_synthesis",
        "generated_at": now.isoformat(),
        "elapsed_s": round(time.time() - t0, 2),
        "ok": core_ok and composite is not None,
        "headline": headline,
        "global_credit_stress": {
            "score_0_100": composite,
            "regime": regime,
            "read": comp_read,
        },
        "alarm_board": {
            "status": board_status,
            "n_active": len([a for a in alarms
                             if a["level"] != "INFO"]),
            "alarms": alarms,
        },
        "single_name_cds": {
            "model": "Merton structural model (KMV-style iterative solve)",
            "risk_free_1y_pct": round(r * 100, 2),
            "lgd_assumption": LGD,
            "banks": banks,
            "corporates": corporates,
            "bank_avg_cds_bp": bank_avg,
            "corporate_avg_cds_bp": corp_avg,
            "widest_bank": (bank_worst or {}).get("name"),
            "widest_corporate": (corp_worst or {}).get("name"),
            "read": sn_read,
        },
        "sovereign_cds": sovereign,
        "global_bond_stress": bond_stress,
        "canary_alarms": canary,
        "systemic_stress": systemic,
        "cross_reference": {
            "bank_stress_engine": bstr.get("regime") or bstr.get("status")
            or bstr.get("state"),
        },
        "sources": ["FMP /stable/ — equity, balance sheet, price history",
                    "FRED — 1Y risk-free rate",
                    "Platform engines — cds-proxy, credit-stress, "
                    "canary-grid, systemic-stress"],
        "errors": errors,
    }

    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, indent=2).encode("utf-8"),
                  ContentType="application/json", CacheControl="max-age=300")
    print(f"[cds-monitor] global credit stress {composite}/100 ({regime}) | "
          f"bank avg CDS {bank_avg}bp | alarms {board_status} | "
          f"names={len(banks)+len(corporates)} errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "composite": composite,
                                "regime": regime,
                                "alarm_board": board_status,
                                "errors": len(errors)})}
