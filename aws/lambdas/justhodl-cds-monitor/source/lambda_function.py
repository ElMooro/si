"""
justhodl-cds-monitor — Global Credit Default & Stress Monitor.

The platform proxies sovereign credit risk from bond yields (cds-proxy) and
tracks corporate bond spreads (credit-stress), but it has no SINGLE-NAME
credit-default read on the big banks and big companies — the names whose
CDS blowing out is the actual systemic event (Lehman 2008, Credit Suisse
2023). Real single-name CDS feeds cost $20k+/yr and are on no free API, so
this engine builds the hedge-fund-standard alternative: the structural
equity-to-credit bridge.

  THE CREDITGRADES MODEL — the industry-standard structural model
  (RiskMetrics / JPMorgan / Goldman / Deutsche Bank, 2002) for translating
  equity into a CDS-like read. It extends Merton with an UNCERTAIN default
  barrier, which fattens the near-term default distribution and fixes
  Merton's well-known failure of pricing investment-grade names at ~0.
  From observable inputs —
      S   — share price                 (FMP, real-time)
      sE  — equity volatility           (FMP price history)
      D   — debt per share              (FMP balance sheet)
      r   — risk-free rate              (FRED)
  it derives a market-implied DISTANCE TO DEFAULT, a 5-year default
  probability and a synthetic CDS spread for the global systemically-
  important banks and large corporates.

  THE SIGNAL is the distance-to-default and its RANK / trend — structural
  models systematically understate the absolute level of investment-grade
  and bank spreads, so the engine leads with DD and treats the synthetic
  bp spread as a directional estimate, not a traded quote.

It then fuses the single-name read with the platform's sovereign-CDS proxy,
corporate bond stress, ECB systemic stress and the ex-US canary grid into
one consolidated global credit-stress composite and an ALARM BOARD.

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
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

s3 = boto3.client("s3")
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/cds-monitor.json"
FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

BANKS = {
    "JPM": "JPMorgan Chase", "BAC": "Bank of America", "C": "Citigroup",
    "WFC": "Wells Fargo", "GS": "Goldman Sachs", "MS": "Morgan Stanley",
    "USB": "U.S. Bancorp", "PNC": "PNC Financial", "DB": "Deutsche Bank",
    "UBS": "UBS Group", "BCS": "Barclays", "HSBC": "HSBC Holdings",
}
CORPORATES = {
    "AAPL": "Apple", "MSFT": "Microsoft", "GOOGL": "Alphabet",
    "AMZN": "Amazon", "META": "Meta Platforms", "NVDA": "NVIDIA",
    "F": "Ford Motor", "GM": "General Motors", "T": "AT&T",
    "VZ": "Verizon", "BA": "Boeing", "INTC": "Intel",
    "ORCL": "Oracle", "DIS": "Walt Disney",
}
# CreditGrades parameters (Finkelstein et al. 2002 standard set)
CG_LBAR = 0.50    # mean global recovery on the reference asset (barrier)
CG_LAMBDA = 0.30  # volatility of the default barrier
CG_R = 0.50       # bond recovery for the spread conversion
CG_T = 5.0        # horizon — the standard CDS tenor


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


# ───────────────────────── CreditGrades model ─────────────────────────
def ncdf(x):
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def credit_grades(S, sigma_E, debt_per_share, r,
                  t=CG_T, lbar=CG_LBAR, lam=CG_LAMBDA, recovery=CG_R):
    """CreditGrades structural model -> distance-to-default, PD, spread.

    Extends Merton with an uncertain default barrier (variance term lam^2)
    so investment-grade names price away from zero."""
    if S <= 0 or sigma_E <= 0 or debt_per_share <= 0:
        return None
    barrier = lbar * debt_per_share
    if barrier <= 0:
        return None
    # asset volatility from equity volatility
    sigma = sigma_E * S / (S + barrier)
    if sigma <= 1e-5:
        return None
    d = (S + barrier) / barrier * math.exp(lam * lam)
    d = max(d, 1.0001)
    lnd = math.log(d)

    def survival(tt):
        a2 = sigma * sigma * tt + lam * lam
        a = math.sqrt(a2)
        return ncdf(-a / 2 + lnd / a) - d * ncdf(-a / 2 - lnd / a)

    p_t = min(max(survival(t), 1e-6), 0.999999)
    pd = 1.0 - p_t
    a_t = math.sqrt(sigma * sigma * t + lam * lam)
    # effective distance to default on the CreditGrades scale
    dd = lnd / a_t - a_t / 2.0
    hazard = -math.log(p_t) / t
    spread_bp = hazard * (1.0 - recovery) * 1e4
    return {"asset_vol": sigma, "survival_5y": p_t,
            "default_prob_5y": pd, "distance_to_default": dd,
            "synthetic_cds_bp": round(spread_bp, 1)}


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def equity_vol(prices):
    """Annualised volatility from a price series."""
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


def dd_regime(dd):
    """Credit regime from the distance-to-default — the robust signal."""
    if dd is None:
        return "UNKNOWN"
    if dd >= 4.5:
        return "VERY SAFE"
    if dd >= 3.5:
        return "INVESTMENT GRADE"
    if dd >= 2.6:
        return "NORMAL"
    if dd >= 1.8:
        return "ELEVATED"
    if dd >= 1.1:
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
    try:
        q = fmp("quote", {"symbol": ticker})
        q = q[0] if isinstance(q, list) and q else (q or {})
        price = q.get("price")
        mcap = q.get("marketCap")
        if not price or price <= 0 or not mcap or mcap <= 0:
            return None, f"{ticker}: no price/market cap"
        shares = mcap / price

        bs = fmp("balance-sheet-statement", {"symbol": ticker, "limit": 1})
        bs = bs[0] if isinstance(bs, list) and bs else (bs or {})
        ltd = bs.get("longTermDebt") or 0
        std = bs.get("shortTermDebt") or 0
        total_debt = bs.get("totalDebt") or 0
        if total_debt <= 0:
            total_debt = ltd + std
        # The structural default barrier is the SENIOR BOND STACK that a 5Y
        # senior CDS references. For a bank that is its long-term debt — NOT
        # total borrowings: FMP's bank `totalDebt` sweeps in repo, fed-funds
        # purchased and short-term wholesale funding, which is secured /
        # self-liquidating and not the run-prone default point. Using it
        # makes every G-SIB price as near-distressed. Corporates use total
        # debt (their borrowings are the relevant barrier).
        if group == "bank":
            barrier_debt = ltd if ltd > 0 else total_debt
        else:
            barrier_debt = total_debt if total_debt > 0 else (ltd + std)
        if barrier_debt <= 0:
            return None, f"{ticker}: no debt figure"
        debt_per_share = barrier_debt / shares

        hp = fmp("historical-price-eod/light", {"symbol": ticker})
        rows = hp if isinstance(hp, list) else (hp or {}).get("historical", [])
        prices = [row.get("price") or row.get("close")
                  for row in rows if isinstance(row, dict)]
        sigma_E = equity_vol(prices)
        if sigma_E is None:
            return None, f"{ticker}: insufficient price history"

        cg = credit_grades(float(price), sigma_E, debt_per_share, r)
        if not cg:
            return None, f"{ticker}: model solve failed"
        return {
            "ticker": ticker, "name": name, "group": group,
            "market_cap_usd_bn": round(mcap / 1e9, 1),
            "total_debt_usd_bn": round(total_debt / 1e9, 1),
            "equity_vol_pct": round(sigma_E * 100, 1),
            "asset_vol_pct": round(cg["asset_vol"] * 100, 1),
            "distance_to_default": round(cg["distance_to_default"], 2),
            "default_prob_5y_pct": round(cg["default_prob_5y"] * 100, 2),
            "synthetic_cds_bp": cg["synthetic_cds_bp"],
            "regime": dd_regime(cg["distance_to_default"]),
        }, None
    except Exception as e:
        return None, f"{ticker}: {str(e)[:60]}"


# ───────────────────────── handler ─────────────────────────
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)
    errors = []

    r = fred_latest("DGS1")
    r = (r / 100.0) if r is not None else 0.04

    # ── 1. single-name CreditGrades pricing ──
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

    # ── 1b. MARKET-ANCHORED CALIBRATION of single-name CDS ──
    # A structural model that compares market cap to debt conflates a low
    # price-to-book (Deutsche Bank, Barclays trade ~0.5x book) with default
    # risk — so its absolute synthetic spreads are unreliable: too hot for
    # out-of-favour names, too cold for richly-valued ones. The structural
    # distance-to-default is kept as the cross-sectional signal, but the
    # LEVEL of each name's synthetic CDS is anchored to the observable
    # credit market (ICE BofA IG OAS) and the model-driven tilt around that
    # anchor is bounded to a realistic dispersion. Single-name CDS then sits
    # on a real scale and moves with the real credit cycle — no false
    # alarms — while still flagging which names diverge from their peers.
    _crst = read_existing("data/credit-stress.json") or {}
    _cdspe = read_existing("data/cds-proxy.json") or {}
    ig_oas_pct = (_crst.get("ig_oas_pct") or _crst.get("ig_oas")
                  or _cdspe.get("ig_oas_pct") or _cdspe.get("ig_oas")
                  or (_crst.get("spreads") or {}).get("ig_oas"))
    try:
        ig_oas_pct = float(ig_oas_pct)
        if not (0.2 <= ig_oas_pct <= 12.0):
            ig_oas_pct = None
    except (TypeError, ValueError):
        ig_oas_pct = None
    IG_OAS_NORMAL = 1.05   # recent calm-regime ICE BofA IG OAS, %

    def market_anchor_cds(rows, base_anchor_bp, tilt_k=1.15,
                          tilt_lo=0.50, tilt_hi=2.30):
        """Re-level each name's synthetic CDS onto the observable credit
        market. anchor = base x (IG OAS / normal); each name is tilted off
        the group-median distance-to-default, the tilt bounded so no name
        runs away from a realistic spread. Returns the anchor (bp)."""
        dds = sorted(x["distance_to_default"] for x in rows
                     if isinstance(x.get("distance_to_default"),
                                   (int, float)))
        if not dds:
            return None
        med = dds[len(dds) // 2]
        cycle = (ig_oas_pct / IG_OAS_NORMAL) if ig_oas_pct else 1.0
        anchor = base_anchor_bp * cycle
        for x in rows:
            dd = x.get("distance_to_default")
            x["structural_cds_raw_bp"] = x.get("synthetic_cds_bp")
            if not isinstance(dd, (int, float)) or dd <= 0 or med <= 0:
                x["synthetic_cds_bp"] = round(anchor, 1)
                continue
            tilt = clamp((med / dd) ** tilt_k, tilt_lo, tilt_hi)
            x["synthetic_cds_bp"] = round(anchor * tilt, 1)
        return round(anchor, 1)

    bank_anchor_bp = market_anchor_cds(banks, 58.0)
    corp_anchor_bp = market_anchor_cds(corporates, 50.0, tilt_lo=0.40,
                                       tilt_hi=2.60)

    # sort riskiest first — widest (calibrated) synthetic spread
    banks.sort(key=lambda x: x["synthetic_cds_bp"], reverse=True)
    corporates.sort(key=lambda x: x["synthetic_cds_bp"], reverse=True)

    def _median(xs):
        s = sorted(xs)
        n = len(s)
        if not n:
            return None
        return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0

    def assign_credit_regimes(rows):
        """Regime from the ABSOLUTE market-anchored synthetic CDS — not a
        peer percentile. A percentile ranking always brands the weakest
        names 'ELEVATED' even when the entire group is healthy: a permanent
        false alarm. Because the synthetic CDS here is already anchored to
        the live IG credit market, these absolute bands move with the real
        cycle — in calm markets every sound name reads STRONG/SOLID and only
        genuine deterioration, or a true cycle turn, lifts a name into
        WATCH or worse. peer_rank is retained as an ordering aid only."""
        for i, x in enumerate(rows):
            s = x.get("synthetic_cds_bp")
            if not isinstance(s, (int, float)):
                x["regime"] = "UNKNOWN"
            elif s < 55:
                x["regime"] = "STRONG"
            elif s < 90:
                x["regime"] = "SOLID"
            elif s < 145:
                x["regime"] = "NORMAL"
            elif s < 230:
                x["regime"] = "WATCH"
            elif s < 400:
                x["regime"] = "ELEVATED"
            elif s < 650:
                x["regime"] = "STRESSED"
            else:
                x["regime"] = "DISTRESSED"
            x["peer_rank"] = i + 1

    assign_credit_regimes(banks)
    assign_credit_regimes(corporates)

    def avg(rows, key):
        v = [x[key] for x in rows]
        return round(sum(v) / len(v), 2) if v else None

    bank_cds = [x["synthetic_cds_bp"] for x in banks]
    corp_cds = [x["synthetic_cds_bp"] for x in corporates]
    bank_avg_dd = avg(banks, "distance_to_default")
    corp_avg_dd = avg(corporates, "distance_to_default")
    bank_avg_cds = avg(banks, "synthetic_cds_bp")
    corp_avg_cds = avg(corporates, "synthetic_cds_bp")
    bank_median_cds = _median(bank_cds)
    corp_median_cds = _median(corp_cds)
    bank_worst = banks[0] if banks else None
    corp_worst = corporates[0] if corporates else None
    sn_read = (
        f"Banks: median synthetic CDS {bank_median_cds:.0f}bp"
        + (f", widest {bank_worst['name']} {bank_worst['synthetic_cds_bp']:.0f}bp"
           if bank_worst else "")
        + f". Corporates: median {corp_median_cds:.0f}bp"
        + (f", widest {corp_worst['name']} {corp_worst['synthetic_cds_bp']:.0f}bp"
           if corp_worst else "")
        + ". Each name is graded on absolute, cycle-aware credit bands — "
        "the synthetic CDS is anchored to the live IG market, so a regime "
        "of WATCH or worse reflects genuine deterioration, not merely being "
        "the weakest of a healthy peer group."
        if bank_median_cds is not None and corp_median_cds is not None
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
        "source": "canary-grid (ex-US early-warning grid)",
    }
    systemic = {
        "ciss_composite": (ssj.get("composite") or {}).get("score_0_100"),
        "regime": (ssj.get("composite") or {}).get("regime"),
        "source": "systemic-stress (ECB CISS/SovCISS)",
    }

    # ── 3. CONSOLIDATED GLOBAL CREDIT-STRESS COMPOSITE (0-100) ──
    parts, wts = [], []
    if bank_median_cds is not None:
        # median synthetic spread is robust to the model's overstated tail;
        # add a bump only when one name genuinely breaks from the pack
        leg = clamp((bank_median_cds - 50) / 300, 0, 1) * 100
        if bank_worst and bank_median_cds > 0 and \
                bank_worst["synthetic_cds_bp"] > 3.0 * bank_median_cds:
            leg = min(100.0, leg + 20.0)
        parts.append(leg)
        wts.append(0.24)
    if isinstance(sovereign["proxy_composite_0_100"], (int, float)):
        parts.append(float(sovereign["proxy_composite_0_100"]))
        wts.append(0.20)
    if isinstance(hy_oas, (int, float)):
        parts.append(clamp((float(hy_oas) - 3.0) / 6.0, 0, 1) * 100)
        wts.append(0.24)
    if isinstance(systemic["ciss_composite"], (int, float)):
        parts.append(float(systemic["ciss_composite"]))
        wts.append(0.16)
    if isinstance(canary["score"], (int, float)):
        cs = float(canary["score"])
        parts.append(clamp(cs, 0, 100))
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

    bmed = bank_median_cds or 0
    for b in banks:
        s = b["synthetic_cds_bp"]
        if s > max(450, 3.5 * bmed):
            add("ALERT", f"Bank credit outlier — {b['name']}",
                f"synthetic CDS {s:.0f}bp vs peer median {bmed:.0f}bp, "
                f"distance-to-default {b['distance_to_default']:.1f}")
        elif s > max(250, 2.2 * bmed):
            add("WATCH", f"Bank credit elevated — {b['name']}",
                f"synthetic CDS {s:.0f}bp vs peer median {bmed:.0f}bp")
    cmed = corp_median_cds or 0
    for c in corporates:
        s = c["synthetic_cds_bp"]
        if s > max(450, 3.5 * cmed):
            add("ALERT", f"Corporate credit outlier — {c['name']}",
                f"synthetic CDS {s:.0f}bp vs peer median {cmed:.0f}bp")
        elif s > max(280, 2.2 * cmed):
            add("WATCH", f"Corporate credit elevated — {c['name']}",
                f"synthetic CDS {s:.0f}bp vs peer median {cmed:.0f}bp")
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
    levels = [a["level"] for a in alarms]
    board_status = ("ALERT" if "ALERT" in levels else
                    "WATCH" if "WATCH" in levels else "CLEAR")

    comp_read = (
        f"Global credit-default stress {composite:.0f}/100 — {regime.lower()}. "
        + ("Credit markets are pricing meaningful default risk; the alarm "
           "board is active — reduce credit and high-beta exposure."
           if composite is not None and composite >= 42 else
           "Credit conditions are contained; single-name distance-to-default, "
           "sovereign and systemic signals are not corroborating acute stress.")
        if composite is not None else "Composite unavailable.")

    headline = (
        f"Global credit stress {regime}. Composite {composite:.0f}/100; bank "
        f"avg distance-to-default {bank_avg_dd:.1f}; alarm board {board_status}."
        if composite is not None and bank_avg_dd is not None
        else "Global credit default & stress monitor: partial data.")

    core_ok = len(banks) >= 6 and len(corporates) >= 6
    out = {
        "schema_version": "1.1",
        "method": "creditgrades_structural_credit_plus_platform_synthesis",
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
            "n_active": len([a for a in alarms if a["level"] != "INFO"]),
            "alarms": alarms,
        },
        "single_name_cds": {
            "model": "CreditGrades structural model (equity-to-CDS bridge, "
                     "5-year horizon)",
            "primary_signal": "absolute_market_anchored_credit_regime",
            "calibration": {
                "method": "market-anchored — synthetic CDS level pinned to "
                          "ICE BofA IG OAS, structural distance-to-default "
                          "drives a bounded cross-sectional tilt",
                "ig_oas_pct": ig_oas_pct,
                "bank_anchor_bp": bank_anchor_bp,
                "corporate_anchor_bp": corp_anchor_bp,
                "note": "structural_cds_raw_bp on each name is the "
                        "un-anchored model output, retained for transparency",
            },
            "risk_free_1y_pct": round(r * 100, 2),
            "recovery_assumption": CG_R,
            "banks": banks,
            "corporates": corporates,
            "bank_avg_distance_to_default": bank_avg_dd,
            "corporate_avg_distance_to_default": corp_avg_dd,
            "bank_avg_cds_bp": bank_avg_cds,
            "corporate_avg_cds_bp": corp_avg_cds,
            "bank_median_cds_bp": bank_median_cds,
            "corporate_median_cds_bp": corp_median_cds,
            "weakest_bank": (bank_worst or {}).get("name"),
            "weakest_corporate": (corp_worst or {}).get("name"),
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
          f"bank avg DD {bank_avg_dd} | alarms {board_status} | "
          f"names={len(banks)+len(corporates)} errors={len(errors)}")
    return {"statusCode": 200,
            "body": json.dumps({"ok": out["ok"], "composite": composite,
                                "regime": regime,
                                "alarm_board": board_status,
                                "errors": len(errors)})}
