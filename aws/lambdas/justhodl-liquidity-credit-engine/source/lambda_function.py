"""
justhodl-liquidity-credit-engine

Pulls the FRED series Khalid specified for measuring system liquidity and
credit-market stress, computes WoW/MoM/QoQ/YoY % changes, z-scores (1y, 5y),
and signal classifications (NORMAL / WATCH / ELEVATED / CRISIS) calibrated
against historical events (GFC 2008, COVID 2020, SVB 2023, Sep 2019 repo).

CATEGORIES
  balance_sheet      — Fed assets, reserves, memo collateral
  liquidity_facilities — central bank swaps, primary credit (FCB stress)
  credit_spreads     — ICE BofA HY OAS (US/Euro/EM)
  corporate_yields   — HQM Corporate spot rates

OUTPUT  data/liquidity-credit-engine.json (5min CDN cache)
SCHEDULE  every 6h (FRED H.4.1 publishes Wednesday 4:30pm ET; ICE BofA daily)

Threshold rationale (research-backed):
  • CCC HY OAS:    GFC peak 2200bp, COVID peak 1900bp, normal 600-900bp
  • Euro HY OAS:   GFC peak 2400bp, COVID peak 1100bp, normal 300-500bp
  • EM HY Corp:    GFC peak 1700bp, COVID peak 1200bp, normal 500-800bp
  • Primary credit (OTHL1690): SVB spike was $164B; normal $0-2B
  • CB swaps (SWP1690): COVID peak $446B, normal $0-1B; reactivations are FX-stress signal
  • Bank reserves week-on-week: -2% in a week = QT acceleration / tightening

Hooks into alert-router on signal-state transitions.
"""
import json
import os
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta
from statistics import mean, pstdev

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/liquidity-credit-engine.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

# ────────────────────────────────────────────────────────────────────────
# SERIES MAP — every series Khalid specified + supporting context series.
# Each entry: (FRED id, category, label, units, threshold spec)
# threshold "kind":
#   "level"  — absolute thresholds {watch, elevated, crisis} on latest value
#   "delta_pct" — thresholds on % change over window (week/month)
#   "z"       — thresholds on z-score (1y default)
#   "spread_to" — compute spread vs another series' latest value
# ────────────────────────────────────────────────────────────────────────
SERIES_MAP = [
    # ════════════════════════════════════════════════════════════════════
    # CATEGORY 1: BALANCE SHEET (H.4.1 — Factors Affecting Reserve Balances)
    # All FRED balance-sheet series report in MILLIONS — converted to billions.
    # ════════════════════════════════════════════════════════════════════

    # ─── Total balance sheet ─────────────────────────────────
    ("WALCL", "balance_sheet", "Fed Balance Sheet (total assets)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5, "elevated": -1.0, "crisis": -2.0,
                     "convert_to_billions": True}),

    # ─── Securities Held Outright sub-portfolios ─────────────
    # Treasury holdings (was missing — WSHOMCB is MBS, not Treasuries!)
    ("WSHOTSL", "balance_sheet", "Securities Held Outright: U.S. Treasury (TOTAL)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5, "elevated": -1.0,
                     "convert_to_billions": True,
                     "note": "Sum of all Treasury holdings — bills + notes + bonds + TIPS"}),
    # MBS portfolio
    ("WSHOMCB", "balance_sheet", "Securities Held Outright: Mortgage-Backed Securities",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5, "elevated": -1.0,
                     "convert_to_billions": True,
                     "note": "Fed MBS portfolio — QT runoff or purchase pace"}),
    # Treasury notes & bonds (Khalid-spec)
    ("RESPPALGUONNWW", "balance_sheet", "Securities Held Outright: Treasury Notes & Bonds",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -0.5, "elevated": -1.0,
                     "convert_to_billions": True,
                     "note": "Khalid-spec — coupon Treasuries on Fed balance sheet"}),
    # Gold reserves
    ("WGCAL", "balance_sheet", "Gold Stock (assets)",
     "billions $", {"kind": "level", "watch": 50, "convert_to_billions": True}),

    # ─── Memo collateral pledges (Khalid-spec) ───────────────
    ("RESPPNTEPNWW", "balance_sheet", "MEMO: Treasury/Agency/MBS Eligible to Be Pledged",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": 5.0, "elevated": 10.0, "crisis": 20.0,
                     "convert_to_billions": True,
                     "note": "Khalid-spec — collateral pledge spike = funding stress"}),

    # ─── Reserves & TGA ──────────────────────────────────────
    ("WRESBAL", "balance_sheet", "Bank Reserves (depository institutions)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -2.0, "elevated": -4.0, "crisis": -8.0,
                     "convert_to_billions": True,
                     "note": "Reserves dropping >2%/week = QT acceleration"}),
    ("TOTRESNS", "balance_sheet", "Total Reserves of Depository Institutions",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": -2.0, "elevated": -4.0,
                     "convert_to_billions": True}),
    ("WTREGEN", "balance_sheet", "Treasury General Account (TGA)",
     "billions $", {"kind": "level", "watch": 800, "elevated": 1000, "crisis": 1500,
                     "convert_to_billions": True,
                     "note": "TGA above $800B drains liquidity from system"}),
    ("WCURCIR", "balance_sheet", "Currency in Circulation (liability)",
     "billions $", {"kind": "delta_pct", "window": "wk", "watch": 1.0, "elevated": 3.0,
                     "convert_to_billions": True,
                     "note": "Surge in cash demand = bank-run signal"}),

    # ─── Reverse Repo ─────────────────────────────────────────
    ("RRPONTSYD", "balance_sheet", "Overnight Reverse Repo (RRP)",
     "billions $", {"kind": "level", "watch": 1500, "elevated": 2000,
                     "convert_to_billions": True,
                     "note": "RRP draining is liquidity-positive; near-zero = MMFs back in T-bills"}),
    ("WLRRAL", "balance_sheet", "Reverse Repos with Foreign Officials",
     "billions $", {"kind": "level", "watch": 400, "elevated": 600,
                     "convert_to_billions": True,
                     "note": "Foreign central bank parking dollars at Fed"}),

    # ════════════════════════════════════════════════════════════════════
    # CATEGORY 2: LIQUIDITY FACILITIES (Discount Window + Emergency Lending)
    # ════════════════════════════════════════════════════════════════════
    ("DPCREDIT", "liquidity_facilities", "Primary Credit (Discount Window)",
     "billions $", {"kind": "level", "watch": 2, "elevated": 5, "crisis": 25,
                     "convert_to_billions": True,
                     "note": "SVB spike was $164B — banks borrowing here = funding stress"}),
    ("OTHL1690", "liquidity_facilities", "Liquidity & Credit Facilities Loans 16-90 Day",
     "billions $", {"kind": "level", "watch": 0.5, "elevated": 5, "crisis": 25,
                     "convert_to_billions": True,
                     "note": "Khalid-spec — emergency facilities active = financial-crisis signal"}),
    ("SWP1690", "liquidity_facilities", "Central Bank Liquidity Swaps: 16-90 Day Maturity",
     "billions $", {"kind": "level", "watch": 0.5, "elevated": 10, "crisis": 25,
                     "convert_to_billions": True,
                     "note": "Khalid-spec — non-zero is FX dollar shortage abroad"}),
    ("SWPT", "liquidity_facilities", "Central Bank Liquidity Swaps: TOTAL",
     "billions $", {"kind": "level", "watch": 1.0, "elevated": 10, "crisis": 50,
                     "convert_to_billions": True,
                     "note": "Total swap lines — COVID peak was $446B"}),
    # Bank Term Funding Program / discount window other loans
    ("WLODL", "liquidity_facilities", "Other Liabilities & Capital: Other Liabilities (BTFP proxy)",
     "billions $", {"kind": "level", "watch": 200, "elevated": 400, "crisis": 800,
                     "convert_to_billions": True,
                     "note": "Includes BTFP and emergency lending program borrowing"}),

    # ════════════════════════════════════════════════════════════════════
    # CATEGORY 3: CREDIT SPREADS (ICE BofA OAS — daily, in % units)
    # ════════════════════════════════════════════════════════════════════

    # ─── US High Yield by credit quality ─────────────────────
    ("BAMLH0A0HYM2", "credit_spreads", "ICE BofA US High Yield Master OAS",
     "%", {"kind": "level", "watch": 5.0, "elevated": 7.0, "crisis": 10.0}),
    ("BAMLH0A1HYBB", "credit_spreads", "ICE BofA BB US High Yield OAS",
     "%", {"kind": "level", "watch": 4.0, "elevated": 5.5, "crisis": 8.0,
            "note": "Crossover credit — BB rated"}),
    ("BAMLH0A2HYB", "credit_spreads", "ICE BofA Single-B US High Yield OAS",
     "%", {"kind": "level", "watch": 6.0, "elevated": 8.0, "crisis": 12.0,
            "note": "Mid-tier junk"}),
    ("BAMLH0A3HYC", "credit_spreads", "ICE BofA CCC & Lower US High Yield OAS",
     "%", {"kind": "level", "watch": 9.0, "elevated": 12.0, "crisis": 18.0,
            "note": "Khalid-spec — riskiest US credit; GFC peak 22%, COVID peak 19%"}),

    # ─── US Investment Grade by credit quality ───────────────
    ("BAMLC0A0CM", "credit_spreads", "ICE BofA US Corporate IG OAS",
     "%", {"kind": "level", "watch": 1.5, "elevated": 2.5, "crisis": 4.0}),
    ("BAMLC0A1CAAA", "credit_spreads", "ICE BofA AAA US Corporate OAS",
     "%", {"kind": "level", "watch": 1.0, "elevated": 1.8, "crisis": 3.0,
            "note": "Highest-quality IG"}),
    ("BAMLC0A2CAA", "credit_spreads", "ICE BofA AA US Corporate OAS",
     "%", {"kind": "level", "watch": 1.2, "elevated": 2.0, "crisis": 3.5}),
    ("BAMLC0A3CA", "credit_spreads", "ICE BofA Single-A US Corporate OAS",
     "%", {"kind": "level", "watch": 1.4, "elevated": 2.2, "crisis": 3.8}),
    ("BAMLC0A4CBBB", "credit_spreads", "ICE BofA BBB US Corporate OAS",
     "%", {"kind": "level", "watch": 1.8, "elevated": 3.0, "crisis": 5.0,
            "note": "Lowest IG — most fallen-angel risk"}),

    # ─── Euro High Yield (only master available — no by-quality on FRED) ─
    ("BAMLHE00EHYIOAS", "credit_spreads", "ICE BofA Euro High Yield OAS",
     "%", {"kind": "level", "watch": 5.0, "elevated": 7.5, "crisis": 11.0,
            "note": "Khalid-spec — Euro HY; GFC peak 24%, COVID peak 11%"}),

    # ─── Emerging Market ─────────────────────────────────────
    ("BAMLEMHBHYCRPIOAS", "credit_spreads", "ICE BofA EM High Yield Corp Plus OAS",
     "%", {"kind": "level", "watch": 7.0, "elevated": 10.0, "crisis": 14.0,
            "note": "Khalid-spec — EM HY corp; GFC peak 17%, COVID peak 12%"}),
    ("BAMLEMCBPIOAS", "credit_spreads", "ICE BofA EM Corporate Plus OAS",
     "%", {"kind": "level", "watch": 4.0, "elevated": 6.0, "crisis": 10.0,
            "note": "Broad EM corp — IG + HY"}),

    # ════════════════════════════════════════════════════════════════════
    # CATEGORY 4: CORPORATE YIELDS (HQM + reference Treasuries)
    # ════════════════════════════════════════════════════════════════════
    ("HQMCB1YR", "corporate_yields", "HQM Corporate Bond Spot Rate (1y)",
     "%", {"kind": "spread_to", "vs": "DGS1", "watch": 1.0, "elevated": 2.0, "crisis": 3.5}),
    ("HQMCB2YR", "corporate_yields", "HQM Corporate Bond Spot Rate (2y)",
     "%", {"kind": "spread_to", "vs": "DGS2", "watch": 1.2, "elevated": 2.2, "crisis": 3.8}),
    ("HQMCB5YR", "corporate_yields", "HQM Corporate Bond Spot Rate (5y)",
     "%", {"kind": "spread_to", "vs": "DGS5", "watch": 1.4, "elevated": 2.4, "crisis": 4.0}),
    ("HQMCB10YR", "corporate_yields", "HQM Corporate Bond Spot Rate (10y)",
     "%", {"kind": "spread_to", "vs": "DGS10", "watch": 1.5, "elevated": 2.5, "crisis": 4.0,
            "note": "Khalid-spec — spread to 10y Treasury reveals corp credit demand"}),
    ("HQMCB30YR", "corporate_yields", "HQM Corporate Bond Spot Rate (30y)",
     "%", {"kind": "spread_to", "vs": "DGS30", "watch": 1.6, "elevated": 2.7, "crisis": 4.5}),

    # Reference Treasuries (for spread_to lookups)
    ("DGS1", "corporate_yields", "1-Year US Treasury", "%", {"kind": "level"}),
    ("DGS2", "corporate_yields", "2-Year US Treasury", "%", {"kind": "level"}),
    ("DGS5", "corporate_yields", "5-Year US Treasury", "%", {"kind": "level"}),
    ("DGS10", "corporate_yields", "10-Year US Treasury", "%", {"kind": "level"}),
    ("DGS30", "corporate_yields", "30-Year US Treasury", "%", {"kind": "level"}),

    # ════════════════════════════════════════════════════════════════════
    # CATEGORY 5: LENDING STANDARDS (SLOOS — Senior Loan Officer Survey)
    # Net % of banks tightening. Quarterly cadence. Positive = tightening.
    # Historical: 0% calm, 25% mid-cycle stress, 50%+ recession, 84% GFC peak.
    # ════════════════════════════════════════════════════════════════════

    # ─── C&I Loans (businesses) ──────────────────────────────
    ("DRTSCILM", "lending_standards", "C&I Loans: Tightening (Large/Middle Firms)",
     "% net", {"kind": "level", "watch": 10, "elevated": 25, "crisis": 50,
                "note": "GFC peak 83.6% — banks shutting credit to large companies"}),
    ("DRTSCIS", "lending_standards", "C&I Loans: Tightening (Small Firms)",
     "% net", {"kind": "level", "watch": 10, "elevated": 25, "crisis": 50,
                "note": "Small firms first to lose access in tightening cycle"}),
    ("DRSDCILM", "lending_standards", "C&I Loans: Demand (Large/Middle Firms)",
     "% net", {"kind": "level", "watch": -15, "elevated": -30, "crisis": -50,
                "note": "Negative = weak demand. Capex pullback signal"}),
    ("DRSDCIS", "lending_standards", "C&I Loans: Demand (Small Firms)",
     "% net", {"kind": "level", "watch": -15, "elevated": -30, "crisis": -50}),

    # ─── Commercial Real Estate (3 sub-types) ────────────────
    ("SUBLPDRCSC", "lending_standards", "CRE Tightening: Construction & Land Development",
     "% net", {"kind": "level", "watch": 15, "elevated": 35, "crisis": 65,
                "note": "C&LD tightening = developer credit shutoff"}),
    ("SUBLPDRCSN", "lending_standards", "CRE Tightening: Nonfarm Nonresidential",
     "% net", {"kind": "level", "watch": 10, "elevated": 30, "crisis": 60,
                "note": "Office, retail, industrial CRE"}),
    ("SUBLPDRCSM", "lending_standards", "CRE Tightening: Multifamily Residential",
     "% net", {"kind": "level", "watch": 10, "elevated": 30, "crisis": 60}),

    # ─── Residential Real Estate ─────────────────────────────
    ("DRTSSP", "lending_standards", "Mortgage: Subprime Tightening Standards",
     "% net", {"kind": "level", "watch": 15, "elevated": 35, "crisis": 60}),

    # ─── Consumer ────────────────────────────────────────────
    ("DRTSCLCC", "lending_standards", "Credit Cards: Tightening Standards",
     "% net", {"kind": "level", "watch": 10, "elevated": 25, "crisis": 45,
                "note": "Credit card tightening = consumer credit squeeze"}),
    ("STDSAUTO", "lending_standards", "Auto Loans: Tightening Standards",
     "% net", {"kind": "level", "watch": 10, "elevated": 25, "crisis": 45}),
    ("STDSOTHCONS", "lending_standards", "Other Consumer Loans: Tightening Standards",
     "% net", {"kind": "level", "watch": 10, "elevated": 25, "crisis": 45,
                "note": "Personal loans, installment loans excluding credit card/auto"}),

    # ─── Special: Willingness to lend ────────────────────────
    ("DRIWCIL", "lending_standards", "Willingness to Make Consumer Installment Loans",
     "% net", {"kind": "level", "watch": -10, "elevated": -25, "crisis": -50,
                "note": "Negative = banks pulling back from consumer lending"}),
]

# ────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────
def _now_iso():
    return datetime.now(timezone.utc).isoformat()


def fred_observations(series_id, days=400):
    """Pull last N days of observations from FRED with retry-on-429.

    FRED limits to ~120 req/min per key; concurrent Lambdas (LCE + other engines)
    can push past it transiently. Retry up to 4 times with exponential backoff +
    jitter so a 429 storm doesn't mark a valid series as unavailable.
    """
    import random
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=days)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
           f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
           f"&observation_start={start.isoformat()}"
           f"&observation_end={end.isoformat()}"
           f"&sort_order=asc")
    for attempt in range(5):  # 0..4 — up to 5 attempts
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-LCE/1.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read().decode("utf-8"))
            obs = []
            for o in data.get("observations", []):
                v = o.get("value")
                if v in (".", "", None):
                    continue
                try:
                    obs.append({"date": o["date"], "value": float(v)})
                except (ValueError, TypeError):
                    continue
            return obs
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 4:
                wait = min(8, (2 ** attempt) + random.uniform(0, 1))
                print(f"[lce] fred {series_id} HTTP {e.code}, retry in {wait:.1f}s (attempt {attempt+1}/5)")
                time.sleep(wait)
                continue
            print(f"[lce] fred {series_id} HTTP {e.code}: giving up after {attempt+1} attempts")
            return []
        except Exception as e:
            if attempt < 2:
                time.sleep(1.0 + random.uniform(0, 0.5))
                continue
            print(f"[lce] fred {series_id} error: {e}")
            return []
    return []


def fred_observations_long(series_id, days=1900):
    """5-year window for z-score calc."""
    return fred_observations(series_id, days=days)


def find_value_n_back(obs, days_back):
    """Find the observation closest to N days before the latest one (within ±3 days)."""
    if not obs:
        return None
    latest_date = datetime.fromisoformat(obs[-1]["date"]).date()
    target = latest_date - timedelta(days=days_back)
    best = None
    best_dist = float("inf")
    for o in obs:
        d = datetime.fromisoformat(o["date"]).date()
        dist = abs((d - target).days)
        if dist < best_dist:
            best_dist = dist
            best = o
    if best and best_dist <= max(3, days_back * 0.10):  # 10% tolerance, min 3 days
        return best
    return None


def pct_change(latest, prior):
    if latest is None or prior is None or prior == 0:
        return None
    return (latest - prior) / abs(prior) * 100.0


def z_score(latest, history):
    """Z-score of latest value relative to a history window."""
    vals = [h["value"] for h in history if h.get("value") is not None]
    if len(vals) < 30:
        return None
    m = mean(vals)
    s = pstdev(vals)
    if s == 0:
        return None
    return (latest - m) / s


# ────────────────────────────────────────────────────────────────────────
# Per-series compute
# ────────────────────────────────────────────────────────────────────────
def compute_series(series_id, threshold, ref_yields=None):
    """Pull a series and compute the full feature set.

    ref_yields: dict mapping FRED ID → latest_value, used for `spread_to` thresholds.
    """
    obs = fred_observations_long(series_id)
    if not obs:
        return {"available": False, "error": f"No data for {series_id}"}

    latest = obs[-1]
    latest_value_raw = latest["value"]  # FRED native units
    latest_date = latest["date"]

    # FRED reports balance-sheet and liquidity-facility series in MILLIONS of $.
    # We want to compare against thresholds expressed in BILLIONS. Convert here
    # but only for series where the threshold "kind" is level/delta_pct AND the
    # category is balance_sheet or liquidity_facilities. Credit spreads (OAS)
    # and yields are reported in % which we keep as-is.
    convert_to_billions = threshold.get("convert_to_billions", False)
    latest_value = latest_value_raw / 1000.0 if convert_to_billions else latest_value_raw

    # Period changes (calendar days) — % change is unit-invariant
    wow = find_value_n_back(obs, 7)
    mom = find_value_n_back(obs, 30)
    qoq = find_value_n_back(obs, 90)
    yoy = find_value_n_back(obs, 365)

    wow_pct = pct_change(latest_value_raw, wow["value"]) if wow else None
    mom_pct = pct_change(latest_value_raw, mom["value"]) if mom else None
    qoq_pct = pct_change(latest_value_raw, qoq["value"]) if qoq else None
    yoy_pct = pct_change(latest_value_raw, yoy["value"]) if yoy else None

    # Z-scores (use raw values; z is unit-invariant)
    z1y_window = [o for o in obs
                   if (datetime.fromisoformat(latest_date).date()
                        - datetime.fromisoformat(o["date"]).date()).days <= 365]
    z1y = z_score(latest_value_raw, z1y_window)
    z5y = z_score(latest_value_raw, obs)

    # Signal classification
    signal = "NORMAL"
    signal_reason = ""
    kind = threshold.get("kind", "level")

    if kind == "level":
        # Direction inferred from sign of any non-null threshold:
        #   positive thresholds → "high is bad" (alert when value >= threshold)
        #   negative thresholds → "low is bad"  (alert when value <= threshold)
        crisis_t = threshold.get("crisis")
        elevated_t = threshold.get("elevated")
        watch_t = threshold.get("watch")
        sign_ref = next((t for t in [watch_t, elevated_t, crisis_t] if t is not None), 0)
        is_low_bad = sign_ref < 0

        if is_low_bad:
            if crisis_t is not None and latest_value <= crisis_t:
                signal = "CRISIS"; signal_reason = f"Level {latest_value:.2f} ≤ crisis {crisis_t}"
            elif elevated_t is not None and latest_value <= elevated_t:
                signal = "ELEVATED"; signal_reason = f"Level {latest_value:.2f} ≤ elevated {elevated_t}"
            elif watch_t is not None and latest_value <= watch_t:
                signal = "WATCH"; signal_reason = f"Level {latest_value:.2f} ≤ watch {watch_t}"
        else:
            if crisis_t is not None and latest_value >= crisis_t:
                signal = "CRISIS"; signal_reason = f"Level {latest_value:.2f} ≥ crisis {crisis_t}"
            elif elevated_t is not None and latest_value >= elevated_t:
                signal = "ELEVATED"; signal_reason = f"Level {latest_value:.2f} ≥ elevated {elevated_t}"
            elif watch_t is not None and latest_value >= watch_t:
                signal = "WATCH"; signal_reason = f"Level {latest_value:.2f} ≥ watch {watch_t}"

    elif kind == "delta_pct":
        window = threshold.get("window", "wk")
        delta = wow_pct if window == "wk" else mom_pct
        if delta is not None:
            crisis_t = threshold.get("crisis")
            elevated_t = threshold.get("elevated")
            watch_t = threshold.get("watch")
            sign_ref = next((t for t in [watch_t, elevated_t, crisis_t] if t is not None), 0)
            is_drop_alert = sign_ref < 0

            if is_drop_alert:
                if crisis_t is not None and delta <= crisis_t:
                    signal = "CRISIS"; signal_reason = f"{window} delta {delta:+.2f}% ≤ crisis {crisis_t:+.2f}%"
                elif elevated_t is not None and delta <= elevated_t:
                    signal = "ELEVATED"; signal_reason = f"{window} delta {delta:+.2f}% ≤ elevated {elevated_t:+.2f}%"
                elif watch_t is not None and delta <= watch_t:
                    signal = "WATCH"; signal_reason = f"{window} delta {delta:+.2f}% ≤ watch {watch_t:+.2f}%"
            else:
                if crisis_t is not None and delta >= crisis_t:
                    signal = "CRISIS"; signal_reason = f"{window} delta {delta:+.2f}% ≥ crisis {crisis_t:+.2f}%"
                elif elevated_t is not None and delta >= elevated_t:
                    signal = "ELEVATED"; signal_reason = f"{window} delta {delta:+.2f}% ≥ elevated {elevated_t:+.2f}%"
                elif watch_t is not None and delta >= watch_t:
                    signal = "WATCH"; signal_reason = f"{window} delta {delta:+.2f}% ≥ watch {watch_t:+.2f}%"

    elif kind == "spread_to":
        # Generalized spread lookup — uses ref_yields dict to find reference value
        ref = threshold.get("vs")
        ref_value = (ref_yields or {}).get(ref) if ref else None
        if ref_value is not None:
            spread = latest_value - ref_value
            if "crisis" in threshold and spread >= threshold["crisis"]:
                signal = "CRISIS"; signal_reason = f"Spread to {ref} {spread:+.2f}% ≥ crisis {threshold['crisis']}%"
            elif "elevated" in threshold and spread >= threshold["elevated"]:
                signal = "ELEVATED"; signal_reason = f"Spread to {ref} {spread:+.2f}% ≥ elevated {threshold['elevated']}%"
            elif "watch" in threshold and spread >= threshold["watch"]:
                signal = "WATCH"; signal_reason = f"Spread to {ref} {spread:+.2f}% ≥ watch {threshold['watch']}%"

    return {
        "available": True,
        "latest_date": latest_date,
        "latest_value": round(latest_value, 4),
        "latest_value_raw": round(latest_value_raw, 4),
        "wow_pct": round(wow_pct, 3) if wow_pct is not None else None,
        "mom_pct": round(mom_pct, 3) if mom_pct is not None else None,
        "qoq_pct": round(qoq_pct, 3) if qoq_pct is not None else None,
        "yoy_pct": round(yoy_pct, 3) if yoy_pct is not None else None,
        "z_1y": round(z1y, 2) if z1y is not None else None,
        "z_5y": round(z5y, 2) if z5y is not None else None,
        "signal": signal,
        "signal_reason": signal_reason,
        "n_observations": len(obs),
    }


def composite_signal(by_id):
    """Composite stress score (0-100) from worst-of and average."""
    rank = {"NORMAL": 0, "WATCH": 25, "ELEVATED": 60, "CRISIS": 90}
    scores = []
    n_firing_by_cat = {}
    for sid, info in by_id.items():
        if not info.get("available"):
            continue
        sig = info.get("signal", "NORMAL")
        scores.append(rank.get(sig, 0))
        cat = info.get("_category", "other")
        if sig in ("ELEVATED", "CRISIS"):
            n_firing_by_cat[cat] = n_firing_by_cat.get(cat, 0) + 1
    if not scores:
        return {"score": 0, "n_firing": 0, "by_category": {}}
    composite = round(max(scores) * 0.7 + (sum(scores) / len(scores)) * 0.3, 1)
    return {
        "score": composite,
        "n_firing": sum(1 for s in scores if s >= 60),
        "by_category": n_firing_by_cat,
    }


def regime_classification(composite, by_id):
    """Coarse regime: CALM / WATCH / ELEVATED / ACUTE_STRESS / CRISIS."""
    score = composite["score"]
    if score >= 80:
        return "CRISIS"
    if score >= 60:
        return "ACUTE_STRESS"
    if score >= 35:
        return "ELEVATED"
    if score >= 15:
        return "WATCH"
    return "CALM"


def load_prior():
    try:
        obj = S3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read())
    except Exception:
        return {}


def detect_transitions(current, prior):
    """Return state-transition entries for alert-router."""
    transitions = []
    cur_series = current.get("series", {})
    prior_series = (prior or {}).get("series", {})
    for sid, info in cur_series.items():
        if not info.get("available"):
            continue
        prior_info = prior_series.get(sid, {})
        prior_sig = prior_info.get("signal", "NORMAL")
        new_sig = info.get("signal", "NORMAL")
        if new_sig != prior_sig:
            transitions.append({
                "series_id": sid,
                "label": info.get("_label"),
                "category": info.get("_category"),
                "prior": prior_sig,
                "new": new_sig,
                "latest_value": info.get("latest_value"),
                "wow_pct": info.get("wow_pct"),
                "z_1y": info.get("z_1y"),
                "reason": info.get("signal_reason"),
            })
    return transitions


# ────────────────────────────────────────────────────────────────────────
# INTERPRETATION ENGINE
# Translates the 51-series LCE state into:
#   - Per-pillar narrative (liquidity / credit / lending)
#   - Cross-asset implications (stocks / bonds / gold / dollar / btc / hy / em)
#   - Concrete portfolio target allocation with rationale
#   - Explicit hedges + assets to avoid
#   - Key risks that would change the call
# Output is baked into the LCE JSON so morning-intel, ai-chat, allocator,
# and the /lce.html page all read the same coherent interpretation.
# ────────────────────────────────────────────────────────────────────────
RANK = {"NORMAL": 0, "WATCH": 1, "ELEVATED": 2, "CRISIS": 3}


def _worst_in_cat(by_cat, series, cat):
    worst = "NORMAL"
    for sid in by_cat.get(cat, []):
        sig = (series.get(sid) or {}).get("signal", "NORMAL")
        if RANK.get(sig, 0) > RANK.get(worst, 0):
            worst = sig
    return worst


def _v(series, sid):
    return (series.get(sid) or {}).get("latest_value")


def _firing(series, sid):
    return (series.get(sid) or {}).get("signal", "NORMAL") in ("WATCH", "ELEVATED", "CRISIS")


def interpret_state(output):
    """Produce a coherent interpretation + portfolio recommendation from LCE state.
       Returns a dict with liquidity/credit/lending narrative, cross-asset signals,
       target portfolio allocation, hedges, avoids, key risks, decisive call."""
    series = output.get("series", {})
    by_cat = output.get("by_category", {})
    regime = output.get("regime", "NORMAL")
    composite = (output.get("composite") or {}).get("score", 0)

    # ── Pillar states ──
    bs_worst = _worst_in_cat(by_cat, series, "balance_sheet")
    fac_worst = _worst_in_cat(by_cat, series, "liquidity_facilities")
    cs_worst = _worst_in_cat(by_cat, series, "credit_spreads")
    cy_worst = _worst_in_cat(by_cat, series, "corporate_yields")
    ls_worst = _worst_in_cat(by_cat, series, "lending_standards")

    liq_rank = max(RANK.get(bs_worst, 0), RANK.get(fac_worst, 0))
    cr_rank = max(RANK.get(cs_worst, 0), RANK.get(cy_worst, 0))
    ls_rank = RANK.get(ls_worst, 0)

    state_label = {0: "ABUNDANT", 1: "DRAINING", 2: "STRESSED", 3: "CRISIS"}
    state_label_credit = {0: "HEALTHY", 1: "WIDENING", 2: "STRESSED", 3: "PANIC"}
    state_label_lending = {0: "EASY", 1: "MILD_TIGHTENING", 2: "TIGHTENING", 3: "RECESSION_LEVEL"}

    liq_state = state_label[liq_rank]
    credit_state = state_label_credit[cr_rank]
    lending_state = state_label_lending[ls_rank]

    # ── Specific drivers per pillar (only include firing) ──
    liq_drivers = []
    walcl_wow = (series.get("WALCL") or {}).get("wow_pct")
    wresbal_wow = (series.get("WRESBAL") or {}).get("wow_pct")
    tga = _v(series, "WTREGEN")
    rrp = _v(series, "RRPONTSYD")
    primary_credit = _v(series, "OTHL1690")
    cb_swaps = _v(series, "SWPT")
    btfp_proxy = _v(series, "WLODL")
    discount = _v(series, "DPCREDIT")

    if walcl_wow is not None and walcl_wow < -0.3:
        liq_drivers.append(f"Fed balance sheet shrinking {walcl_wow:.2f}%/wk (active QT)")
    if wresbal_wow is not None and wresbal_wow < -1.5:
        liq_drivers.append(f"Bank reserves draining {wresbal_wow:.1f}%/wk")
    if tga and tga > 800:
        liq_drivers.append(f"TGA ${tga:.0f}B above $800B watch — Treasury issuance draining liquidity")
    if rrp is not None and rrp < 200:
        liq_drivers.append(f"RRP ${rrp:.0f}B near-zero — MMF backstop nearly exhausted")
    if primary_credit and primary_credit > 0.5:
        liq_drivers.append(f"Primary credit ${primary_credit:.2f}B — banks accessing discount window")
    if cb_swaps and cb_swaps > 1:
        liq_drivers.append(f"CB swap lines ${cb_swaps:.1f}B drawn — FX dollar shortage abroad")
    if btfp_proxy and btfp_proxy > 200:
        liq_drivers.append(f"Other liabilities ${btfp_proxy:.0f}B (BTFP-proxy) elevated")
    if discount and discount > 2:
        liq_drivers.append(f"Discount window ${discount:.1f}B — funding stress")

    cr_drivers = []
    hy_master = _v(series, "BAMLH0A0HYM2")
    ccc = _v(series, "BAMLH0A3HYC")
    bb = _v(series, "BAMLH0A1HYBB")
    b = _v(series, "BAMLH0A2HYB")
    bbb = _v(series, "BAMLC0A4CBBB")
    ig = _v(series, "BAMLC0A0CM")
    aaa = _v(series, "BAMLC0A1CAAA")
    euro_hy = _v(series, "BAMLHE00EHYIOAS")
    em_hy = _v(series, "BAMLEMHBHYCRPIOAS")

    if ccc and ccc > 9:
        cr_drivers.append(f"CCC HY OAS {ccc:.2f}% (canary stressed; GFC peak 22%, COVID 19%)")
    if hy_master and hy_master > 5:
        cr_drivers.append(f"US HY OAS {hy_master:.2f}%")
    if bbb and bbb > 1.8:
        cr_drivers.append(f"BBB OAS {bbb:.2f}% (fallen-angel watch)")
    if euro_hy and euro_hy > 5:
        cr_drivers.append(f"Euro HY OAS {euro_hy:.2f}%")
    if em_hy and em_hy > 7:
        cr_drivers.append(f"EM HY OAS {em_hy:.2f}%")
    # Quality migration check
    if bb and b and (b - bb) > 3:
        cr_drivers.append(f"B-BB spread compression {(b-bb):.2f}% (quality differentiation)")

    ls_drivers = []
    ci_lg = _v(series, "DRTSCILM")
    ci_sm = _v(series, "DRTSCIS")
    ci_dem_lg = _v(series, "DRSDCILM")
    cre_construction = _v(series, "SUBLPDRCSC")
    cre_nonres = _v(series, "SUBLPDRCSN")
    cre_multi = _v(series, "SUBLPDRCSM")
    cc_tightening = _v(series, "DRTSCLCC")
    auto_tightening = _v(series, "STDSAUTO")
    willingness = _v(series, "DRIWCIL")

    if ci_lg is not None and ci_lg > 10:
        ls_drivers.append(f"C&I large {ci_lg:.0f}% net tightening")
    if ci_sm is not None and ci_sm > 10:
        ls_drivers.append(f"C&I small {ci_sm:.0f}%")
    if ci_dem_lg is not None and ci_dem_lg < -15:
        ls_drivers.append(f"C&I demand large {ci_dem_lg:.0f}% (capex pullback)")
    if cre_construction is not None and cre_construction > 15:
        ls_drivers.append(f"CRE construction {cre_construction:.0f}%")
    if cre_nonres is not None and cre_nonres > 10:
        ls_drivers.append(f"CRE nonfarm/nonres {cre_nonres:.0f}%")
    if cc_tightening is not None and cc_tightening > 10:
        ls_drivers.append(f"Credit cards {cc_tightening:.0f}%")
    if auto_tightening is not None and auto_tightening > 10:
        ls_drivers.append(f"Auto loans {auto_tightening:.0f}%")
    if willingness is not None and willingness < -10:
        ls_drivers.append(f"Consumer credit willingness {willingness:.0f}% (pulling back)")

    # ── Build narratives ──
    if liq_state == "ABUNDANT":
        liq_narrative = ("System liquidity is abundant. Bank reserves stable, no drawdown of "
                          "emergency facilities, TGA in normal range. Fed balance sheet trajectory benign.")
    elif liq_state == "DRAINING":
        liq_narrative = "Liquidity is gradually tightening — early-cycle stress. " + " · ".join(liq_drivers[:3])
    elif liq_state == "STRESSED":
        liq_narrative = "Funding stress emerging — multiple elevated metrics. " + " · ".join(liq_drivers[:4])
    else:
        liq_narrative = ("Acute liquidity crisis — emergency facilities active. " + " · ".join(liq_drivers[:5]))

    if credit_state == "HEALTHY":
        cr_narrative = ("Corporate credit spreads tight across all qualities. Risk appetite for "
                         "credit intact, no quality migration, defaults quiet.")
    elif credit_state == "WIDENING":
        cr_narrative = ("Credit spreads widening at the bottom of the quality spectrum first — "
                         "early-cycle differentiation. " + " · ".join(cr_drivers[:3]))
    elif credit_state == "STRESSED":
        cr_narrative = "Significant credit stress — quality migration evident. " + " · ".join(cr_drivers[:4])
    else:
        cr_narrative = "Credit panic — distressed pricing across HY and IG. " + " · ".join(cr_drivers[:5])

    if lending_state == "EASY":
        ls_narrative = ("Banks at or below midpoint on standards across categories. Credit "
                         "channel functional; loan demand mixed but not collapsing.")
    elif lending_state == "MILD_TIGHTENING":
        ls_narrative = "Modest tightening cycle starting. " + " · ".join(ls_drivers[:3])
    elif lending_state == "TIGHTENING":
        ls_narrative = ("Clear tightening cycle — historically recession-prone. "
                         + " · ".join(ls_drivers[:4]))
    else:
        ls_narrative = ("Recession-level tightening across loan categories. "
                         + " · ".join(ls_drivers[:5]))

    # ── Cross-asset signals (-2 short hard, -1 underweight, 0 neutral, +1 overweight, +2 long hard) ──
    overall_rank = max(liq_rank, cr_rank, ls_rank, RANK.get(regime, 0))
    cross_asset = {}

    # US LARGE EQUITY
    if overall_rank >= 3:
        cross_asset["us_large_equity"] = {"signal": -2, "rationale": "Crisis regime — risk-asset shock probable"}
    elif overall_rank == 2:
        cross_asset["us_large_equity"] = {"signal": -1, "rationale": "Stressed liquidity/credit — multiple compression risk"}
    elif overall_rank == 1:
        cross_asset["us_large_equity"] = {"signal": 0,
                                            "rationale": "Watch state — maintain core but tilt cautious"}
    else:
        cross_asset["us_large_equity"] = {"signal": +1, "rationale": "Liquidity abundant — risk-on supported"}

    # US SMALL CAP — most rate/credit-sensitive
    if overall_rank >= 2 or ls_rank >= 2:
        cross_asset["us_small_cap"] = {"signal": -2, "rationale": "Small caps most exposed to bank-tightening + funding stress"}
    elif overall_rank >= 1 or ls_rank >= 1:
        cross_asset["us_small_cap"] = {"signal": -1, "rationale": "Bank-tightening cycle starting hits IWM first"}
    else:
        cross_asset["us_small_cap"] = {"signal": +1, "rationale": "Easy-money regime favors small caps"}

    # LONG DURATION (TLT)
    if overall_rank >= 2 and (cr_rank >= 2 or ls_rank >= 2):
        cross_asset["long_duration"] = {"signal": +2, "rationale": "Flight-to-quality + recession bid for duration"}
    elif overall_rank >= 1:
        cross_asset["long_duration"] = {"signal": +1, "rationale": "Defensive bid building"}
    else:
        cross_asset["long_duration"] = {"signal": 0, "rationale": "No clear duration signal"}

    # GOLD
    if overall_rank >= 2:
        cross_asset["gold"] = {"signal": +2, "rationale": "Liquidity/credit stress + rate cuts ahead"}
    elif overall_rank >= 1:
        cross_asset["gold"] = {"signal": +1, "rationale": "Hedge against widening stress"}
    else:
        cross_asset["gold"] = {"signal": 0, "rationale": "Tactical only"}

    # DOLLAR (UUP)
    if liq_rank >= 2 or (cb_swaps and cb_swaps > 5):
        cross_asset["dollar"] = {"signal": +2, "rationale": "FX dollar shortage / safe-haven bid"}
    elif overall_rank >= 1:
        cross_asset["dollar"] = {"signal": +1, "rationale": "Defensive USD bid in tightening cycle"}
    else:
        cross_asset["dollar"] = {"signal": -1, "rationale": "Easy liquidity weakens USD"}

    # HIGH YIELD CREDIT (HYG)
    if cr_rank >= 2 or ls_rank >= 2:
        cross_asset["high_yield"] = {"signal": -2, "rationale": "Spreads widening + bank tightening = HY drawdown risk"}
    elif cr_rank >= 1 or ls_rank >= 1:
        cross_asset["high_yield"] = {"signal": -1, "rationale": "Credit cycle deteriorating — trim HY"}
    else:
        cross_asset["high_yield"] = {"signal": +1, "rationale": "Tight spreads + carry support HYG"}

    # EMERGING MARKETS
    if liq_rank >= 2 or (cb_swaps and cb_swaps > 5):
        cross_asset["emerging_markets"] = {"signal": -2, "rationale": "EM hit hardest by USD shortage + risk-off"}
    elif overall_rank >= 1:
        cross_asset["emerging_markets"] = {"signal": -1, "rationale": "Tightening USD liquidity weighs on EM"}
    else:
        cross_asset["emerging_markets"] = {"signal": +1, "rationale": "Risk-on regime favors EM"}

    # BITCOIN
    if overall_rank >= 3:
        cross_asset["bitcoin"] = {"signal": -2, "rationale": "Acute liquidity crisis — BTC initially sells off (then rallies)"}
    elif overall_rank >= 2:
        cross_asset["bitcoin"] = {"signal": -1, "rationale": "Risk-off; BTC follows risk assets short-term"}
    elif overall_rank >= 1:
        cross_asset["bitcoin"] = {"signal": 0, "rationale": "Mixed — volatile around watch state"}
    else:
        cross_asset["bitcoin"] = {"signal": +2, "rationale": "Liquidity abundant + risk-on = BTC tailwind"}

    # ── Portfolio target allocation ──
    # Baseline (CALM/risk-on): 55% equity, 20% duration, 5% gold, 5% BTC, 5% HY, 10% cash
    # Tilts come from cross_asset signals.
    if overall_rank == 0:
        # CALM
        posture = "RISK_ON"
        allocations = [
            {"ticker": "SPY", "weight_pct": 28, "rationale": "Core US large-cap; liquidity abundant"},
            {"ticker": "QQQ", "weight_pct": 18, "rationale": "Tech growth on easy-money regime"},
            {"ticker": "IWM", "weight_pct": 6, "rationale": "Small-cap upside in easy-credit cycle"},
            {"ticker": "EFA", "weight_pct": 5, "rationale": "DM intl diversifier"},
            {"ticker": "EEM", "weight_pct": 6, "rationale": "EM benefits from USD softness + risk-on"},
            {"ticker": "BTC", "weight_pct": 8, "rationale": "Liquidity tailwind for crypto"},
            {"ticker": "HYG", "weight_pct": 6, "rationale": "Tight spreads + carry"},
            {"ticker": "IEF", "weight_pct": 8, "rationale": "Mid-curve duration anchor"},
            {"ticker": "GLD", "weight_pct": 5, "rationale": "Strategic 5% gold hold"},
            {"ticker": "CASH", "weight_pct": 10, "rationale": "Dry powder"},
        ]
        avoid = []
        hedges = []
    elif overall_rank == 1:
        # WATCH — current state
        posture = "BALANCED_CAUTIOUS"
        allocations = [
            {"ticker": "SPY", "weight_pct": 22, "rationale": "Core, modestly trimmed"},
            {"ticker": "QQQ", "weight_pct": 14, "rationale": "Mega-cap quality preferred over breadth"},
            {"ticker": "IWM", "weight_pct": 3, "rationale": "Underweight — small caps vulnerable to bank tightening"},
            {"ticker": "EFA", "weight_pct": 5, "rationale": "DM intl unchanged"},
            {"ticker": "EEM", "weight_pct": 4, "rationale": "Modest underweight on USD tailwind"},
            {"ticker": "BTC", "weight_pct": 5, "rationale": "Trim crypto on watch state"},
            {"ticker": "HYG", "weight_pct": 3, "rationale": "Underweight — credit spreads at watch level"},
            {"ticker": "TLT", "weight_pct": 8, "rationale": "Add long duration as defensive bid"},
            {"ticker": "IEF", "weight_pct": 10, "rationale": "Mid-curve overweight"},
            {"ticker": "GLD", "weight_pct": 8, "rationale": "Increase gold hedge"},
            {"ticker": "UUP", "weight_pct": 3, "rationale": "Modest USD hedge"},
            {"ticker": "CASH", "weight_pct": 15, "rationale": "Raise cash from 10% baseline"},
        ]
        avoid = ["KRE (regional banks)", "Junk-rated CCC names", "Frontier-market debt"]
        hedges = ["GLD (8%)", "TLT (8%)", "Cash (15%)"]
    elif overall_rank == 2:
        # ELEVATED
        posture = "DEFENSIVE_TILT"
        allocations = [
            {"ticker": "SPY", "weight_pct": 15, "rationale": "Underweight — multiple compression risk"},
            {"ticker": "QQQ", "weight_pct": 10, "rationale": "Mega-cap quality only"},
            {"ticker": "IWM", "weight_pct": 0, "rationale": "EXIT small caps — bank-tightening exposure"},
            {"ticker": "EEM", "weight_pct": 0, "rationale": "EXIT EM — USD shortage hits hardest"},
            {"ticker": "EFA", "weight_pct": 3, "rationale": "Partial DM intl"},
            {"ticker": "BTC", "weight_pct": 3, "rationale": "Trim crypto significantly"},
            {"ticker": "HYG", "weight_pct": 0, "rationale": "EXIT high yield — spread widening"},
            {"ticker": "TLT", "weight_pct": 18, "rationale": "Long duration overweight"},
            {"ticker": "IEF", "weight_pct": 12, "rationale": "Mid-curve safety"},
            {"ticker": "GLD", "weight_pct": 12, "rationale": "Strategic gold overweight"},
            {"ticker": "UUP", "weight_pct": 7, "rationale": "USD strength on tightening"},
            {"ticker": "CASH", "weight_pct": 20, "rationale": "Significant cash position"},
        ]
        avoid = ["IWM/SP600", "HYG/JNK", "EEM", "FXI/KWEB", "KRE", "URA",
                  "BDCs (TCPC, ARCC, etc — bank-tightening exposure)",
                  "Office REITs (BXMT, ARI)"]
        hedges = ["TLT (18%)", "GLD (12%)", "UUP (7%)", "Cash (20%)"]
    else:
        # CRISIS / ACUTE_STRESS
        posture = "DEFENSIVE_BUNKER"
        allocations = [
            {"ticker": "SPY", "weight_pct": 5, "rationale": "Minimal equity — opportunistic only"},
            {"ticker": "QQQ", "weight_pct": 0, "rationale": "EXIT growth"},
            {"ticker": "TLT", "weight_pct": 28, "rationale": "Heavy long duration — flight to quality"},
            {"ticker": "IEF", "weight_pct": 12, "rationale": "Mid-curve safety"},
            {"ticker": "GLD", "weight_pct": 22, "rationale": "Heavy gold — central-bank rate cuts coming"},
            {"ticker": "UUP", "weight_pct": 13, "rationale": "USD strength on liquidity stress"},
            {"ticker": "VXX", "weight_pct": 5, "rationale": "Vol hedge"},
            {"ticker": "CASH", "weight_pct": 15, "rationale": "Liquidity for opportunistic adds"},
        ]
        avoid = ["ALL EQUITY EX MEGA-CAP", "ALL HY", "ALL EM", "BTC", "BDCs",
                  "Regional banks", "REITs", "Energy MLPs", "Frontier debt"]
        hedges = ["TLT (28%)", "GLD (22%)", "UUP (13%)", "VXX (5%)", "Cash (15%)"]

    # ── Decisive call (one-sentence) ──
    if overall_rank == 0:
        decisive = (f"RISK-ON OK · liquidity {liq_state.lower()} · credit {credit_state.lower()} · "
                    f"banks {lending_state.lower().replace('_', ' ')} — maintain full risk-asset allocation, "
                    f"add carry trades, watch for any escalation in firing series.")
    elif overall_rank == 1:
        decisive = (f"MONITOR · {liq_state.lower()} liquidity (composite {composite}/100) "
                    f"+ {credit_state.lower()} credit + {lending_state.lower().replace('_',' ')} bank standards — "
                    f"trim small caps, EM, and HY by 30%, raise cash to 15%, increase gold and duration "
                    f"as the canary signals (CCC HY OAS, TGA, primary credit) are already at watch level.")
    elif overall_rank == 2:
        decisive = (f"DEFENSIVE TILT · {liq_state.lower()} liquidity + {credit_state.lower()} credit "
                    f"+ {lending_state.lower().replace('_',' ')} lending — exit small caps, EM, HY entirely, "
                    f"shift 40-50% of portfolio to TLT/GLD/UUP/cash, hedge equity beta with VXX or puts.")
    else:
        decisive = (f"CRISIS POSTURE · acute stress across pillars (composite {composite}/100) — "
                    f"reduce equity to <10%, deploy heavy duration + gold + dollar + cash, expect "
                    f"opportunistic re-entry only after credit spreads + reserves stabilize for 2+ weeks.")

    # ── Key risks (things that would change the call) ──
    key_risks = []
    if overall_rank == 0:
        key_risks = ["TGA crossing $800B (Treasury issuance front-loaded)",
                      "CCC HY OAS crossing 9% (canary for risk appetite)",
                      "C&I large tightening crossing +10% in next SLOOS",
                      "Primary credit usage > $0.5B for two consecutive weeks"]
    elif overall_rank == 1:
        key_risks = ["CCC HY OAS crossing 12% (escalation to ELEVATED)",
                      "Bank reserves dropping >2%/wk (QT acceleration)",
                      "CB swap lines drawn (FX dollar shortage)",
                      "Primary credit > $5B (banks systemically borrowing)",
                      "Next SLOOS showing >25% net tightening on C&I or CRE"]
    elif overall_rank == 2:
        key_risks = ["CCC HY OAS crossing 18% (panic level)",
                      "Bank reserves dropping >5%/wk",
                      "BTFP-proxy or emergency facilities expanding rapidly",
                      "Quality migration accelerating (BBB widening to 3%+)"]
    else:
        key_risks = ["Watch for credit-spread peak + reverse for re-entry signal",
                      "Bank reserves stabilizing for 2+ weeks",
                      "Fed pivot announcement",
                      "TGA drawdown / new issuance pause"]

    # ── Confidence in the call ──
    # High confidence if multiple pillars agree; lower if mixed.
    pillars_agreeing = sum(1 for r in [liq_rank, cr_rank, ls_rank] if r == overall_rank)
    if pillars_agreeing >= 2:
        confidence = "HIGH"
    elif pillars_agreeing == 1 and overall_rank > 0:
        confidence = "MEDIUM"
    else:
        confidence = "LOW"

    return {
        "as_of": output.get("generated_at"),
        "regime": regime,
        "composite_score": composite,
        "overall_posture": posture,
        "confidence": confidence,
        "pillars": {
            "liquidity": {"state": liq_state, "rank": liq_rank, "narrative": liq_narrative,
                            "drivers": liq_drivers},
            "credit":    {"state": credit_state, "rank": cr_rank, "narrative": cr_narrative,
                            "drivers": cr_drivers},
            "lending":   {"state": lending_state, "rank": ls_rank, "narrative": ls_narrative,
                            "drivers": ls_drivers},
        },
        "cross_asset": cross_asset,
        "target_allocation": allocations,
        "avoid": avoid,
        "hedges": hedges,
        "key_risks": key_risks,
        "decisive_call": decisive,
    }


# ────────────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────────────
def _emit_engine_error(error: Exception, phase: str = "handler"):
    """Best-effort engine.error event. Never raises."""
    try:
        from system_events import publish, EVT_ENGINE_ERROR
        import traceback
        publish(EVT_ENGINE_ERROR, {
            "engine":   "justhodl-liquidity-credit-engine",
            "phase":    phase,
            "error":    f"{type(error).__name__}: {str(error)[:200]}",
            "traceback": traceback.format_exc()[-400:],
        }, source_engine="liquidity-credit-engine")
    except Exception:
        pass


def lambda_handler(event=None, context=None):
    """Top-level safety net. Captures any uncaught exception, emits an
    engine.error event (Telegram alert fires for critical engines), and
    re-raises so CloudWatch metric still records the failure."""
    try:
        return _do_handler(event, context)
    except Exception as e:
        _emit_engine_error(e, phase="handler")
        raise


def _do_handler(event=None, context=None):
    started = time.time()
    print("[lce] start")

    # ─── PASS 1: fetch reference Treasury yields (used by spread_to) ───
    # Plus any other prerequisites. We do this in a small first pass so
    # subsequent series can compute spreads against them.
    ref_ids = ["DGS1", "DGS2", "DGS5", "DGS10", "DGS30"]
    ref_yields = {}
    for rid in ref_ids:
        obs = fred_observations_long(rid)
        if obs:
            ref_yields[rid] = obs[-1]["value"]
    print(f"[lce] reference yields: {ref_yields}")

    # ─── PASS 2: process all series (including refs which already cached
    #     via FRED if rate-limit permits; we still re-fetch for full WoW/MoM/etc) ───
    series_out = {}
    by_category = {
        "balance_sheet": [],
        "liquidity_facilities": [],
        "credit_spreads": [],
        "corporate_yields": [],
        "lending_standards": [],
    }

    for sid, category, label, units, threshold in SERIES_MAP:
        result = compute_series(sid, threshold, ref_yields=ref_yields)
        result["_label"] = label
        result["_units"] = units
        result["_category"] = category
        result["_threshold_note"] = threshold.get("note", "")
        result["_threshold_kind"] = threshold.get("kind")
        series_out[sid] = result
        if category in by_category:
            by_category[category].append(sid)
        else:
            by_category[category] = [sid]

    # Composite + regime
    comp = composite_signal(series_out)
    regime = regime_classification(comp, series_out)

    # Detect transitions vs prior run
    prior = load_prior()
    output = {
        "schema_version": "1.1",
        "generated_at": _now_iso(),
        "elapsed_sec": round(time.time() - started, 2),
        "regime": regime,
        "composite": comp,
        "series": series_out,
        "by_category": by_category,
        "reference": ref_yields,
    }
    transitions = detect_transitions(output, prior)
    output["transitions"] = transitions

    # ── INTERPRETATION & RECOMMENDATION ──
    # Generated dynamically from the same data. Bakes liquidity/credit/lending
    # narrative + concrete portfolio target into the JSON so morning-intel,
    # ai-chat, allocator, and /lce.html all see the same coherent read.
    try:
        output["interpretation"] = interpret_state(output)
    except Exception as e:
        print(f"[lce] interpret_state failed: {e}")
        output["interpretation"] = {"error": str(e)[:200]}

    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=300, s-maxage=60",
    )
    print(f"[lce] regime={regime} composite={comp['score']} firing={comp['n_firing']} "
          f"transitions={len(transitions)} series={len(series_out)}")

    return {"statusCode": 200, "body": json.dumps({
        "regime": regime, "composite_score": comp["score"],
        "n_series": len(series_out),
        "transitions_count": len(transitions),
    })}
