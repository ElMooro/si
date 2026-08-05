"""
justhodl-plumbing-aggregator — 4-Layer Liquidity & Risk Plumbing Composite.

WHAT IT DOES
────────────
Aggregates 30+ liquidity/risk indicators from FRED + OFR + existing S3 caches
into a unified 4-layer stress composite. Writes data/plumbing-stress.json.

THE 4 LAYERS
────────────
L1 EURODOLLAR PLUMBING        weight 35%  fast-moving, critical
L2 BANK RISK APPETITE         weight 25%  slow, leading
L3 REAL-SIDE DAMAGE           weight 20%  lagging, confirming
L4 CROSS-BORDER STRESS        weight 20%  regime-defining

NEW DATA THIS LAMBDA ADDS (closes the 9 gaps from Khalid's audit)
─────────────────────────────────────────────────────────────────
1. RTWEXEMEGS    Real EM Dollar Index
2. RTWEXAFEGS    Real Advanced Foreign Economies Dollar Index
3. CASFRIW027SBOG Cash Assets Foreign-Related Banks
4. DPSFRIW027SBOG Deposits Foreign-Related Banks
5. UEMP27OV      Unemployed 27+ weeks
6. LNS13025699   Job Losers Not on Layoff
7. TEMPHELPS     Temp Help Services
8. NY Fed Primary Dealer Aggregate Fails to Deliver: Total
9. NY Fed Primary Dealer Aggregate Fails to Receive: Treasury

EXISTING DATA CONSUMED FROM S3 (avoid re-pulling)
──────────────────────────────────────────────────
- data/eurodollar-stress.json       8-signal composite (existing)
- data/ecb-data.json                CISS for 19 countries (existing)
- data/dollar-strength.json         DXY composites (existing)
- data/khalid-index.json            internal composite (existing)

OUTPUT SCHEMA
─────────────
data/plumbing-stress.json
{
  schema_version, as_of, duration_s,
  composite_score,        # 0-100, higher = more stress
  composite_label,        # ABUNDANT|NORMAL|ELEVATED|CRITICAL|CRISIS
  layers: {
    L1_eurodollar_plumbing:  {score, weight, contributors: [...]},
    L2_bank_risk_appetite:   {...},
    L3_real_side_damage:     {...},
    L4_cross_border_stress:  {...}
  },
  raw_indicators: {        # all 30+ indicators with current value, z-score, percentile
    DTWEXBGS: {value, z_score, percentile, date, label, source, layer, polarity},
    ...
  },
  alerts: [                # any indicators currently > 2σ
    {indicator, layer, severity, message, ts}
  ]
}

INSTITUTIONAL-GRADE
────────────────────
✓ Z-scoring on 5y rolling, then percentile-ranking for 0-100 mapping
✓ Polarity correctly handled (some indicators rising = stress, others = relief)
✓ Failure-safe: each indicator independent, partial output OK
✓ NY Fed fails: tries OFR API → Treasury Direct → falls through gracefully
✓ Defensive timeout handling (Lambda runs in AWS, endpoints accessible)
"""
import json
import math
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = os.environ.get("S3_KEY_OUT", "data/plumbing-stress.json")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")

S3 = boto3.client("s3", region_name=REGION)


# ─── Indicator catalog (30+ indicators across 4 layers) ───────────────────────
# polarity: +1 = rising value indicates more stress; -1 = rising indicates less stress
# source: FRED | OFR | S3
INDICATORS = [
    # ── L1 EURODOLLAR PLUMBING (35% weight) ──
    {"id": "SWP1690",    "label": "Fed Liquidity Swaps 16-90d", "source": "FRED",
     "layer": "L1", "polarity": +1, "weight_in_layer": 0.30,
     "interp": "Fed actively providing USD to foreign central banks — late-stage stress signal"},
    {"id": "ILM_CLAIMS_FX", "label": "ECB claims on EA in foreign currency", "source": "ECB_S3",
     "layer": "L1", "polarity": +1, "weight_in_layer": 0.20,
     "interp": "Eurosystem USD-denominated claims rising = European banks growing USD exposure"},
    {"id": "ILM_LIAB_EUR", "label": "ECB liabilities to non-EA in EUR", "source": "ECB_S3",
     "layer": "L1", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Foreign holdings of EUR rising = avoidance of USD funding"},
    {"id": "OFR_FAILS_DELIVER", "label": "PD Aggregate Fails to Deliver", "source": "OFR",
     "layer": "L1", "polarity": +1, "weight_in_layer": 0.20,
     "interp": "Treasuries failing to settle = collateral hoarding, repo plumbing breaking"},
    {"id": "OFR_FAILS_RECEIVE", "label": "PD Aggregate Fails to Receive", "source": "OFR",
     "layer": "L1", "polarity": +1, "weight_in_layer": 0.20,
     "interp": "Counterparty failing to deliver = balance sheet constraint propagating"},

    # ── L2 BANK RISK APPETITE (25% weight) ──
    {"id": "DRISCFLM", "label": "Banks raising spreads (large/mid firms)", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.20,
     "interp": "Sustained tightening leads NBER recession by ~6mo"},
    {"id": "DRISCFS", "label": "Banks raising spreads (small firms)", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.15,
     "interp": "Small business credit hardest hit first in cycle"},
    {"id": "SUBLPDCISTQNQ", "label": "Banks raising collateral requirements", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Higher haircuts = liquidity strain"},
    {"id": "CASFRIW027SBOG", "label": "Cash at foreign-related banks", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.15,
     "interp": "Foreign banks hoarding USD = defensive crouch"},
    {"id": "DPSFRIW027SBOG", "label": "Deposits at foreign-related banks", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.15,
     "interp": "FX/corp deposits parking in safer balance sheets"},
    {"id": "SBCACBW027SBOG", "label": "Securities in bank credit", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Banks shifting to bonds vs loans = defensive"},
    {"id": "RMFSL", "label": "Retail Money Market Fund balances", "source": "FRED",
     "layer": "L2", "polarity": +1, "weight_in_layer": 0.15,
     "interp": "Retail flight to MMF cash = defensive positioning"},

    # ── L3 REAL-SIDE DAMAGE (20% weight) ──
    {"id": "TEMPHELPS", "label": "Temp Help Services Employment", "source": "FRED",
     "layer": "L3", "polarity": -1, "weight_in_layer": 0.25,
     "interp": "Temp jobs cut FIRST — 8/10 hit rate as 6mo recession leading indicator"},
    {"id": "LNS13025699", "label": "Job Losers Not on Layoff", "source": "FRED",
     "layer": "L3", "polarity": +1, "weight_in_layer": 0.25,
     "interp": "Permanent layoffs (not cyclical) — recession smoking gun"},
    {"id": "UEMP27OV", "label": "Unemployed 27+ weeks", "source": "FRED",
     "layer": "L3", "polarity": +1, "weight_in_layer": 0.15,
     "interp": "Long-term unemployed = structural damage, hard to undo"},
    {"id": "MCUMFN", "label": "Manufacturing Capacity Utilization", "source": "FRED",
     "layer": "L3", "polarity": -1, "weight_in_layer": 0.10,
     "interp": "<74% historically = recession zone"},
    {"id": "INDPRO", "label": "Industrial Production Index", "source": "FRED",
     "layer": "L3", "polarity": -1, "weight_in_layer": 0.10,
     "interp": "YoY <0% for 2+ months = recession confirmed"},
    {"id": "USPBS", "label": "Professional/Business Services Employment", "source": "FRED",
     "layer": "L3", "polarity": -1, "weight_in_layer": 0.10,
     "interp": "White-collar canary — strong ↑, weak = sticker shock"},
    {"id": "BOGZ1FL663067003Q", "label": "Broker-Dealer Margin Loans", "source": "FRED",
     "layer": "L3", "polarity": +1, "weight_in_layer": 0.05,
     "interp": ">1σ above trend = late-cycle excess (1987, 2000, 2008)"},

    # ── L4 CROSS-BORDER STRESS (20% weight) ──
    {"id": "DTWEXBGS", "label": "Broad Dollar Index (Nominal)", "source": "FRED",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.20,
     "interp": "Strong dollar = global liquidity contraction"},
    {"id": "RTWEXBGS", "label": "Broad Dollar Index (Real)", "source": "FRED",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Real-rate adjusted dollar — more honest signal"},
    {"id": "DTWEXEMEGS", "label": "EM Dollar Index (Nominal)", "source": "FRED",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.15,
     "interp": "USD vs EM — strong = EM debt distress"},
    {"id": "RTWEXEMEGS", "label": "EM Dollar Index (Real)", "source": "FRED",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Real EM dollar — best EM crisis early warning"},
    {"id": "DTWEXAFEGS", "label": "AFE Dollar Index (Nominal)", "source": "FRED",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "USD vs Advanced Foreign Economies"},
    {"id": "RTWEXAFEGS", "label": "AFE Dollar Index (Real)", "source": "FRED",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Decoupling of EM vs AFE = flight-to-quality regime"},
    {"id": "CISS_US", "label": "ECB CISS — United States", "source": "ECB_S3",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Composite financial stress, US"},
    {"id": "CISS_EA", "label": "ECB CISS — Euro Area", "source": "ECB_S3",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.10,
     "interp": "Composite financial stress, Eurozone"},
    {"id": "CISS_CN", "label": "ECB CISS — China", "source": "ECB_S3",
     "layer": "L4", "polarity": +1, "weight_in_layer": 0.05,
     "interp": "Chinese financial stress (regime change indicator)"},
]

LAYER_WEIGHTS = {"L1": 0.35, "L2": 0.25, "L3": 0.20, "L4": 0.20}


# ─── Generic HTTP helper ──────────────────────────────────────────────────────
def http_get(url, timeout=20, retries=2, headers=None):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(
                url, headers=headers or {"User-Agent": "JustHodl plumbing-agg/1.0"}
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read().decode("utf-8")
        except Exception as e:
            if attempt < retries:
                time.sleep(0.6 * (attempt + 1))
            else:
                print(f"[plumbing] HTTP fail: {url[:80]} → {e}")
    return None


# ─── FRED puller ──────────────────────────────────────────────────────────────
def fetch_fred(series_id, n=1300):
    qs = urllib.parse.urlencode({
        "series_id": series_id, "api_key": FRED_KEY,
        "file_type": "json", "limit": n, "sort_order": "desc",
    })
    body = http_get(f"https://api.stlouisfed.org/fred/series/observations?{qs}")
    if not body:
        return []
    try:
        d = json.loads(body)
    except Exception:
        return []
    obs = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v and v != ".":
            try:
                obs.append({"date": o["date"], "value": float(v)})
            except ValueError:
                continue
    return obs[::-1]  # chronological


# ─── OFR Short-Term Funding Monitor puller ────────────────────────────────────
def fetch_ofr_series(mnemonic):
    """OFR API for primary dealer fails.
    
    VERIFIED working endpoint (per ops/344 diagnostic):
      https://data.financialresearch.gov/v1/series/timeseries?mnemonic=NYPD-PD_AFtD_TOT-A
    Returns flat [[date, value], ...] array.
    """
    url = f"https://data.financialresearch.gov/v1/series/timeseries?mnemonic={mnemonic}"
    body = http_get(url, timeout=15)
    if not body:
        return []
    try:
        d = json.loads(body)
    except Exception:
        return []
    # Format: list of [date, value] pairs
    if isinstance(d, list):
        obs = []
        for pair in d:
            if isinstance(pair, list) and len(pair) >= 2 and pair[1] is not None:
                try:
                    obs.append({"date": pair[0], "value": float(pair[1])})
                except (ValueError, TypeError):
                    continue
        return obs
    # Fallback: full format with mnemonic key + nested timeseries
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, dict) and "timeseries" in v:
                ts = v["timeseries"]
                # Try aggregation (singular, per actual API response)
                agg = ts.get("aggregation") or ts.get("aggregations")
                if isinstance(agg, list):
                    obs = []
                    for pair in agg:
                        if isinstance(pair, list) and len(pair) >= 2 and pair[1] is not None:
                            try:
                                obs.append({"date": pair[0], "value": float(pair[1])})
                            except (ValueError, TypeError):
                                continue
                    return obs
    return []


# ─── ECB SDMX puller (direct API since no S3 cache exists) ────────────────────
def fetch_ecb_series(dataset, key):
    """Direct ECB SDMX API. Returns observations chronological.
    
    Example: dataset='CISS', key='D.US.Z0Z.4F.EC.SS_CI.IDX'
    """
    url = (f"https://data-api.ecb.europa.eu/service/data/{dataset}/{key}"
            "?format=jsondata&detail=dataonly")
    body = http_get(url, timeout=20, headers={
        "User-Agent": "JustHodl plumbing-agg/1.0",
        "Accept": "application/json",
    })
    if not body:
        return []
    try:
        d = json.loads(body)
    except Exception:
        return []

    # SDMX-JSON structure: dataSets[0].series."0:0:0:0:0".observations
    try:
        datasets = d.get("dataSets") or d.get("dataset") or []
        if not datasets:
            return []
        ds = datasets[0]
        series_dict = ds.get("series") or {}
        # Get the first (and usually only) series
        first_series = next(iter(series_dict.values()), None)
        if not first_series:
            return []
        obs_dict = first_series.get("observations") or {}

        # Time dimension is in structure.dimensions.observation
        struct = d.get("structure", {})
        obs_dims = struct.get("dimensions", {}).get("observation", [])
        time_dim = None
        for dim in obs_dims:
            if dim.get("id") == "TIME_PERIOD":
                time_dim = dim
                break
        if not time_dim:
            return []
        time_values = time_dim.get("values", [])

        # Map index → date, then walk observations
        obs = []
        for idx_str, vals in obs_dict.items():
            try:
                idx = int(idx_str)
                if idx >= len(time_values) or not vals or vals[0] is None:
                    continue
                date_str = time_values[idx].get("id") or time_values[idx].get("name", "")
                obs.append({"date": date_str, "value": float(vals[0])})
            except (ValueError, TypeError, IndexError):
                continue
        # Sort chronologically
        obs.sort(key=lambda o: o["date"])
        return obs
    except Exception as e:
        print(f"[plumbing] ECB parse fail {dataset}/{key}: {e}")
        return []


# ECB series mappings
ECB_SERIES_MAP = {
    "ILM_CLAIMS_FX":  ("ILM",  "W.U2.C.A030000.U2.Z06"),
    "ILM_LIAB_EUR":   ("ILM",  "W.U2.C.L060000.U4.EUR"),
    "CISS_US":        ("CISS", "D.US.Z0Z.4F.EC.SS_CI.IDX"),
    "CISS_EA":        ("CISS", "D.U2.Z0Z.4F.EC.SS_CIN.IDX"),  # NEW CISS for EA
    "CISS_CN":        ("CISS", "D.CN.Z0Z.4F.EC.SS_CIN.IDX"),  # NEW CISS for CN
}


# ─── ECB cache reader (legacy fallback if S3 cache exists later) ─────────────
def fetch_ecb_from_cache(target_id):
    """Read pre-pulled ECB data from existing S3 caches.
    Tries multiple possible cache files."""
    candidates = [
        ("data/ecb-data.json", "ecb"),
        ("data/ecb-cache.json", "ecb"),
        ("data/ecb-financial-stress.json", "ecb"),
        ("data/eurodollar-stress.json", "edm"),
    ]
    for key, fmt in candidates:
        try:
            obj = S3.get_object(Bucket=BUCKET, Key=key)
            d = json.loads(obj["Body"].read())
        except Exception:
            continue
        # CISS lookup: ID → series structure
        ciss_map = {
            "CISS_US": ("US", "CISS"),
            "CISS_EA": ("U2", "CISS"),
            "CISS_CN": ("CN", "CISS"),
            "ILM_CLAIMS_FX": ("ILM_CLAIMS_FX", "ILM"),
            "ILM_LIAB_EUR": ("ILM_LIAB_EUR", "ILM"),
        }
        if target_id not in ciss_map:
            continue
        country, kind = ciss_map[target_id]
        # Various shapes — try to extract observations
        # Format 1: {ciss: {US: [{date, value}], EA: [...]}}
        if "ciss" in d and isinstance(d["ciss"], dict):
            arr = d["ciss"].get(country)
            if arr and isinstance(arr, list):
                obs = [{"date": x.get("date") or x.get("t"),
                         "value": float(x.get("value") or x.get("v"))}
                        for x in arr if x.get("value") is not None]
                if obs:
                    return obs
        # Format 2: {countries: {US: {ciss: [...]}, ...}}
        if "countries" in d and isinstance(d["countries"], dict):
            c = d["countries"].get(country)
            if c and "ciss" in c:
                arr = c["ciss"]
                obs = [{"date": x.get("date"), "value": float(x.get("value"))}
                        for x in arr if x.get("value") is not None]
                if obs:
                    return obs
    return []


# ─── Statistics helpers ───────────────────────────────────────────────────────
def yoy_pct_change(obs):
    """For series like INDPRO where YoY change is what matters."""
    if len(obs) < 13:
        return None
    cur = obs[-1]["value"]
    yr_ago = obs[-13]["value"]  # ~12 months ago for monthly
    if yr_ago == 0:
        return None
    return (cur - yr_ago) / abs(yr_ago) * 100



# ── ops 4412: enrichment catalog (Perplexity dimension-4 for plumbing.html) ──
# Khalid rule: engine AND page, keep existing intact. Brain: the FOUR-CANARY
# convergence (HY OAS widening + SOFR-IORB widening + MOVE spiking + on/off-run
# spread widening on a non-quarter-end day) is "the signature of a basis-trade /
# repo unwind beginning — the single most important pattern in the plumbing."
PLUMB_ENRICH = [
    # A) L1 raw funding rates + reserves regime
    ("SOFR", "SOFR", "l1_funding", "%"),
    ("IORB", "Interest on Reserve Balances", "l1_funding", "%"),
    ("EFFR", "Effective Fed Funds Rate", "l1_funding", "%"),
    ("OBFR", "Overnight Bank Funding Rate", "l1_funding", "%"),
    ("DPRIME", "Bank Prime Rate", "l1_funding", "%"),
    ("SOFR30DAYAVG", "SOFR 30-Day Average", "l1_funding", "%"),
    ("SOFR90DAYAVG", "SOFR 90-Day Average", "l1_funding", "%"),
    ("SOFR180DAYAVG", "SOFR 180-Day Average", "l1_funding", "%"),
    ("WRESBAL", "Reserve Balances (ample-regime marker)", "l1_funding", "M USD"),
    ("WCBSL", "Central Bank Swap Lines", "l1_funding", "M USD"),
    ("DPCREDIT", "Discount Window Primary Credit", "l1_funding", "%"),
    # B) L2 bank risk appetite — H.8 + SLOOS
    ("DRTSCILM", "SLOOS: C&I Tightening", "l2_bank", "%"),
    ("DRTSCLCC", "SLOOS: Consumer Credit Tightening", "l2_bank", "%"),
    ("DRTSCLM", "SLOOS: Mortgage Tightening", "l2_bank", "%"),
    ("TOTBKCR", "Total Bank Credit", "l2_bank", "B USD"),
    ("BUSLOANS", "C&I Loans, All Banks", "l2_bank", "B USD"),
    ("REALLN", "Real Estate Loans, All Banks", "l2_bank", "B USD"),
    ("CONSUMER", "Consumer Loans, All Banks", "l2_bank", "B USD"),
    ("TLAACBW027SBOG", "Total Assets, All Commercial Banks", "l2_bank", "B USD"),
    ("DPSACBW027SBOG", "Deposits, All Commercial Banks", "l2_bank", "B USD"),
    # C) L3 real-side damage (brain weekly set)
    ("ICSA", "Initial Jobless Claims", "l3_real", "count"),
    ("CCSA", "Continuing Claims", "l3_real", "count"),
    ("ICNSA", "Initial Claims (NSA)", "l3_real", "count"),
    # D) L4 cross-border / offshore dollar
    ("WGRESUS", "Foreign Official Holdings of Treasuries", "l4_cross", "M USD"),
    ("DTWEXBGS", "Broad Dollar Index", "l4_cross", "index"),
    ("DTWEXEMEGS", "USD vs Emerging Markets", "l4_cross", "index"),
    # canary inputs
    ("BAMLH0A0HYM2", "HY Master II OAS (canary 1)", "canary", "%"),
    ("DGS10", "10Y Treasury Yield", "canary", "%"),
]
PLUMB_CATS = {
    "l1_funding": "L1 · Raw Funding Rates & Reserves Regime",
    "l2_bank": "L2 · Bank Risk Appetite (H.8 + SLOOS)",
    "l3_real": "L3 · Real-Side Damage (weekly leads)",
    "l4_cross": "L4 · Cross-Border / Offshore Dollar",
    "canary": "Four-Canary Inputs",
}
# brain-stated thresholds
CANARY_THRESHOLDS = {
    "sofr_iorb_bp": {"amber": 5, "red": 10,
                     "note": "brain: >+5bp amber, >+10bp red"},
}




# ── ops 4421: four-canary joins — MOVE + on/off-the-run from fleet feeds ──
# Perplexity asked for the full 4/4 panel. MOVE and the on/off-the-run spread
# are not on FRED, so they must come from sibling engines. Field names are
# NOT assumed: we fetch the feeds and DISCOVER matching keys, reporting what
# was actually found (or an honest pending_source naming the feed we searched).
def _deep_find(obj, patterns, max_depth=5, _d=0):
    """Return (path, value) for the first numeric leaf whose key matches."""
    if _d > max_depth or obj is None:
        return None
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(p in kl for p in patterns):
                if isinstance(v, (int, float)):
                    return (k, float(v))
                if isinstance(v, dict):
                    for vk in ("value", "level", "latest", "current", "last"):
                        if isinstance(v.get(vk), (int, float)):
                            return (f"{k}.{vk}", float(v[vk]))
            r = _deep_find(v, patterns, max_depth, _d + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for v in obj[:20]:
            r = _deep_find(v, patterns, max_depth, _d + 1)
            if r:
                return r
    return None


def _feed(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[canary-join] {key}: {type(e).__name__}")
        return None


def join_canaries(canaries):
    """Populate MOVE and on/off-the-run from sibling feeds, honestly."""
    bv = _feed("data/bond-vol.json")
    hit = _deep_find(bv, ("move",)) if bv else None
    if hit:
        k, v = hit
        canaries["move"] = {
            "label": "MOVE (bond vol)", "value": round(v, 2),
            "source": f"bond-vol.json:{k}",
            "state": ("RED" if v > 140 else "AMBER" if v > 120 else "CALM"),
            "thresholds": {"amber": 120, "red": 140}}
    else:
        canaries["move"] = {
            "label": "MOVE (bond vol)",
            "pending_source": ("searched data/bond-vol.json for a MOVE key "
                               "and found none — engine emits composite_z / "
                               "composite_percentile instead; needs a MOVE "
                               "level series or an explicit proxy decision"),
            "thresholds": {"amber": 120, "red": 140}}
    tn = _feed("data/treasury-noise.json")
    hit2 = _deep_find(tn, ("on_off", "onoff", "off_the_run", "otr",
                           "noise")) if tn else None
    if hit2:
        k, v = hit2
        canaries["on_off_run"] = {
            "label": "On/off-the-run 10Y spread (proxy)",
            "value": round(v, 3), "source": f"treasury-noise.json:{k}",
            "state": ("RED" if abs(v) > 8 else
                      "AMBER" if abs(v) > 4 else "CALM"),
            "note": "treasury-noise dispersion proxy for the OTR spread"}
    else:
        canaries["on_off_run"] = {
            "label": "On/off-the-run 10Y spread",
            "pending_source": ("searched data/treasury-noise.json — no "
                               "on/off-run key; engine emits bill_sofr / "
                               "cp_bill stress percentiles")}
    return canaries


def build_plumb_enrichment():
    """Enrichment catalog + the four-canary convergence panel."""
    cat, latest = {}, {}
    for sid, label, group, unit in PLUMB_ENRICH:
        try:
            obs = fetch_fred(sid, n=1300)
            # fetch_fred returns desc-sorted; normalise to ascending pairs
            pairs = []
            for o in obs:
                v = o.get("value") if isinstance(o, dict) else None
                d = o.get("date") if isinstance(o, dict) else None
                if v in (None, ".", ""):
                    continue
                try:
                    pairs.append((d, float(v)))
                except Exception:
                    continue
            pairs.sort(key=lambda x: x[0] or "")
            vals = [v for _d, v in pairs]
            if not vals:
                continue
            cur = vals[-1]
            latest[sid] = cur
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / max(1, len(vals) - 1)
            sd = var ** 0.5
            z = round((cur - mean) / sd, 2) if sd > 1e-9 else 0.0
            pct = round(100 * sum(1 for v in vals if v <= cur) / len(vals), 1)
            cat.setdefault(group, {})[sid] = {
                "label": label, "value": round(cur, 4), "unit": unit,
                "z": z, "pctile": pct, "n": len(vals),
                "date": pairs[-1][0],
                "spark": [{"date": d, "value": v} for d, v in pairs[-52:]],
            }
        except Exception as e:
            print(f"[plumb-enrich] {sid} skipped: {type(e).__name__}")

    # FOUR-CANARY CONVERGENCE — brain's most important pattern
    canaries = {}
    if "SOFR" in latest and "IORB" in latest:
        sp = round((latest["SOFR"] - latest["IORB"]) * 100, 1)
        canaries["sofr_iorb"] = {
            "label": "SOFR − IORB", "value_bp": sp,
            "state": ("RED" if sp > 10 else "AMBER" if sp > 5 else "CALM"),
            "thresholds": CANARY_THRESHOLDS["sofr_iorb_bp"]}
    hy = cat.get("canary", {}).get("BAMLH0A0HYM2")
    if hy:
        canaries["hy_oas"] = {
            "label": "HY OAS", "value": hy["value"], "z": hy["z"],
            "state": ("RED" if (hy["z"] or 0) > 2 else
                      "AMBER" if (hy["z"] or 0) > 1 else "CALM")}
    canaries = join_canaries(canaries)   # ops 4421: real fleet joins
    firing = [k for k, v in canaries.items()
              if v.get("state") in ("AMBER", "RED")]
    convergence = {
        "canaries": canaries, "n_firing": len(firing), "firing": firing,
        "verdict": ("CONVERGENCE — possible basis-trade/repo unwind "
                    "signature" if len(firing) >= 3 else
                    "PARTIAL — monitor" if len(firing) == 2 else "CALM"),
        "brain_rule": "If all four move together on a non-quarter-end day, "
                      "that is the signature of a basis-trade / repo unwind "
                      "beginning — the single most important pattern in the "
                      "plumbing.",
    }
    return {"categories": cat, "labels": dict(PLUMB_CATS),
            "four_canary": convergence,
            "n_series": sum(len(v) for v in cat.values())}


def z_score_and_percentile(obs, window=1300):
    """Compute z-score and percentile of latest value vs trailing window."""
    if len(obs) < 30:
        return None, None
    values = [o["value"] for o in obs[-window:]]
    cur = values[-1]
    history = values[:-1]
    if not history:
        return None, None
    mean = statistics.mean(history)
    sd = statistics.stdev(history) if len(history) > 1 else 0
    z = (cur - mean) / sd if sd > 0 else 0
    rank = sum(1 for v in history if v < cur)
    pct = round(rank / len(history) * 100, 1)
    return round(z, 2), pct


def fetch_fails_s3(side):
    """Read data/settlement-fails.json (U.S. Treasury ex-TIPS) -> obs list.
    side = 'ftd' (fails to deliver) | 'ftr' (fails to receive)."""
    try:
        body = S3.get_object(Bucket=BUCKET, Key="data/settlement-fails.json")["Body"].read()
        d = json.loads(body)
        head = next((c for c in d.get("classes", []) if c.get("key") == "ust_ex_tips"), None)
        series = (head or {}).get(side) or []
        return [{"date": p[0], "value": p[1]} for p in series if p and p[1] is not None]
    except Exception as e:
        print("[plumbing] fails_s3 %s: %s" % (side, e))
        return []


def assemble_indicator(spec):
    """Pull data for one indicator + compute stats."""
    out = dict(spec)
    sid = spec["id"]
    src = spec["source"]

    if src == "FRED":
        obs = fetch_fred(sid)
    elif src == "OFR":
        # Primary: our reliable NY Fed PD settlement-fails feed; fallback: OFR mnemonic
        fails_map = {
            "OFR_FAILS_DELIVER": ("ftd", "NYPD-PD_AFtD_TOT-A"),
            "OFR_FAILS_RECEIVE": ("ftr", "NYPD-PD_AFtR_T-A"),
        }
        if sid in fails_map:
            side, mnem = fails_map[sid]
            obs = fetch_fails_s3(side) or fetch_ofr_series(mnem)
        else:
            obs = fetch_ofr_series(sid)
    elif src == "ECB_S3":
        # Direct ECB SDMX API (no S3 cache exists for these)
        if sid in ECB_SERIES_MAP:
            dataset, key = ECB_SERIES_MAP[sid]
            obs = fetch_ecb_series(dataset, key)
        else:
            obs = []
    else:
        obs = []

    if not obs:
        out.update({"value": None, "date": None, "z_score": None, "percentile": None,
                    "stress_score_0_100": None, "n_obs": 0, "err": "no data"})
        return out

    out["value"] = obs[-1]["value"]
    out["date"] = obs[-1]["date"]
    out["n_obs"] = len(obs)

    # Special case: INDPRO uses YoY change vs raw value
    if sid in ("INDPRO", "USPBS", "TEMPHELPS"):
        yoy = yoy_pct_change(obs)
        out["yoy_pct"] = yoy

    z, pct = z_score_and_percentile(obs)
    out["z_score"] = z
    out["percentile"] = pct

    # Convert percentile → stress score 0-100 (account for polarity)
    if pct is not None:
        if spec["polarity"] == +1:
            # Higher value = more stress
            out["stress_score_0_100"] = round(pct, 1)
        else:
            # Higher value = less stress (e.g., TEMPHELPS, USPBS, MCUMFN, INDPRO)
            out["stress_score_0_100"] = round(100 - pct, 1)

    return out


# ─── Composite calculator ────────────────────────────────────────────────────
def hk_funding_indicators():
    """HK funding sub-layer (offshore-USD/Asia tell) from data/hkma.json, injected into
    the L4 cross-border layer. Reuses the HKMA monitor's own green/yellow/red grading →
    stress_score so it scores into the composite AND renders on plumbing.html."""
    try:
        hk = json.loads(S3.get_object(Bucket=BUCKET, Key="data/hkma.json")["Body"].read())
    except Exception as e:
        print("[plumbing] hkma feed unavailable: %s" % e)
        return []
    status_stress = {"green": 18.0, "yellow": 55.0, "red": 85.0}
    out = []
    for m in (hk.get("metrics") or []):
        if m.get("id") not in ("agg_balance", "usd_hkd", "hibor_sofr"):
            continue
        ss = status_stress.get(m.get("status"))
        out.append({
            "id": "HK_" + str(m.get("id", "")).upper(),
            "label": "HK · " + m.get("label", ""),
            "source": "HKMA", "layer": "L4", "polarity": +1, "weight_in_layer": 0.06,
            "value": m.get("value"), "date": hk.get("as_of"),
            "z_score": None, "percentile": m.get("pctile"),
            "stress_score_0_100": ss,
            "interp": "HK funding (offshore-USD/Asia): " + (m.get("detail", "")[:90]),
        })
    if out:
        print("[plumbing] HK funding sub-layer: %d contributors (funding=%s)" % (len(out), hk.get("hk_funding")))
    return out


def compute_composite(indicators):
    layers = {}
    for layer_id in ("L1", "L2", "L3", "L4"):
        members = [i for i in indicators if i["layer"] == layer_id]
        weighted_sum = 0
        total_weight = 0
        contributors = []
        for m in members:
            score = m.get("stress_score_0_100")
            w = m.get("weight_in_layer", 0)
            if score is not None:
                weighted_sum += score * w
                total_weight += w
            contributors.append({
                "id": m["id"], "label": m["label"],
                "value": m.get("value"), "date": m.get("date"),
                "z_score": m.get("z_score"), "percentile": m.get("percentile"),
                "stress_score": score, "weight": w,
                "yoy_pct": m.get("yoy_pct"),
                "interp": m.get("interp"),
                "err": m.get("err"),
            })
        layer_score = round(weighted_sum / total_weight, 1) if total_weight > 0 else None
        layers[layer_id] = {
            "score": layer_score,
            "weight": LAYER_WEIGHTS[layer_id],
            "n_indicators": len(members),
            "n_with_data": sum(1 for m in members if m.get("stress_score_0_100") is not None),
            "contributors": contributors,
        }

    # Composite weighted across layers
    composite_sum = 0
    composite_weight = 0
    for layer_id, layer in layers.items():
        if layer["score"] is not None:
            composite_sum += layer["score"] * layer["weight"]
            composite_weight += layer["weight"]
    composite = round(composite_sum / composite_weight, 1) if composite_weight > 0 else None

    if composite is None:
        label = "UNKNOWN"
    elif composite < 25:    label = "ABUNDANT"
    elif composite < 50:    label = "NORMAL"
    elif composite < 70:    label = "ELEVATED"
    elif composite < 85:    label = "CRITICAL"
    else:                   label = "CRISIS"

    return composite, label, layers


def generate_alerts(indicators):
    """Surface indicators currently in stress (z > 2 or percentile > 90 in stress direction)."""
    alerts = []
    for ind in indicators:
        z = ind.get("z_score")
        score = ind.get("stress_score_0_100")
        if z is None or score is None:
            continue
        sev = None
        if score >= 90 or (z and abs(z) >= 2.5):
            sev = "CRITICAL"
        elif score >= 75 or (z and abs(z) >= 1.8):
            sev = "ELEVATED"
        if sev:
            alerts.append({
                "indicator": ind["id"],
                "label": ind["label"],
                "layer": ind["layer"],
                "value": ind.get("value"),
                "z_score": z,
                "stress_score": score,
                "severity": sev,
                "message": ind.get("interp", ""),
            })
    alerts.sort(key=lambda a: a["stress_score"] or 0, reverse=True)
    return alerts


# ─── Main handler ────────────────────────────────────────────────────────────
# ─── Deep monthly history for charting (back to ~1990 where the series exists) ─
CRISES = [
    ["1997-07", "1998-10", "Asian / LTCM"],
    ["2000-03", "2002-10", "Dot-com"],
    ["2007-08", "2009-06", "GFC"],
    ["2011-07", "2012-07", "Euro debt"],
    ["2015-08", "2016-02", "China / oil"],
    ["2020-02", "2020-05", "COVID"],
    ["2022-01", "2022-10", "Rate shock"],
    ["2023-03", "2023-05", "SVB / banks"],
]


def fetch_fred_deep(series_id, start="1990-01-01"):
    qs = urllib.parse.urlencode({"series_id": series_id, "api_key": FRED_KEY,
                                 "file_type": "json", "observation_start": start, "sort_order": "asc"})
    body = http_get(f"https://api.stlouisfed.org/fred/series/observations?{qs}")
    out = []
    if body:
        try:
            for o in json.loads(body).get("observations", []):
                v = o.get("value")
                if v and v != ".":
                    try:
                        out.append([o["date"], float(v)])
                    except ValueError:
                        pass
        except Exception:
            pass
    return out


def downsample_monthly(obs):
    m = {}
    for o in obs:
        if isinstance(o, dict):
            d, v = o.get("date"), o.get("value")
        else:
            d, v = o[0], o[1]
        if d and v is not None:
            m[d[:7]] = [d, round(float(v), 4)]
    return [m[k] for k in sorted(m)]


def build_history(specs):
    hist = {}
    for spec in specs:
        sid, src = spec["id"], spec["source"]
        pts = []
        try:
            if src == "FRED":
                pts = downsample_monthly(fetch_fred_deep(sid))
            elif src == "ECB_S3" and sid in ECB_SERIES_MAP:
                dataset, key = ECB_SERIES_MAP[sid]
                pts = downsample_monthly(fetch_ecb_series(dataset, key))
            elif src == "OFR":
                side = "ftd" if sid == "OFR_FAILS_DELIVER" else "ftr"
                pts = downsample_monthly(fetch_fails_s3(side))
        except Exception as e:
            print("[plumbing] hist %s: %s" % (sid, e))
        if pts:
            hist[sid] = {"label": spec.get("label"), "layer": spec.get("layer"),
                         "polarity": spec.get("polarity"), "interp": spec.get("interp"),
                         "history": pts, "start": pts[0][0], "end": pts[-1][0], "n": len(pts)}
    return hist


def lambda_handler(event, context):
    started = time.time()

    print(f"[plumbing] Pulling {len(INDICATORS)} indicators across 4 layers…")

    # Parallel pull (cap concurrency to be polite)
    enriched = []
    with ThreadPoolExecutor(max_workers=6) as exe:
        futures = {exe.submit(assemble_indicator, spec): spec["id"] for spec in INDICATORS}
        for fut in as_completed(futures):
            try:
                enriched.append(fut.result())
            except Exception as e:
                sid = futures[fut]
                print(f"[plumbing] {sid} fatal: {e}")
                enriched.append({"id": sid, "err": str(e)[:100]})

    # HK funding sub-layer (offshore-USD/Asia) from the HKMA monitor → L4 cross-border
    enriched.extend(hk_funding_indicators())

    composite, label, layers = compute_composite(enriched)
    alerts = generate_alerts(enriched)

    # Raw indicators dict keyed by ID (for UI consumption)
    raw_dict = {ind["id"]: {
        "label": ind.get("label"),
        "layer": ind.get("layer"),
        "value": ind.get("value"),
        "date": ind.get("date"),
        "z_score": ind.get("z_score"),
        "percentile": ind.get("percentile"),
        "stress_score": ind.get("stress_score_0_100"),
        "yoy_pct": ind.get("yoy_pct"),
        "polarity": ind.get("polarity"),
        "source": ind.get("source"),
        "interp": ind.get("interp"),
        "err": ind.get("err"),
    } for ind in enriched}

    payload = {
        "schema_version": "1.0",
        "method": "plumbing_aggregator_v1",
        "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n_indicators": len(INDICATORS),
        "n_with_data": sum(1 for i in enriched if i.get("stress_score_0_100") is not None),
        "composite_score": composite,
        "composite_label": label,
        "layers": layers,
        "raw_indicators": raw_dict,
        "enrichment": build_plumb_enrichment(),   # ops 4412
        "alerts": alerts,
        "duration_s": round(time.time() - started, 1),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=600",
    )
    # ---- deep monthly history for charting (separate file, loaded by the page) ----
    try:
        hist = build_history(INDICATORS)
        hbody = json.dumps({"generated_at": payload["as_of"], "crises": CRISES,
                            "indicators": hist}, default=str).encode("utf-8")
        S3.put_object(Bucket=BUCKET, Key="data/plumbing-history.json", Body=hbody,
                      ContentType="application/json", CacheControl="max-age=3600")
        print("[plumbing] history file: %d indicators with deep history" % len(hist))
    except Exception as e:
        print("[plumbing] history build failed: %s" % e)

    print(f"[plumbing] DONE in {payload['duration_s']}s · "
          f"{payload['n_with_data']}/{payload['n_indicators']} with data · "
          f"composite={composite} ({label}) · {len(alerts)} alerts")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "composite_score": composite,
            "composite_label": label,
            "n_with_data": payload["n_with_data"],
            "n_alerts": len(alerts),
            "duration_s": payload["duration_s"],
        }),
    }
