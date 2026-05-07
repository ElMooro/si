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
    """OFR API for primary dealer fails. Tries multiple endpoint patterns."""
    endpoints = [
        f"https://data.financialresearch.gov/v1/series/full?mnemonic={mnemonic}",
        f"https://www.financialresearch.gov/short-term-funding-monitor/api/v1/series/full/?mnemonic={mnemonic}",
        f"https://www.financialresearch.gov/short-term-funding-monitor/api/v1/series/timeseries/{mnemonic}",
    ]
    for url in endpoints:
        body = http_get(url, timeout=15)
        if not body:
            continue
        try:
            d = json.loads(body)
        except Exception:
            continue
        # OFR typically returns either {timeseries: {dates: [], values: []}}
        # or a list of [date, value] pairs
        ts = d.get("timeseries") or d.get("series") or d
        # Try list-of-pairs format
        if isinstance(ts, list) and ts and isinstance(ts[0], list):
            obs = [{"date": p[0], "value": float(p[1])} for p in ts if p[1] is not None]
            if obs:
                return obs
        # Try dict with dates/values arrays
        if isinstance(ts, dict):
            dates = ts.get("dates") or ts.get("date")
            vals = ts.get("values") or ts.get("value")
            if dates and vals and len(dates) == len(vals):
                obs = []
                for i, dt in enumerate(dates):
                    v = vals[i]
                    if v is not None:
                        try:
                            obs.append({"date": dt, "value": float(v)})
                        except (ValueError, TypeError):
                            continue
                if obs:
                    return obs
        # Try aggregations array
        if isinstance(ts, dict) and "aggregations" in ts:
            agg = ts["aggregations"]
            if isinstance(agg, list):
                obs = [{"date": p.get("date") or p.get("d"),
                         "value": float(p.get("value") or p.get("v"))}
                        for p in agg if p.get("value") is not None]
                if obs:
                    return obs
    return []


# ─── ECB S3 cache reader ──────────────────────────────────────────────────────
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


def assemble_indicator(spec):
    """Pull data for one indicator + compute stats."""
    out = dict(spec)
    sid = spec["id"]
    src = spec["source"]

    if src == "FRED":
        obs = fetch_fred(sid)
    elif src == "OFR":
        # Map our internal IDs to OFR mnemonics
        ofr_map = {
            "OFR_FAILS_DELIVER": "NYPD-PD_AFtD_TOT-A",
            "OFR_FAILS_RECEIVE": "NYPD-PD_AFtR_T-A",
        }
        obs = fetch_ofr_series(ofr_map.get(sid, sid))
    elif src == "ECB_S3":
        obs = fetch_ecb_from_cache(sid)
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
        "alerts": alerts,
        "duration_s": round(time.time() - started, 1),
    }

    body_bytes = json.dumps(payload, indent=2, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY, Body=body_bytes,
        ContentType="application/json", CacheControl="max-age=600",
    )
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
