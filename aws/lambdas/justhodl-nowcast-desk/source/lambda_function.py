"""
justhodl-nowcast-desk — the free real-time macro "nowcast" layer that Bloomberg /
Refinitiv terminals feature front-and-center, reverse-engineered from public Fed
sources (no terminal subscription):

  GROWTH   — Atlanta Fed GDPNow (current-quarter real GDP, SAAR)
  INFLATION— the Fed's own "underlying inflation" suite (the measures that strip out
             noise and actually guide policy): Sticky-Price CPI, Core Sticky CPI,
             Cleveland Median CPI, 16% Trimmed-Mean CPI, Dallas Trimmed-Mean PCE,
             plus a synthesized underlying-inflation composite vs the 2% target.
  LABOR    — Atlanta Fed Wage Growth Tracker (overall + job switchers vs stayers).

Combined into a growth x inflation nowcast quadrant. All series from FRED (free).
Real data only; a series that can't fetch reports null, never fake. Writes
data/nowcast-desk.json.
"""
import os
import json
import statistics
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/nowcast-desk.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "")
UA = {"User-Agent": "Mozilla/5.0 (JustHodl nowcast-desk)"}
FED_TARGET = 2.0


def fred(sid, obs=180):
    """Latest-first [(date, value)] ascending — most recent `obs` observations."""
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=%d" % (sid, FRED_KEY, obs))
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        d = json.loads(r.read())
    out = []
    for o in d.get("observations", []):
        v = o.get("value")
        if v not in (".", "", None):
            try:
                out.append((o["date"], float(v)))
            except Exception:
                pass
    out.sort()
    return out


def metric(sid, label, annualized_note=""):
    try:
        s = fred(sid)
        if not s:
            return {"label": label, "error": "no data"}
        cur = s[-1]
        prev = s[-2][1] if len(s) > 1 else None
        yago = s[-13][1] if len(s) > 13 else None
        vals = [v for _, v in s]
        return {"label": label, "value": round(cur[1], 2), "asof": cur[0],
                "chg": round(cur[1] - prev, 2) if prev is not None else None,
                "yoy": round(cur[1] - yago, 2) if yago is not None else None,
                "note": annualized_note,
                "history": [{"p": d, "v": round(v, 2)} for d, v in s[-36:]]}
    except Exception as e:
        return {"label": label, "error": str(e)[:100]}


def block_gdp_nowcast():
    m = metric("GDPNOW", "Atlanta Fed GDPNow (real GDP, SAAR %)", "current-quarter real GDP growth, annualized")
    if "value" in m:
        v = m["value"]
        m["signal"] = ("STRONG" if v >= 3 else "SOLID" if v >= 2 else "SOFT" if v >= 1 else "STALL/CONTRACTION")
    return m


def block_underlying_inflation():
    series = {
        "sticky_cpi": ("STICKCPIM159SFRBATL", "Sticky-Price CPI (1m ann.)"),
        "core_sticky_cpi": ("CORESTICKM159SFRBATL", "Core Sticky-Price CPI (1m ann.)"),
        "median_cpi": ("MEDCPIM159SFRBCLE", "Cleveland Median CPI (1m ann.)"),
        "trimmed_mean_cpi": ("TRMMEANCPIM159SFRBCLE", "16% Trimmed-Mean CPI (1m ann.)"),
        "trimmed_mean_pce": ("PCETRIM12M159SFRBDAL", "Dallas Trimmed-Mean PCE (12m %)"),
    }
    out = {}
    latest_vals = []
    for k, (sid, label) in series.items():
        m = metric(sid, label)
        out[k] = m
        if "value" in m:
            latest_vals.append(m["value"])
    if latest_vals:
        comp = round(statistics.mean(latest_vals), 2)
        out["composite"] = {
            "underlying_inflation_pct": comp,
            "vs_fed_target_bps": round((comp - FED_TARGET) * 100, 0),
            "n_measures": len(latest_vals),
            "trend": ("HOT (>3%)" if comp >= 3 else "ABOVE TARGET" if comp > 2.3
                      else "AT TARGET" if comp >= 1.7 else "BELOW TARGET"),
            "note": "mean of the Fed's underlying-inflation gauges — strips volatile components; what policy actually tracks",
        }
    # Supercore (services less rent of shelter) — read the BLS agent's authoritative
    # value so the whole platform shows one number (single source of truth).
    sc_yoy = (read_s3_json("data/bls-labor.json").get("summary") or {}).get("supercore_yoy_pct")
    if sc_yoy is not None:
        out["supercore"] = {"yoy_pct": sc_yoy, "asof": "BLS",
                            "sticky": sc_yoy > 3.0,
                            "note": "core services ex-shelter (BLS) — the stickiest, most policy-relevant inflation cut"}
    return out


def read_s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return {}


def block_growth_confirmation():
    """Cross-check GDPNow with hard data the nowcast doesn't otherwise see: the
    GDP-GDI gap (income vs output side), freight (goods-economy pulse), and core
    capital-goods orders (business investment). Orthogonal confirmation of the
    growth axis — flags when GDPNow may over/understate the real economy."""
    out = {}
    gg = (read_s3_json("data/bea-economic.json").get("gdp_gdi") or {})
    if gg.get("gap_pct") is not None:
        out["gdp_gdi_gap_pct"] = gg["gap_pct"]
        out["real_gdi_pct"] = gg.get("real_gdi_pct")
    fr = ((read_s3_json("data/macro-leads.json").get("freight_activity") or {}).get("composite") or {})
    if fr.get("avg_yoy_pct") is not None:
        out["freight_yoy_pct"] = fr["avg_yoy_pct"]
        out["freight_read"] = fr.get("read")
    cx = ((read_s3_json("data/census-economic.json").get("manufacturing_orders") or {}).get("core_capex_orders") or {})
    if cx.get("yoy_pct") is not None:
        out["core_capex_yoy_pct"] = cx["yoy_pct"]
    # tally soft vs firm hard reads
    soft = firm = 0
    if out.get("gdp_gdi_gap_pct") is not None:
        if out["gdp_gdi_gap_pct"] > 1.0: soft += 1
        elif out["gdp_gdi_gap_pct"] < -1.0: firm += 1
    if out.get("freight_read"):
        if "CONTRACT" in out["freight_read"]: soft += 1
        elif "EXPAND" in out["freight_read"]: firm += 1
    if out.get("core_capex_yoy_pct") is not None:
        if out["core_capex_yoy_pct"] < 0: soft += 1
        elif out["core_capex_yoy_pct"] > 5: firm += 1
    n = sum(1 for k in ("gdp_gdi_gap_pct", "freight_read", "core_capex_yoy_pct") if out.get(k) is not None)
    if n:
        out["hard_data_bias"] = ("SOFTER than GDPNow" if soft > firm
                                 else "FIRMER than GDPNow" if firm > soft else "CONFIRMS GDPNow")
        out["n_hard_reads"] = n
    return out


def block_wage_tracker():
    out = {}
    for k, (sid, label) in {
        "overall": ("FRBATLWGT3MMAUMHWGO", "Wage Growth Tracker — overall (3m MA, %)"),
        "job_switchers": ("FRBATLWGTJSUMHWGO", "Wage Growth — job switchers (%)"),
        "job_stayers": ("FRBATLWGTSAUMHWGO", "Wage Growth — job stayers (%)"),
    }.items():
        m = metric(sid, label)
        if "value" in m or "error" in m:
            out[k] = m
    if out.get("job_switchers", {}).get("value") is not None and out.get("job_stayers", {}).get("value") is not None:
        out["switcher_premium_pct"] = round(out["job_switchers"]["value"] - out["job_stayers"]["value"], 2)
    return out


def handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    gdp = block_gdp_nowcast()
    infl = block_underlying_inflation()
    wage = block_wage_tracker()
    gconf = block_growth_confirmation()

    quadrant = None
    try:
        g = gdp.get("value")
        c = (infl.get("composite") or {}).get("underlying_inflation_pct")
        if g is not None and c is not None:
            growth = "ABOVE-TREND" if g >= 2 else "BELOW-TREND"
            inflation = "HOT" if c > 2.3 else "COOLING" if c < 1.7 else "AT-TARGET"
            regime = {("ABOVE-TREND", "HOT"): "OVERHEAT",
                      ("ABOVE-TREND", "COOLING"): "GOLDILOCKS",
                      ("ABOVE-TREND", "AT-TARGET"): "GOLDILOCKS",
                      ("BELOW-TREND", "HOT"): "STAGFLATION",
                      ("BELOW-TREND", "COOLING"): "DISINFLATION/SLOWDOWN",
                      ("BELOW-TREND", "AT-TARGET"): "SOFT LANDING"}.get((growth, inflation), "MIXED")
            quadrant = {"growth": growth, "inflation": inflation, "regime": regime,
                        "gdpnow": g, "underlying_inflation": c}
            sc = (infl.get("supercore") or {}).get("yoy_pct")
            if sc is not None:
                quadrant["supercore_yoy"] = sc
            bias = gconf.get("hard_data_bias")
            if bias:
                quadrant["growth_confirmation"] = bias
                quadrant["regime_confidence"] = ("HIGH — hard data confirms" if "CONFIRMS" in bias
                                                 else "MODERATE — hard data (GDI/freight/capex) diverges from GDPNow")
    except Exception:
        pass

    populated = sum([("value" in gdp),
                     bool((infl.get("composite") or {}).get("underlying_inflation_pct") is not None),
                     bool(wage.get("overall", {}).get("value") is not None)])
    out = {
        "generated_at": now.isoformat(),
        "source": "FRED — Atlanta/Cleveland/Dallas Fed nowcasts & underlying-inflation suite (free; the Bloomberg-terminal macro nowcast layer)",
        "gdp_nowcast": gdp,
        "underlying_inflation": infl,
        "growth_confirmation": gconf,
        "wage_growth_tracker": wage,
        "nowcast_quadrant": quadrant,
        "_blocks_live": populated,
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    return {"ok": True, "blocks_live": populated, "regime": (quadrant or {}).get("regime")}


def lambda_handler(event=None, context=None):
    return handler(event, context)
