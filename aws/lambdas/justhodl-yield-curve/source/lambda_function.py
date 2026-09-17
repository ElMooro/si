"""
justhodl-yield-curve — Yield Curve Shape Decomposition

Pulls full nominal + real Treasury curve from FRED, computes:
  - Slope spreads: 2s10s, 3M10Y, 5s30s, 2s5s
  - Butterfly: 5Y - (2Y + 10Y)/2 (curvature signal)
  - Real yields (TIPS): 5Y, 10Y, 30Y
  - Break-evens (inflation expectations): 5Y, 10Y
  - Term premium proxy
  - Curve regime classification (4-quadrant)
  - 1d / 5d / 20d / 60d level + slope changes

Curve regime taxonomy (Bull/Bear x Steepener/Flattener):
  - BULL_STEEPENER   : short rates falling faster than long → slope increases
  - BEAR_STEEPENER   : long rates rising faster than short  → growth surprise / inflation
  - BULL_FLATTENER   : long rates falling faster than short → slope decreases
  - BEAR_FLATTENER   : short rates rising faster than long → Fed hiking
  - INVERTED         : 2s10s < 0 → curve inversion; not a timed recession forecast

Output: data/yield-curve.json
"""
import math
from donor_contract import inspect_donor, numeric
import json
import os
import time
import boto3
import urllib.request
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from statistics import mean
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
KEY = "data/yield-curve.json"

FRED_KEY = managed_secret(('FRED_KEY', 'FRED_API_KEY'), ("/justhodl/fred/api-key",))

# Nominal Treasury curve (constant maturity) — daily
NOMINAL_TENORS = [
    ("DGS1MO",  "1M",   1/12),
    ("DGS3MO",  "3M",   0.25),
    ("DGS6MO",  "6M",   0.5),
    ("DGS1",    "1Y",   1.0),
    ("DGS2",    "2Y",   2.0),
    ("DGS3",    "3Y",   3.0),
    ("DGS5",    "5Y",   5.0),
    ("DGS7",    "7Y",   7.0),
    ("DGS10",   "10Y",  10.0),
    ("DGS20",   "20Y",  20.0),
    ("DGS30",   "30Y",  30.0),
]

# Real yields (TIPS)
REAL_TENORS = [
    ("DFII5",   "5Y_REAL",   5.0),
    ("DFII7",   "7Y_REAL",   7.0),
    ("DFII10",  "10Y_REAL",  10.0),
    ("DFII20",  "20Y_REAL",  20.0),
    ("DFII30",  "30Y_REAL",  30.0),
]

# Break-evens (inflation expectations)
BREAKEVENS = [
    ("T5YIE",   "5Y_BREAKEVEN"),
    ("T10YIE",  "10Y_BREAKEVEN"),
    ("T5YIFR",  "5Y5Y_FORWARD"),  # 5y5y forward inflation expectation
]

# Other context series
EXTRAS = [
    ("DFF", "FED_FUNDS_RATE_EFFECTIVE"),
    ("DFEDTARU", "FED_TARGET_UPPER"),
    ("DFEDTARL", "FED_TARGET_LOWER"),
    ("SOFR30DAYAVG", "SOFR_30D_AVG"),
]


def fred_obs(series_id, n=80):
    """Fetch last n observations from FRED for a daily series."""
    url = (
        "https://api.stlouisfed.org/fred/series/observations?"
        + urllib.parse.urlencode({
            "series_id": series_id,
            "api_key": FRED_KEY,
            "file_type": "json",
            "limit": n,
            "sort_order": "desc",
        })
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-yield-curve/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read().decode())
        out = []
        for o in data.get("observations", []):
            v = o.get("value", ".")
            if v == "." or v == "":
                continue
            try:
                value=float(v)
                if math.isfinite(value):
                    out.append({"date": o["date"], "value": value})
            except ValueError:
                continue
        # FRED returned desc, so reverse to ascending chronological
        out.reverse()
        return out
    except Exception as e:
        print(f"[fred_obs] {series_id} failed: {type(e).__name__}")
        return []


def latest_and_changes(obs):
    """Given ascending-time observations, return latest + 1d/5d/20d/60d changes (bps)."""
    obs=sorted({o['date']:o for o in obs if numeric(o.get('value')) is not None}.values(),key=lambda o:o['date'])
    if not obs:
        return None
    latest = obs[-1]
    out = {
        "date": latest["date"],
        "value": round(latest["value"], 4),
        "chg_1d_bps": None,
        "chg_5d_bps": None,
        "chg_20d_bps": None,
        "chg_60d_bps": None,
        "change_basis": "valid daily observations; endpoints disclosed",
        "change_dates": {str(n):obs[-n-1]["date"] if len(obs)>n else None for n in (1,5,20,60)},
    }
    n = len(obs)
    if n >= 2:
        out["chg_1d_bps"] = round((obs[-1]["value"] - obs[-2]["value"]) * 100, 1)
    if n >= 6:
        out["chg_5d_bps"] = round((obs[-1]["value"] - obs[-6]["value"]) * 100, 1)
    if n >= 21:
        out["chg_20d_bps"] = round((obs[-1]["value"] - obs[-21]["value"]) * 100, 1)
    if n >= 61:
        out["chg_60d_bps"] = round((obs[-1]["value"] - obs[-61]["value"]) * 100, 1)
    return out


def classify_curve_regime(short_chg_5d, long_chg_5d, twos_tens_now):
    """Classify into bull/bear x steepener/flattener using 5d changes."""
    if short_chg_5d == 0 and long_chg_5d == 0:
        return ('INVERTED_UNCHANGED' if twos_tens_now is not None and twos_tens_now < 0 else 'UNCHANGED', 'No change at the 2Y and 10Y endpoints')
    if short_chg_5d is not None and long_chg_5d is not None and short_chg_5d == long_chg_5d:
        return ('PARALLEL_BEAR' if short_chg_5d > 0 else 'PARALLEL_BULL', 'Equal endpoint changes; slope unchanged')
    if twos_tens_now is not None and twos_tens_now < 0:
        # Inverted curve overrides regime
        # But still classify whether moving toward/away from inversion
        if short_chg_5d is None or long_chg_5d is None:
            return ("INVERTED", "2s10s inverted — curve inversion; not a timed recession forecast")
        steepening = (long_chg_5d - short_chg_5d) > 0
        bull = (short_chg_5d + long_chg_5d) / 2 < 0  # avg falling
        if steepening and bull:
            return ("INVERTED_BULL_STEEPENING", "2s10s inverted but disinverting via long-end falling slower")
        if steepening and not bull:
            return ("INVERTED_BEAR_STEEPENING", "2s10s inverted, disinverting via long-end rising")
        if not steepening and bull:
            return ("INVERTED_BULL_FLATTENING", "2s10s deepening inversion as short-end falls slower")
        return ("INVERTED_BEAR_FLATTENING", "2s10s deepening inversion as short-end rises faster")

    if short_chg_5d is None or long_chg_5d is None:
        return ("UNKNOWN", "insufficient data for regime classification")

    spread_chg = long_chg_5d - short_chg_5d  # positive = steepening
    avg_chg = (short_chg_5d + long_chg_5d) / 2  # positive = bear (rates up)

    # Regime: 4-quadrant
    if spread_chg > 0 and avg_chg > 0:
        return ("BEAR_STEEPENER", "long rates rising faster than short — slope increases")
    if spread_chg > 0 and avg_chg <= 0:
        return ("BULL_STEEPENER", "short rates falling faster than long — slope increases")
    if spread_chg <= 0 and avg_chg > 0:
        return ("BEAR_FLATTENER", "short rates rising faster than long — slope decreases")
    return ("BULL_FLATTENER", "long rates falling faster than short — slope decreases")


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[yield-curve] start")

    # Fetch all series in parallel
    observations = {}
    series_quality = {}
    nominal = {}
    real = {}
    breakevens = {}
    extras = {}

    def fetch_nominal(sid, label, years):
        obs = fred_obs(sid, n=80)
        return ("nominal", sid, label, years, obs)

    def fetch_real(sid, label, years):
        obs = fred_obs(sid, n=80)
        return ("real", sid, label, years, obs)

    def fetch_breakeven(sid, label):
        obs = fred_obs(sid, n=80)
        return ("breakeven", sid, label, None, obs)

    def fetch_extra(sid, label):
        obs = fred_obs(sid, n=80)
        return ("extra", sid, label, None, obs)

    tasks = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sid, label, years in NOMINAL_TENORS:
            tasks.append(ex.submit(fetch_nominal, sid, label, years))
        for sid, label, years in REAL_TENORS:
            tasks.append(ex.submit(fetch_real, sid, label, years))
        for sid, label in BREAKEVENS:
            tasks.append(ex.submit(fetch_breakeven, sid, label))
        for sid, label in EXTRAS:
            tasks.append(ex.submit(fetch_extra, sid, label))

        for fut in as_completed(tasks):
            kind, sid, label, years, obs = fut.result()
            observations[(kind,sid,label,years)] = obs
            point = latest_and_changes(obs)
            if point is None:
                continue
            point["series_id"] = sid
            point["label"] = label
            if years is not None:
                point["years_to_maturity"] = years
            if kind == "nominal":
                nominal[label] = point
            elif kind == "real":
                real[label] = point
            elif kind == "breakeven":
                breakevens[label] = point
            else:
                extras[label] = point

    print(f"[yield-curve] fetched: nominal={len(nominal)} real={len(real)} be={len(breakevens)} extras={len(extras)}")

    # Align observations to one nominal-curve date. Missing members remain
    # missing; never average a changing set of tenors as the full curve level.
    nominal_hist=[{o['date'] for o in rows if numeric(o.get('value')) is not None}
                  for (kind,*_),rows in observations.items() if kind=='nominal' and rows]
    common=set.intersection(*nominal_hist) if nominal_hist else set()
    curve_date=max(common) if common else None
    nominal,real,breakevens,extras={},{},{},{}
    today=datetime.now(timezone.utc).date()
    for (kind,sid,label,years),rows in observations.items():
        latest=latest_and_changes(rows)
        point=latest_and_changes([o for o in rows if curve_date and o['date']<=curve_date])
        try:age=(today-datetime.strptime(latest['date'],'%Y-%m-%d').date()).days
        except (TypeError,ValueError):age=None
        aligned=bool(point and point['date']==curve_date and latest and latest['date']>=curve_date)
        point_age=(today-datetime.strptime(point['date'],'%Y-%m-%d').date()).days if point else None
        if point_age is not None and point_age>7:
            age=point_age
        status='missing' if age is None else 'invalid' if age<0 else 'stale' if age>7 else 'fresh' if aligned else 'unaligned'
        series_quality[label]={'status':status,'latest_source_date':latest['date'] if latest else None,
                               'observation_date':point['date'] if point else None,'max_age_days':7}
        if status!='fresh':continue
        point.update(series_id=sid,label=label,quality=series_quality[label])
        if years is not None:point['years_to_maturity']=years
        {'nominal':nominal,'real':real,'breakeven':breakevens,'extra':extras}[kind][label]=point
    missing_nominal=[label for _,label,_ in NOMINAL_TENORS if label not in nominal]
    quality={'status':'fresh' if not missing_nominal else 'incomplete','missing':missing_nominal,
             'observation_date':curve_date,'max_age_days':7,'frequency':'daily'}

    # Build curve points (ascending maturity)
    curve_points = []
    for sid, label, years in NOMINAL_TENORS:
        if label in nominal:
            curve_points.append({
                "tenor": label,
                "years": years,
                "yield_pct": nominal[label]["value"],
                "date": nominal[label]["date"],
                "chg_1d_bps": nominal[label]["chg_1d_bps"],
                "chg_5d_bps": nominal[label]["chg_5d_bps"],
                "chg_20d_bps": nominal[label]["chg_20d_bps"],
            })

    # Key spreads (in bps)
    def spread_bps(long_label, short_label):
        if long_label in nominal and short_label in nominal:
            return round((nominal[long_label]["value"] - nominal[short_label]["value"]) * 100, 1)
        return None

    spreads = {
        "2s10s": spread_bps("10Y", "2Y"),
        "3M10Y": spread_bps("10Y", "3M"),
        "5s30s": spread_bps("30Y", "5Y"),
        "2s5s":  spread_bps("5Y",  "2Y"),
        "10s30s": spread_bps("30Y", "10Y"),
        "fed_funds_to_10y": (
            round((nominal["10Y"]["value"] - extras["FED_FUNDS_RATE_EFFECTIVE"]["value"]) * 100, 1)
            if "10Y" in nominal and "FED_FUNDS_RATE_EFFECTIVE" in extras else None
        ),
    }

    # Butterfly: 5Y - (2Y + 10Y)/2 — positive = humped, negative = "tent"
    butterfly = None
    if all(k in nominal for k in ("2Y", "5Y", "10Y")):
        butterfly = round(
            (nominal["5Y"]["value"] - (nominal["2Y"]["value"] + nominal["10Y"]["value"]) / 2) * 100, 1
        )

    # Curve regime
    short_chg_5d = nominal.get("2Y", {}).get("chg_5d_bps")
    long_chg_5d = nominal.get("10Y", {}).get("chg_5d_bps")
    endpoint_match=(nominal.get('2Y',{}).get('change_dates',{}).get('5') is not None and
                    nominal.get('2Y',{}).get('change_dates',{}).get('5')==nominal.get('10Y',{}).get('change_dates',{}).get('5'))
    regime, regime_desc = classify_curve_regime(short_chg_5d if endpoint_match else None, long_chg_5d if endpoint_match else None, spreads.get("2s10s"))
    if quality['status']!='fresh':
        regime,regime_desc='UNKNOWN','Complete current nominal curve unavailable.' 

    inversion_flags={
        '2s10s_inverted':spreads['2s10s']<0 if spreads['2s10s'] is not None else None,
        '3M10Y_inverted':spreads['3M10Y']<0 if spreads['3M10Y'] is not None else None}
    flags=list(inversion_flags.values())
    inversion_flags['any_inversion']=True if True in flags else False if all(v is False for v in flags) else None

    # Real yields snapshot
    real_yields = {
        label: {"date": real[label]["date"], "value_pct": real[label]["value"], "chg_5d_bps": real[label].get("chg_5d_bps")}
        for label in real
    }

    # Inflation expectations
    inflation_expectations = {
        label: {"date": breakevens[label]["date"], "value_pct": breakevens[label]["value"], "chg_5d_bps": breakevens[label].get("chg_5d_bps")}
        for label in breakevens
    }

    # Term premium proxy: 10Y nominal - 10Y real - 10Y breakeven
    # Should be ~0 if all three are perfectly internally consistent; deviations = liquidity/term-premium
    term_premium_proxy = None
    if "10Y" in nominal and "10Y_REAL" in real and "10Y_BREAKEVEN" in breakevens:
        term_premium_proxy = round(
            (nominal["10Y"]["value"] - real["10Y_REAL"]["value"] - breakevens["10Y_BREAKEVEN"]["value"]) * 100, 1
        )

    # Level / slope / curvature decomposition (simplified, not full PCA)
    # Level = mean of curve
    # Slope = 10Y - 2Y
    # Curvature = 2 * 5Y - 2Y - 10Y
    level = slope = curvature = None
    yields_for_decomp = [(p["years"], p["yield_pct"]) for p in curve_points if p["years"] is not None]
    if yields_for_decomp and not missing_nominal:
        level = round(mean([y for _, y in yields_for_decomp]), 4)
    if all(k in nominal for k in ("2Y", "10Y")):
        slope = round((nominal["10Y"]["value"] - nominal["2Y"]["value"]) * 100, 1)
    if all(k in nominal for k in ("2Y", "5Y", "10Y")):
        curvature = round(
            (2 * nominal["5Y"]["value"] - nominal["2Y"]["value"] - nominal["10Y"]["value"]) * 100, 1
        )

    # Signal interpretation
    signals = []
    if inversion_flags["2s10s_inverted"]:
        signals.append({
            "name": "2s10s_inverted",
            "severity": "HIGH",
            "message": f"2s10s = {spreads['2s10s']:+.0f}bps — curve inversion; not a timed recession forecast"
        })
    if inversion_flags["3M10Y_inverted"]:
        signals.append({
            "name": "3M10Y_inverted",
            "severity": "HIGH",
            "message": f"3M10Y = {spreads['3M10Y']:+.0f}bps — Fed-favored recession indicator"
        })
    if regime == "BULL_STEEPENER" and not inversion_flags["any_inversion"]:
        signals.append({
            "name": "bull_steepener",
            "severity": "MEDIUM",
            "message": "Bull steepener: lower average endpoint yields and a wider 2s10s spread"
        })
    if regime == "BEAR_STEEPENER":
        signals.append({
            "name": "bear_steepener",
            "severity": "MEDIUM",
            "message": "Bear steepener: higher average endpoint yields and a wider 2s10s spread"
        })
    if butterfly is not None and butterfly < -30:
        signals.append({
            "name": "butterfly_negative",
            "severity": "MEDIUM",
            "message": f"5Y rich vs 2Y+10Y barbell ({butterfly:+.0f}bps) — curve curvature observation"
        })
    if "10Y_BREAKEVEN" in breakevens:
        be_5d = breakevens["10Y_BREAKEVEN"].get("chg_5d_bps")
        if be_5d is not None and be_5d > 15:
            signals.append({
                "name": "breakeven_rising_fast",
                "severity": "MEDIUM",
                "message": f"10Y breakeven up {be_5d:+.0f}bps in 5d — inflation expectations rising"
            })
        elif be_5d is not None and be_5d < -15:
            signals.append({
                "name": "breakeven_falling_fast",
                "severity": "MEDIUM",
                "message": f"10Y breakeven down {be_5d:+.0f}bps in 5d — inflation expectations falling"
            })
    if False:  # residual is not term premium and cannot generate that alert
        signals.append({
            "name": "term_premium_anomaly",
            "severity": "LOW",
            "message": f"Term premium proxy = {term_premium_proxy:+.0f}bps — TIPS/nominal/breakeven dislocation"
        })

    _tp_bps, _tp_src, _tp_acm = None, "unavailable", None
    try:
        _aj = json.loads(S3.get_object(Bucket=BUCKET, Key="data/term-premium.json")["Body"].read())
        _al = _aj.get("latest") or {}
        _contract=inspect_donor(_aj,'data/term-premium.json',72,observed_paths=('latest.date',),required_paths=('latest.tp10',),max_observation_age_hours=10*24)
        if _contract['usable'] and numeric(_al.get('tp10')) is not None:
            _tp_bps = round(_al["tp10"] * 100, 1)
            _tp_src = "ACM"
            _tp_acm = {"date": _al["date"], "tp10_pct": _al["tp10"], "tp5_pct": _al.get("tp5"),
                       "tp2_pct": _al.get("tp2"), "z_10y": _aj.get("z_10y"),
                       "pctile": _aj.get("pctile_full_history"),
                       "d21_bps": (_aj.get("deltas_bps") or {}).get("d21"),
                       "regime": _aj.get("regime")}
    except Exception as _e:
        print(f"[yc] acm read: {str(_e)[:60]}")
    out = {
        "version": "1.1",
        "methodology_version": "dated-curve.v2", "quality": quality, "series_quality": series_quality,
        "execution_eligible": False, "call": None,
        "nominal_real_breakeven_residual_bps": term_premium_proxy,
        "residual_definition": "Matched-date nominal minus real minus breakeven consistency residual; not term premium.",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "duration_s": round(time.time() - started, 2),
        "as_of_date": nominal.get("10Y", {}).get("date"),
        "regime": regime,
        "regime_description": regime_desc,
        "inversion_flags": inversion_flags,
        "decomposition": {
            "level_pct": level,
            "slope_2s10s_bps": slope,
            "curvature_butterfly_bps": curvature,
        },
        "spreads_bps": spreads,
        "butterfly_5y_bps": butterfly,
        # ops 2710: real ACM tp10 published into the legacy proxy field when
        # fresh (<=10d), so cycle-clock + every reader inherit the true series;
        # regression proxy retained as automatic fallback.
        "term_premium_proxy_bps": _tp_bps,
        "term_premium_source": _tp_src,
        "term_premium_acm": _tp_acm,
        "curve_points": curve_points,
        "nominal_yields": nominal,
        "real_yields": real_yields,
        "inflation_expectations": inflation_expectations,
        "fed_context": extras,
        "signals": signals,
        "n_signals": len(signals),
        "data_sources": {"all": "FRED API (free)"},
        "regime_definitions": {
            "BULL_STEEPENER": "Short rates falling faster than long — slope increases",
            "BEAR_STEEPENER": "Long rates rising faster than short — slope increases",
            "BULL_FLATTENER": "Long rates falling faster than short — slope decreases",
            "BEAR_FLATTENER": "Short rates rising faster than long — slope decreases",
            "INVERTED": "2s10s < 0 — curve inversion; not a timed recession forecast",
        },
    }

    if quality['status']!='fresh':
        out['signals']=[];out['n_signals']=0
    body = json.dumps(out, allow_nan=False, default=str).encode("utf-8")
    S3.put_object(
        Bucket=BUCKET, Key=KEY, Body=body,
        ContentType="application/json", CacheControl="public, max-age=3600",
    )
    print(f"[yield-curve] regime={regime} 2s10s={spreads.get('2s10s')}bps butterfly={butterfly}bps signals={len(signals)}")
    print(f"[yield-curve] wrote s3://{BUCKET}/{KEY} — {len(body):,}b in {out['duration_s']}s")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "regime": regime,
            "twos_tens_bps": spreads.get("2s10s"),
            "butterfly_bps": butterfly,
            "n_signals": len(signals),
            "duration_s": out["duration_s"],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler({}, None), indent=2, default=str))
