"""justhodl-macro-nowcast (v2)

Composite real-time nowcast indicator. Fetches 7 FRED series directly
(data/report.json only stores current snapshots, no history needed for
z-scoring against trailing 5y).

For each input series, compute YoY % change for flow series (INDPRO,
PAYEMS, RSAFS, HOUST) or level for level series (UMCSENT, T10Y2Y,
UNRATE), then convert to z-score vs trailing 60 monthly observations.

Output: data/macro-nowcast.json with composite score + regime label.

Schedule: rate(6 hours).
"""
from __future__ import annotations
import math
import json
import os
import statistics
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials
try:
    import _fred_shim  # noqa: F401
except Exception:
    pass

REGION = "us-east-1"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
OUTPUT_KEY = "data/macro-nowcast.json"
FRED_KEY = managed_secret(('FRED_KEY', 'FRED_API_KEY'), ("/justhodl/fred/api-key",))
FRED_START = "2000-01-01"   # 25 years of history → solid 60-obs trailing window

WEIGHTS = {
    "INDPRO":  {"weight": +0.20, "label": "Industrial Production",
                "transform": "yoy_pct", "rationale": "Leading manufacturing pulse"},
    "PAYEMS":  {"weight": +0.25, "label": "Nonfarm Payrolls",
                "transform": "yoy_pct", "rationale": "Single best monthly growth indicator"},
    "RSAFS":   {"weight": +0.20, "label": "Retail Sales",
                "transform": "yoy_pct", "rationale": "Consumer demand"},
    "HOUST":   {"weight": +0.10, "label": "Housing Starts",
                "transform": "yoy_pct", "rationale": "Rate-sensitive, leading"},
    "UMCSENT": {"weight": +0.10, "label": "Consumer Sentiment (UMich)",
                "transform": "level_z", "rationale": "Soft data, leads spending"},
    "T10Y2Y":  {"weight": +0.10, "label": "2s10s Yield Curve",
                "transform": "level_z", "rationale": "Inversion = forward slowing"},
    "UNRATE":  {"weight": -0.05, "label": "Unemployment Rate",
                "transform": "level_z", "rationale": "Inverse: rising unemp = slowing"},
}

s3 = boto3.client("s3", region_name=REGION)


def fred_fetch(series_id: str):
    """Fetch monthly observations for a FRED series."""
    url = ("https://api.stlouisfed.org/fred/series/observations?"
           + urllib.parse.urlencode({
               "series_id": series_id,
               "api_key": FRED_KEY,
               "file_type": "json",
               "observation_start": FRED_START,
               "aggregation_method": "eop" if series_id=="SP500" else "avg",
               "frequency": "m",  # monthly aggregation (FRED auto-resamples)
               "limit": 100000,
           }))
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "justhodl-macro-nowcast/2.0"})
        with urllib.request.urlopen(req, timeout=20) as r:
            data = json.loads(r.read().decode())
        out = []
        for o in data.get("observations", []):
            v = o.get("value", ".")
            if v in (".", "", None):
                continue
            try:
                value=float(v)
                if math.isfinite(value):out.append((o["date"], value))
            except (ValueError, KeyError):
                continue
        out.sort(key=lambda x: x[0])
        return out, None
    except Exception as e:
        return [], type(e).__name__


def month_id(d):
    y,m=map(int,d[:7].split('-'))
    if not 1<=m<=12:raise ValueError('invalid month')
    return y*12+m-1


def clean_monthly(history):
    rows={}
    for d,v in history:
        try:
            m=month_id(d)
            if not math.isfinite(v):continue
            if m in rows:raise ValueError('duplicate observation month')
            rows[m]=(d,v)
        except (TypeError,ValueError) as exc:
            if str(exc)=='duplicate observation month':raise
    return rows


def transform_zscore(history, transform: str):
    try:rows=clean_monthly(history)
    except ValueError:return None,None,'duplicate_month'
    if not rows:return None,None,'empty_series'
    current_month=max(rows);current=rows[current_month][1]
    if transform=='yoy_pct':
        values={m:(v/rows[m-12][1]-1)*100 for m,(_,v) in rows.items()
                if m-12 in rows and rows[m-12][1]!=0}
        if current_month not in values:return None,None,'missing_exact_year_ago'
    elif transform=='level_z':values={m:v for m,(_,v) in rows.items()}
    else:return None,current,'unknown_transform'
    latest=values[current_month]
    baseline=[v for m,v in values.items() if current_month-60<=m<current_month]
    if len(baseline)<24:return None,latest,'insufficient_60_calendar_month_baseline'
    sd=statistics.stdev(baseline)
    if sd==0:return None,latest,'zero_baseline_stdev'
    return (latest-statistics.mean(baseline))/sd,latest,None


def regime_for_score(score: float):
    """Return (regime_label, color)."""
    if score > 1.0:
        return "STRONG EXPANSION", "green"
    if score > 0.3:
        return "EXPANSION", "green"
    if score > -0.3:
        return "MUDDLE", "yellow"
    if score > -1.0:
        return "SLOWING", "amber"
    return "CONTRACTION RISK", "red"


def fetch_spy_monthly():
    """Fetch monthly S&P 500 prices from FRED (SP500 series).
    Returns sorted [(date, close_price), ...]. Note: FRED's SP500 only
    goes back ~10 years (2015-present), which is fine for our 120-month
    historical window."""
    return fred_fetch("SP500")[0]


def compute_spy_returns_by_regime(historical_scores, spy_data):
    """For each historical month T classified as regime R, compute
    SP500 price-only forward returns at 1, 3, 6, 12 months. Aggregate by regime.

    Returns: {regime: {n_obs, mean_pct: {1m, 3m, 6m, 12m}, hit_rate: {...}}}
    """
    spy_data=[(d,p) for d,p in spy_data if isinstance(p,(int,float)) and math.isfinite(p) and p>0]
    if not historical_scores or not spy_data:
        return {}

    # Build a date → spy_price lookup. SP500 price data is monthly already
    # (FRED returns monthly observations when frequency=m). Match by year-month.
    spy_by_ym = {}
    for d, p in spy_data:
        ym = d[:7]  # "2024-01"
        if ym not in spy_by_ym:
            spy_by_ym[ym] = p

    sorted_yms = sorted(spy_by_ym.keys())
    ym_idx = {ym: i for i, ym in enumerate(sorted_yms)}
    horizons = [1, 3, 6, 12]   # months forward

    by_regime = {}
    for h in historical_scores:
        regime = h["regime"]
        ym = h["date"][:7]
        if ym not in ym_idx:
            continue
        idx = ym_idx[ym]
        spy_now = spy_by_ym[ym]
        if spy_now is None or spy_now == 0:
            continue

        if regime not in by_regime:
            by_regime[regime] = {"n_obs": 0, "returns": {h: [] for h in horizons}}
        by_regime[regime]["n_obs"] += 1

        for fwd_months in horizons:
            target=month_id(ym)+fwd_months
            target_ym=f'{target//12:04d}-{target%12+1:02d}'
            spy_fwd = spy_by_ym.get(target_ym)
            if spy_fwd is None:
                continue
            ret_pct = (spy_fwd - spy_now) / spy_now * 100
            by_regime[regime]["returns"][fwd_months].append(ret_pct)

    # Compute summary stats per regime
    out = {}
    for regime, info in by_regime.items():
        summary = {"n_obs": info["n_obs"], "horizons": {}, "return_basis":"SP500 monthly end-of-period price-only; dividends excluded", "minimum_summary_n":12, "validation_status":"CURRENT_VINTAGE_DESCRIPTIVE_ONLY"}
        for h, vals in info["returns"].items():
            if len(vals)<12:
                summary["horizons"][f"{h}m"] = None
                continue
            n_pos = sum(1 for v in vals if v > 0)
            sorted_vals = sorted(vals)
            mid = len(sorted_vals) // 2
            median = sorted_vals[mid] if len(sorted_vals) % 2 else (sorted_vals[mid-1] + sorted_vals[mid]) / 2
            summary["horizons"][f"{h}m"] = {
                "n": len(vals),
                "mean_pct": round(sum(vals) / len(vals), 2),
                "median_pct": round(median, 2),
                "hit_rate_pct": round(n_pos / len(vals) * 100, 1),
                "min_pct": round(min(vals), 2),
                "max_pct": round(max(vals), 2),
            }
        out[regime] = summary
    return out


def compute_historical_scores(fred_data: dict, lookback_months: int = 120):
    """Reconstruct monthly scores using current-vintage histories, not publication vintages.

    This produces a 10-year historical track of a reconstruction using currently revised observations. Each month uses the same z-scoring methodology
    (prior 60 calendar months; revised data is not point-in-time evidence).
    """
    # Build a unified date axis: union of all month-ends across all series
    all_dates = set()
    for sid, history in fred_data.items():
        for d, _ in history:
            all_dates.add(d)
    sorted_dates = sorted(all_dates)
    if len(sorted_dates) < 24:
        return []

    # For each historical month T, replay all components with data up to T
    # (no future leakage). Skip months too early to compute z-scores.
    historical = []
    n_months = len(sorted_dates)
    start_idx = max(24, n_months - lookback_months)

    for i in range(start_idx, n_months):
        target_date = sorted_dates[i]
        comp_contribs = []
        weighted_sum = 0.0
        weight_used_abs = 0.0

        for fred_id, spec in WEIGHTS.items():
            full = fred_data.get(fred_id, [])
            # Slice to obs at or before target_date
            slice_ = [(d, v) for d, v in full if d <= target_date]
            if len(slice_) < 24 or not slice_ or slice_[-1][0][:7]!=target_date[:7]:
                continue
            z, raw_value, err = transform_zscore(slice_, spec["transform"])
            if z is None:
                continue
            contrib = spec["weight"] * z
            comp_contribs.append({"id": fred_id, "z": round(z, 3),
                                  "contribution": round(contrib, 4)})
            weighted_sum += contrib
            weight_used_abs += abs(spec["weight"])

        if len(comp_contribs)!=len(WEIGHTS):
            continue
        total_abs_weight = sum(abs(s["weight"]) for s in WEIGHTS.values())
        normalized = weighted_sum * (total_abs_weight / weight_used_abs)
        regime, color = regime_for_score(normalized)
        historical.append({
            "date": target_date, "vintage_basis":"current_vintage_reconstruction", "point_in_time_validated":False,
            "score": round(normalized, 3),
            "raw_score": round(weighted_sum, 3),
            "regime": regime,
            "regime_color": color,
            "coverage_pct": round(weight_used_abs / total_abs_weight * 100, 1),
            "n_components": len(comp_contribs),
        })

    return historical


def optional_block(module, *args):
    try:
        return __import__(module).block(*args)
    except Exception as exc:
        return {'status':'UNAVAILABLE','reason':type(exc).__name__}


def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[nowcast-v2] start {datetime.now(timezone.utc).isoformat()}")

    fred_data = {}
    fred_errors = {}
    spy_data = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        # 7 nowcast components + SPY (for forward-return analysis)
        futs = {ex.submit(fred_fetch, sid): sid for sid in WEIGHTS.keys()}
        spy_fut = ex.submit(fred_fetch, "SP500")
        for f in as_completed(futs):
            sid = futs[f]
            history, err = f.result()
            fred_data[sid] = history
            if err:
                fred_errors[sid] = err
            print(f"[nowcast-v2] {sid}: {len(history)} obs  err={err}")
        spy_history, spy_err = spy_fut.result()
        spy_data = spy_history
        if spy_err:
            print(f"[nowcast-v2] SP500: ERR {spy_err}")
        else:
            print(f"[nowcast-v2] SP500: {len(spy_data)} obs")

    # Daily series aggregated to the current partial month must not be mixed
    # with closed monthly releases. Publication and observation remain distinct.
    current_month=datetime.now(timezone.utc).strftime('%Y-%m')
    fred_data={sid:[(d,v) for d,v in rows if d[:7]<current_month] for sid,rows in fred_data.items()}
    spy_data=[(d,v) for d,v in spy_data if d[:7]<current_month and math.isfinite(v) and v>0]
    observation_quality={}
    for sid,rows in fred_data.items():
        age=(datetime.now(timezone.utc).date()-datetime.strptime(rows[-1][0],'%Y-%m-%d').date()).days if rows else None
        status='unavailable' if age is None else 'invalid' if age<0 else 'stale' if age>100 else 'fresh'
        observation_quality[sid]={'status':status,'observation_date':rows[-1][0] if rows else None,'max_age_days':100,'frequency':'monthly'}
        if status!='fresh':fred_errors[sid]='observation_'+status

    components = []
    weighted_sum = 0.0
    weight_used_abs = 0.0

    for fred_id, spec in WEIGHTS.items():
        history = fred_data.get(fred_id, [])
        if not history or fred_id in fred_errors:
            components.append({
                "fred_id": fred_id, "label": spec["label"],
                "transform": spec["transform"], "weight": spec["weight"],
                "rationale": spec["rationale"], "z": None, "raw_value": None,
                "contribution": None, "error": fred_errors.get(fred_id, "no_data"),
            })
            continue

        z, raw_value, err = transform_zscore(history, spec["transform"])
        if z is None:
            components.append({
                "fred_id": fred_id, "label": spec["label"],
                "transform": spec["transform"], "weight": spec["weight"],
                "rationale": spec["rationale"], "z": None,
                "raw_value": round(raw_value, 3) if raw_value is not None else None,
                "contribution": None, "error": err, "n_obs": len(history),
                "latest_date": history[-1][0],
            })
            continue

        contribution = spec["weight"] * z
        components.append({
            "fred_id": fred_id, "label": spec["label"],
            "transform": spec["transform"], "weight": spec["weight"],
            "rationale": spec["rationale"], "z": round(z, 3),
            "raw_value": round(raw_value, 3) if raw_value is not None else None,
            "contribution": round(contribution, 4),
            "n_obs": len(history),
            "latest_date": history[-1][0],
        })
        weighted_sum += contribution
        weight_used_abs += abs(spec["weight"])

    total_abs_weight=sum(abs(v['weight']) for v in WEIGHTS.values())
    coverage=weight_used_abs/total_abs_weight
    ready=sum(c.get('contribution') is not None for c in components)==len(WEIGHTS)
    normalized_score=weighted_sum if ready else None
    regime,regime_color=regime_for_score(normalized_score) if ready else ('UNAVAILABLE','gray')
    for c in components:
        c['quality']=observation_quality.get(c['fred_id'],{'status':'unavailable'})
        if c.get('error'):c['quality']={**c['quality'],'status':'unavailable','reason':c['error']}

    components.sort(key=lambda c: -abs(c.get("contribution") or 0))

    # Historical replay — what would the nowcast have read each month
    # over the past 10 years? Current-vintage reconstruction only.
    print("[nowcast-v2] computing historical replay…")
    hist_started = time.time()
    historical_scores = compute_historical_scores(fred_data, lookback_months=120)
    print(f"[nowcast-v2] historical: {len(historical_scores)} months "
          f"computed in {round(time.time()-hist_started, 2)}s")

    # Describe SP500 price-only outcomes by reconstructed regime; no forecast claim.
    print("[nowcast-v2] computing SP500 price returns by regime…")
    regime_spy_started = time.time()
    regime_spy_performance = compute_spy_returns_by_regime(historical_scores, spy_data)
    print(f"[nowcast-v2] regime-SP500: {len(regime_spy_performance)} regimes "
          f"in {round(time.time()-regime_spy_started, 2)}s")

    # Summary stats for the historical track (useful for page rendering)
    hist_summary = {}
    if historical_scores:
        scores = [h["score"] for h in historical_scores]
        regime_counts = {}
        for h in historical_scores:
            regime_counts[h["regime"]] = regime_counts.get(h["regime"], 0) + 1
        hist_summary = {
            "n_months": len(historical_scores),
            "first_date": historical_scores[0]["date"],
            "last_date": historical_scores[-1]["date"],
            "min_score": round(min(scores), 3),
            "max_score": round(max(scores), 3),
            "mean_score": round(sum(scores) / len(scores), 3),
            "regime_distribution": regime_counts,
            "current_score_percentile": round(
                sum(1 for s in scores if s <= normalized_score) / len(scores) * 100, 1) if normalized_score is not None else None,
        }

    output = {
        "v": "2.3", "methodology_version":"monthly-measurement.v2",
        "quality":{"status":"fresh" if ready else "incomplete", "required_series":list(WEIGHTS),
                   "observation_dates":{k:v.get('observation_date') for k,v in observation_quality.items()},
                   "basis":"ragged-edge latest closed monthly observations; all seven required"},
        "call":None,"execution_eligible":False,"calibration_status":"HEURISTIC_REVIEW_ONLY",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "global_confidence": optional_block("wl_series", {
        "cci_jp": ("ECONOMICS:JPCCI", "Japan consumer conf"),
        "cci_de": ("ECONOMICS:DECCI", "Germany consumer conf"),
        "cci_fr": ("ECONOMICS:FRCCI", "France consumer conf"),
        "cci_gb": ("ECONOMICS:GBCCI", "UK consumer conf"),
        "cci_kr": ("ECONOMICS:KRCCI", "Korea consumer conf"),
        "cci_cn": ("ECONOMICS:CNCCI", "China consumer conf"),
        "cci_it": ("ECONOMICS:ITCCI", "Italy consumer conf"),
        "cci_es": ("ECONOMICS:ESCCI", "Spain consumer conf"),
        "bcoi_gb": ("ECONOMICS:GBBCOI", "UK business conf"),
        "bcoi_cn": ("ECONOMICS:CNBCOI", "China business conf"),
        "bcoi_eu": ("ECONOMICS:EUBCOI", "EA industry conf"),
        "bcoi_it": ("ECONOMICS:ITBCOI", "Italy business conf"),
        "gdp_de": ("ECONOMICS:DEGDPYY", "Germany GDP YoY"),
        "gdp_fr": ("ECONOMICS:FRGDPYY", "France GDP YoY"),
        "gdp_ea": ("ECONOMICS:EUGDPYY", "EA19 GDP YoY"),
    }, True),  # ops 3244: series-level fusion
        "wl_research": optional_block("wl_fusion", ('GROWTH', 'INFLATION')),
        "duration_s": round(time.time() - started, 2),
        "raw_score": round(weighted_sum, 4) if ready else None,
        "normalized_score": round(normalized_score, 4) if normalized_score is not None else None,
        "regime": regime,
        "regime_color": regime_color,
        "coverage_pct": round(coverage * 100, 1),
        "n_components_used": sum(1 for c in components if c.get("contribution") is not None),
        "n_components_failed": sum(1 for c in components if c.get("error")),
        "components": components,
        "historical_scores": historical_scores,
        "historical_summary": hist_summary,
        "regime_spy_performance": regime_spy_performance,
        "regime_spy_horizons": ["1m", "3m", "6m", "12m"],
        "thresholds": {
            "strong_expansion": 1.0, "expansion": 0.3,
            "muddle": -0.3, "slowing": -1.0,
        },
        "data_sources": {"all": "FRED (st. louis fed)"},
        "return_study":{"instrument":"S&P 500 index", "source":"FRED SP500", "dividends_included":False,
                        "price_convention":"monthly end-of-period", "historical_availability":"not point-in-time validated",
                        "note":"Revised macro histories and overlapping horizons make this descriptive, not investable performance."},
        "methodology":"Seven FRED monthly indicators; exact-calendar YoY, prior 60 calendar-month baseline (at least 24 valid values). "
                      "Current partial months excluded. All seven fresh components required; missing data does not become MUDDLE. "
                      "Historical scores reconstruct current-vintage data, not release-time information. "
                      "SP500 returns are price-only, monthly end-of-period, exact calendar horizons; no SPY total-return claim."

    }

    # The watchlist cache has unvalidated units/frequencies and observed dates
    # years behind its publication stamp. Preserve it as quarantined context.
    confidence=output.get('global_confidence') or {}
    confidence.update(composite_z=None,composite_n=0,status='BLOCKED_UNVALIDATED_SOURCE_AND_FREQUENCY',
                      score_eligible=False,note='Watchlist cache is historical context only; not admitted to a macro composite.')
    for row in (confidence.get('series') or {}).values():
        row['score_eligible']=False;row['quality_status']='CHECK_DATA'
    output['global_confidence']=confidence
    s3.put_object(
        Bucket=S3_BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, allow_nan=False, default=str).encode(),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )

    # ─── Regime-change detection ────────────────────────────────────
    # State persisted in SSM /justhodl/nowcast/last_state. Lambda runs
    # every 6h but the underlying FRED data updates monthly, so this
    # fires Telegram only when the regime label changes — typically
    # once per quarter at most.
    change_summary = check_regime_change(regime, normalized_score) if ready and not (event or {}).get("suppress_alerts") else {"changed":False,"suppressed":True}

    print(f"[nowcast-v2] regime={regime}  score={normalized_score}  "
          f"coverage={round(coverage*100, 0)}%  duration={round(time.time()-started, 2)}s")
    return {"statusCode": 200, "body": json.dumps({
        "regime": regime,
        "score": round(normalized_score, 4) if normalized_score is not None else None,
        "coverage_pct": round(coverage * 100, 1),
        "regime_change": change_summary,
    })}


def check_regime_change(current_regime: str, current_score: float):
    """Compare current vs previous regime in SSM. Fire Telegram on change."""
    ssm = boto3.client("ssm", region_name=REGION)
    state_key = "/justhodl/nowcast/last_state"

    # Read previous state
    prev = None
    try:
        v = ssm.get_parameter(Name=state_key)["Parameter"]["Value"]
        prev = json.loads(v)
    except Exception as e:
        if "ParameterNotFound" not in str(e):
            print(f"[regime-change] read err: {e}")

    new_state = {
        "regime": current_regime,
        "score": round(current_score, 4),
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    summary = {"changed": False, "previous": prev, "new": new_state}

    if prev and prev.get("regime") and prev["regime"] != current_regime:
        # REGIME CHANGED — fire Telegram
        summary["changed"] = True
        summary["from"] = prev["regime"]
        summary["to"] = current_regime
        summary["score_delta"] = round(current_score - (prev.get("score") or 0), 4)
        try:
            send_regime_change_alert(prev, new_state)
            summary["telegram_sent"] = True
        except Exception as e:
            summary["telegram_err"] = str(e)[:200]

    # Always persist current state
    try:
        ssm.put_parameter(
            Name=state_key, Value=json.dumps(new_state),
            Type="String", Overwrite=True,
            Description="Most recent nowcast regime + score (for change detection)",
        )
    except Exception as e:
        print(f"[regime-change] persist err: {e}")

    return summary


def send_regime_change_alert(prev, new):
    """Send Telegram message announcing regime change."""
    ssm = boto3.client("ssm", region_name=REGION)

    # Get bot token + chat_id from SSM
    try:
        token = ssm.get_parameter(
            Name="/justhodl/telegram/bot_token", WithDecryption=True
        )["Parameter"]["Value"]
    except Exception:
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    try:
        chat_id = ssm.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception:
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        raise RuntimeError("missing token or chat_id")

    icon_map = {
        "STRONG EXPANSION": "🟢🟢", "EXPANSION": "🟢",
        "MUDDLE": "🟡", "SLOWING": "🟠", "CONTRACTION RISK": "🔴",
    }
    new_icon = icon_map.get(new["regime"], "⚪")
    old_icon = icon_map.get(prev["regime"], "⚪")
    score_delta = (new["score"] or 0) - (prev.get("score") or 0)
    delta_arrow = "↑" if score_delta > 0 else "↓" if score_delta < 0 else "→"

    text = (
        f"⚡ *MACRO NOWCAST — REGIME CHANGE*\n\n"
        f"{old_icon} *{prev['regime']}*  →  {new_icon} *{new['regime']}*\n"
        f"`Score: {prev.get('score'):+.3f}  {delta_arrow}  {new['score']:+.3f}`\n"
        f"`Δ:    {score_delta:+.3f} z-score units`\n\n"
        f"_Previous reading: {prev.get('ts', 'unknown')}_\n"
        f"_New reading:      {new['ts']}_\n\n"
        f"[Open dashboard](https://justhodl.ai/macro-data.html)"
    )

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = json.dumps({
        "chat_id": chat_id,
        "text": text[:4096],
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    }).encode()
    import urllib.request
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        print(f"[regime-change] telegram sent: {r.status}")
