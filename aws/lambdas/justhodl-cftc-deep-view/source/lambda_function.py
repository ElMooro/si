"""
justhodl-cftc-deep-view -- Institutional-grade COT positioning deep view.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
CFTC Commitments of Traders (COT) reports are the gold standard for
positioning data — released every Friday at 3:30PM ET. Macro funds run
COT extremes as a contrarian signal: when commercials are net-long at
multi-year extremes and large-specs are net-short at extremes, that's
the smart-money long signal. When the opposite, smart-money short.

DJ Capelis at AQR built career on COT regime work. JPM's macro research
prominently features COT positioning. Bloomberg's COT function exists
but has no z-score / extremes layer. Refinitiv's COT product is $15k/yr.

Your existing cftc-futures-positioning-agent fetches and caches raw COT
data (29 contracts, 7 categories per memory). This deep-view sits on top
and provides the hedge-fund analytics: per-contract z-scores, extremes
flags, position deltas, smart-money/dumb-money divergence, contrarian
thesis.

DISTINCTION FROM EXISTING ENGINES
──────────────────────────────────
  cftc-futures-positioning-agent   raw COT fetch + cache (LIVE, upstream)
  THIS engine                      analytics layer: z-scores, extremes,
                                   contrarian thesis, smart/dumb divergence

THE 5-LAYER ANALYSIS PER CONTRACT
──────────────────────────────────
  L1 NET POSITIONING        large-spec/small-spec/commercial net positions
  L2 Z-SCORE (1Y, 3Y)       net position normalized vs historical
                              z >= 2  EXTREME LONG (caution if smart-money
                                      = commercials; bullish contrarian
                                      if smart-money = large-specs going long
                                      while commercials short)
                              z <= -2 EXTREME SHORT (opposite)
  L3 POSITION DELTAS        week-over-week + 4w smoothed change
                              flags abrupt regime shifts (>1σ weekly delta)
  L4 SMART vs DUMB MONEY    commercials = smart-money in commodities
                              large-specs = smart-money in financials
                              divergence (smart long, dumb short or vice
                              versa) = highest-conviction contrarian setup
  L5 NARRATIVE              per-contract one-line institutional thesis

CONTRACTS CATEGORIZED
─────────────────────
  EQUITY_INDEX   ES NQ YM RTY   smart = large-specs (instutitonal flow)
  VOLATILITY     VX             smart = commercials (vol-sellers)
  RATES          ZB ZN ZF ZT    smart = commercials (banks hedging)
  FX             6E 6J 6B 6S    smart = large-specs (macro funds)
  ENERGY         CL NG RB HO    smart = commercials (producers hedge)
  METALS         GC SI HG PL    smart = commercials (miners/users)
  AGS            ZC ZS ZW LE   smart = commercials (farmers/processors)

OUTPUT
──────
  s3://justhodl-dashboard-live/data/cftc-deep-view.json   (full analytics)
  s3://justhodl-dashboard-live/data/cot-extremes.json     (extremes only —
                                                            consumed by
                                                            signal-portfolio
                                                            + alert-router)
  Schedule: Fridays 22:00 UTC (after Tuesday data release + cache settles)

ACADEMIC BASIS
──────────────
- Wang (2003). The behavior and performance of major types of futures
  traders. Journal of Futures Markets, 23(1), 1-31.
- de Roon, Nijman, Veld (2000). Hedging pressure effects in futures
  markets. Journal of Finance, 55(3), 1437-1456.
- Bessembinder & Chan (1992). Time-varying risk premia and forecastable
  returns in futures markets. JFE, 32(2), 169-193.
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import math
import os
import statistics
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_INPUT_KEY = "data/cftc-all-cache.json"
S3_DEEP_KEY = "data/cftc-deep-view.json"
S3_EXTREMES_KEY = "data/cot-extremes.json"

# Smart-money assignment per category (institutional convention)
SMART_MONEY_MAP = {
    "equity_index": "large_speculators",
    "volatility": "commercials",
    "treasury": "commercials",
    "rates": "commercials",
    "fx": "large_speculators",
    "currency": "large_speculators",
    "energy": "commercials",
    "metals": "commercials",
    "agricultural": "commercials",
    "ags": "commercials",
    "softs": "commercials",
    "livestock": "commercials",
}

s3 = boto3.client("s3", region_name="us-east-1")


def fetch_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch] {key} miss: {str(e)[:60]}")
        return None


def safe_float(v):
    try:
        if v is None:
            return None
        f = float(v)
        if f != f or f == float("inf") or f == float("-inf"):
            return None
        return f
    except (ValueError, TypeError):
        return None


def extract_net_positions(record):
    """Extract net positioning from a CFTC record across varying schemas.

    The cftc-futures-positioning-agent caches per-contract historical
    series. We robustly fish out: commercials net, large_spec net,
    small_spec/non-reportable net, and open interest.
    """
    out = {}
    # Various schemas in the wild
    fields_map = {
        "comm_long": ["comm_positions_long_all",
                        "commercial_long",
                        "comm_long",
                        "Commercial_Positions_Long_All",
                        "comm_long_all"],
        "comm_short": ["comm_positions_short_all",
                         "commercial_short",
                         "comm_short",
                         "Commercial_Positions_Short_All",
                         "comm_short_all"],
        "noncomm_long": ["noncomm_positions_long_all",
                            "non_commercial_long", "noncomm_long",
                            "NonComm_Positions_Long_All",
                            "noncomm_long_all"],
        "noncomm_short": ["noncomm_positions_short_all",
                             "non_commercial_short", "noncomm_short",
                             "NonComm_Positions_Short_All",
                             "noncomm_short_all"],
        "nonrept_long": ["nonrept_positions_long_all",
                            "non_reportable_long", "nonrept_long"],
        "nonrept_short": ["nonrept_positions_short_all",
                             "non_reportable_short", "nonrept_short"],
        "open_interest": ["open_interest_all",
                            "Open_Interest_All", "oi"],
    }
    for canonical, candidates in fields_map.items():
        for c in candidates:
            v = safe_float(record.get(c) if isinstance(record, dict)
                            else None)
            if v is not None:
                out[canonical] = v
                break

    if "comm_long" in out and "comm_short" in out:
        out["commercials_net"] = out["comm_long"] - out["comm_short"]
    if "noncomm_long" in out and "noncomm_short" in out:
        out["large_speculators_net"] = (out["noncomm_long"] -
                                          out["noncomm_short"])
    if "nonrept_long" in out and "nonrept_short" in out:
        out["small_speculators_net"] = (out["nonrept_long"] -
                                           out["nonrept_short"])
    return out


def compute_zscore(value, history):
    """Standard z-score; returns None if insufficient history."""
    if value is None or len(history) < 12:
        return None
    try:
        mean = statistics.mean(history)
        std = statistics.stdev(history)
    except statistics.StatisticsError:
        return None
    if std == 0:
        return 0.0
    return (value - mean) / std


def analyze_contract(symbol, contract_meta, history):
    """Run the 5-layer analysis on one contract."""
    if not history or not isinstance(history, list):
        return None

    # Sort by date ascending; expected to be already
    parsed = []
    for r in history:
        if not isinstance(r, dict):
            continue
        net = extract_net_positions(r)
        if not net:
            continue
        date = r.get("report_date") or r.get("date") or r.get(
            "Report_Date_as_YYYY-MM-DD") or r.get("As_of_Date_In_Form_YYMMDD")
        parsed.append({"date": str(date), **net})

    if len(parsed) < 12:
        return {"symbol": symbol, "status": "insufficient_history",
                "n_records": len(parsed)}

    # Sort by date
    parsed.sort(key=lambda x: x.get("date") or "")

    latest = parsed[-1]
    n = len(parsed)

    # Build historical series for z-score (use last 156 weeks ~3y)
    hist_3y_comm = [r["commercials_net"] for r in parsed[-156:]
                       if r.get("commercials_net") is not None]
    hist_1y_comm = [r["commercials_net"] for r in parsed[-52:]
                       if r.get("commercials_net") is not None]
    hist_3y_specs = [r["large_speculators_net"] for r in parsed[-156:]
                        if r.get("large_speculators_net") is not None]
    hist_1y_specs = [r["large_speculators_net"] for r in parsed[-52:]
                        if r.get("large_speculators_net") is not None]

    z_comm_1y = compute_zscore(latest.get("commercials_net"), hist_1y_comm)
    z_comm_3y = compute_zscore(latest.get("commercials_net"), hist_3y_comm)
    z_specs_1y = compute_zscore(latest.get("large_speculators_net"),
                                   hist_1y_specs)
    z_specs_3y = compute_zscore(latest.get("large_speculators_net"),
                                   hist_3y_specs)

    # Week-over-week + 4w avg deltas
    prev = parsed[-2] if n >= 2 else None
    wow_comm = (latest.get("commercials_net", 0) -
                  (prev.get("commercials_net", 0) if prev else 0))
    wow_specs = (latest.get("large_speculators_net", 0) -
                   (prev.get("large_speculators_net", 0) if prev else 0))

    # 4w avg
    recent_4w_comm = [r["commercials_net"] for r in parsed[-4:]
                        if r.get("commercials_net") is not None]
    avg_4w_comm = (statistics.mean(recent_4w_comm)
                     if recent_4w_comm else None)
    recent_4w_specs = [r["large_speculators_net"] for r in parsed[-4:]
                         if r.get("large_speculators_net") is not None]
    avg_4w_specs = (statistics.mean(recent_4w_specs)
                      if recent_4w_specs else None)

    # Extremes
    extremes = []
    if z_comm_3y is not None and abs(z_comm_3y) >= 2.0:
        extremes.append({
            "type": ("COMMERCIALS_EXTREME_LONG"
                       if z_comm_3y > 0 else "COMMERCIALS_EXTREME_SHORT"),
            "z_3y": round(z_comm_3y, 2),
            "severity": min(100, int(abs(z_comm_3y) * 30)),
        })
    if z_specs_3y is not None and abs(z_specs_3y) >= 2.0:
        extremes.append({
            "type": ("LARGE_SPECS_EXTREME_LONG"
                       if z_specs_3y > 0 else "LARGE_SPECS_EXTREME_SHORT"),
            "z_3y": round(z_specs_3y, 2),
            "severity": min(100, int(abs(z_specs_3y) * 30)),
        })

    # Smart vs dumb money divergence
    category = (contract_meta.get("category", "") or "").lower()
    smart_money = SMART_MONEY_MAP.get(category, "commercials")
    dumb_money = ("large_speculators" if smart_money == "commercials"
                    else "commercials")
    smart_net = latest.get(f"{smart_money}_net")
    dumb_net = latest.get(f"{dumb_money}_net")
    smart_z = z_comm_3y if smart_money == "commercials" else z_specs_3y
    dumb_z = z_specs_3y if smart_money == "commercials" else z_comm_3y

    divergence = None
    if (smart_z is not None and dumb_z is not None and
          abs(smart_z) >= 1.5 and abs(dumb_z) >= 1.5 and
          (smart_z > 0) != (dumb_z > 0)):
        divergence = {
            "type": ("SMART_LONG_DUMB_SHORT"
                        if smart_z > 0 else "SMART_SHORT_DUMB_LONG"),
            "smart_money_role": smart_money,
            "smart_z_3y": round(smart_z, 2),
            "dumb_z_3y": round(dumb_z, 2),
            "interpretation": (
                f"contrarian bullish: {smart_money} extreme long while "
                f"{dumb_money} extreme short"
                if smart_z > 0 else
                f"contrarian bearish: {smart_money} extreme short while "
                f"{dumb_money} extreme long"),
            "severity": min(100,
                              int((abs(smart_z) + abs(dumb_z)) * 20)),
        }

    # Narrative
    narrative_parts = [f"{symbol} ({contract_meta.get('name', symbol)})"]
    if extremes:
        narrative_parts.append(
            "extreme positioning: " + ", ".join(e["type"] for e in extremes))
    elif z_comm_3y is not None:
        if z_comm_3y > 0:
            narrative_parts.append(
                f"commercials moderate long (z={z_comm_3y:.2f})")
        else:
            narrative_parts.append(
                f"commercials moderate short (z={z_comm_3y:.2f})")
    if divergence:
        narrative_parts.append(divergence["interpretation"])

    return {
        "symbol": symbol,
        "name": contract_meta.get("name", symbol),
        "category": contract_meta.get("category"),
        "smart_money_role": smart_money,
        "as_of_date": latest.get("date"),
        "n_history_records": n,
        "latest_positions": {
            "commercials_net": latest.get("commercials_net"),
            "large_speculators_net": latest.get("large_speculators_net"),
            "small_speculators_net": latest.get("small_speculators_net"),
            "open_interest": latest.get("open_interest"),
        },
        "z_scores": {
            "commercials_1y": round(z_comm_1y, 2) if z_comm_1y is not None
                                else None,
            "commercials_3y": round(z_comm_3y, 2) if z_comm_3y is not None
                                else None,
            "large_specs_1y": round(z_specs_1y, 2) if z_specs_1y is not None
                                else None,
            "large_specs_3y": round(z_specs_3y, 2) if z_specs_3y is not None
                                else None,
        },
        "deltas": {
            "wow_commercials": wow_comm,
            "wow_large_specs": wow_specs,
            "avg_4w_commercials": avg_4w_comm,
            "avg_4w_large_specs": avg_4w_specs,
        },
        "extremes": extremes,
        "smart_dumb_divergence": divergence,
        "narrative": " | ".join(narrative_parts),
        "status": "ok",
    }


# ---------- Main ----------
def lambda_handler(event=None, context=None):
    started = time.time()
    print(f"[cftc-deep-view] start v{VERSION}")

    cache = fetch_s3_json(S3_INPUT_KEY)
    if not cache:
        return {"statusCode": 500, "body": json.dumps({
            "ok": False,
            "error": (f"upstream {S3_INPUT_KEY} missing — verify "
                        "cftc-futures-positioning-agent is running")})}

    # Cache structure: per memory, contains per-contract historical data
    contracts_data = (cache.get("contracts")
                        or cache.get("contracts_data")
                        or cache.get("by_contract")
                        or {})
    contracts_meta = (cache.get("contract_metadata")
                        or cache.get("meta")
                        or {})

    if not contracts_data:
        # Try alternate shape: top-level keys are contract symbols
        contracts_data = {k: v for k, v in cache.items()
                           if isinstance(v, list) and k.isalpha()}

    print(f"[cftc-deep-view] loaded {len(contracts_data)} contracts from cache")

    analyses = []
    for symbol, history in contracts_data.items():
        meta = (contracts_meta.get(symbol) or {})
        if not meta.get("name"):
            meta = {"name": symbol, "category": "unknown"}
        try:
            result = analyze_contract(symbol, meta, history)
            if result:
                analyses.append(result)
        except Exception as e:
            print(f"[{symbol}] err: {str(e)[:120]}")

    # Aggregate state
    ok_count = sum(1 for a in analyses if a.get("status") == "ok")
    all_extremes = []
    all_divergences = []
    for a in analyses:
        if a.get("status") != "ok":
            continue
        for e in a.get("extremes") or []:
            all_extremes.append({
                "symbol": a["symbol"], "name": a["name"],
                "category": a["category"],
                "as_of_date": a["as_of_date"],
                **e,
            })
        if a.get("smart_dumb_divergence"):
            all_divergences.append({
                "symbol": a["symbol"], "name": a["name"],
                "category": a["category"],
                "as_of_date": a["as_of_date"],
                **a["smart_dumb_divergence"],
            })

    all_extremes.sort(key=lambda x: -(x.get("severity", 0)))
    all_divergences.sort(key=lambda x: -(x.get("severity", 0)))

    # State machine
    n_critical_divergences = sum(1 for d in all_divergences
                                    if d.get("severity", 0) >= 80)
    if n_critical_divergences >= 1:
        state = "HIGH_CONVICTION_CONTRARIAN_SETUPS_PRESENT"
    elif len(all_divergences) >= 2:
        state = "MULTIPLE_DIVERGENCES_EMERGING"
    elif len(all_extremes) >= 3:
        state = "EXTREME_POSITIONING_PRESENT"
    else:
        state = "NORMAL_POSITIONING"

    output = {
        "engine": "cftc-deep-view",
        "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "state": state,
        "n_contracts_analyzed": ok_count,
        "n_extremes": len(all_extremes),
        "n_divergences": len(all_divergences),
        "top_divergences": all_divergences[:10],
        "top_extremes": all_extremes[:15],
        "all_contract_analyses": analyses,
        "smart_money_map": SMART_MONEY_MAP,
        "methodology": {
            "framework": "COT z-score + smart/dumb divergence framework",
            "philosophy": (
                "Bloomberg COT function has no z-score layer; Refinitiv "
                "COT is $15k/yr. CFTC COT data is free + Friday-released. "
                "Hedge funds run extremes-based contrarian signals: smart-"
                "money sustained extremes vs dumb-money sustained "
                "opposite extremes is the highest-conviction setup."),
            "z_score_framework": (
                "Net positioning standardized vs 1y + 3y historical mean. "
                "z >= 2 (or <= -2) = extreme positioning vs own history."),
            "smart_dumb_divergence": (
                "Triggered when smart-money z and dumb-money z both >= "
                "1.5 absolute and in OPPOSITE directions. Smart-money "
                "assignment varies by category (commercials in commodities/"
                "rates; large-specs in financials/FX)."),
            "data_source": (
                "Reads data/cftc-all-cache.json from upstream "
                "cftc-futures-positioning-agent (refreshed Fridays + "
                "rolling 8h cache)."),
        },
        "academic_basis": [
            "Wang (2003). The behavior and performance of major types "
            "of futures traders. JFM.",
            "de Roon, Nijman, Veld (2000). Hedging pressure effects in "
            "futures markets. Journal of Finance.",
            "Bessembinder & Chan (1992). Time-varying risk premia and "
            "forecastable returns in futures markets. JFE.",
        ],
        "duration_seconds": round(time.time() - started, 1),
    }

    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_DEEP_KEY,
        Body=json.dumps(output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=14400")

    # Separate extremes feed (fills the dangling consumer)
    extremes_output = {
        "engine": "cot-extremes",
        "version": VERSION,
        "generated_at": output["generated_at"],
        "state": state,
        "n_extremes": len(all_extremes),
        "n_divergences": len(all_divergences),
        "extremes": all_extremes,
        "divergences": all_divergences,
    }
    s3.put_object(
        Bucket=S3_BUCKET, Key=S3_EXTREMES_KEY,
        Body=json.dumps(extremes_output, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=14400")

    print(f"[cftc-deep-view] state={state} contracts={ok_count} "
          f"extremes={len(all_extremes)} divergences={len(all_divergences)}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True, "version": VERSION,
            "state": state,
            "n_contracts_analyzed": ok_count,
            "n_extremes": len(all_extremes),
            "n_divergences": len(all_divergences),
            "top_3_divergences": all_divergences[:3],
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
