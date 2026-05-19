"""justhodl-market-extremes -- the Market Cycle Extremes Radar.

The honest starting point, the same as for volatility: nobody picks
the exact day of a market top or bottom. What is real is that major
tops and major bottoms each carry a recognisable HISTORICAL signature
-- a cluster of conditions that, far more often than chance, are
present near the turn. This engine scores those signatures.

It reads two extremes:

  TOP-RISK / euphoria -- the signs that cluster near major market
  tops: stretched valuations, a narrowing breadth (the index making
  highs on fewer and fewer names -- the classic divergence), euphoric
  sentiment, a complacent volatility risk premium, credit spreads
  pinned tight, insiders distributing into strength, and retail in
  full FOMO.

  CAPITULATION / bottom -- the platform already runs a dedicated
  Capitulation engine for the washout side. This radar does not
  recompute it; it consumes its score directly, so the two extremes
  sit on one dial.

The two resolve to a cycle posture -- CAPITULATION, ACCUMULATION,
EXPANSION, DISTRIBUTION, EUPHORIA -- and a single cycle-position dial
from 0 (deep washout) to 100 (deep euphoria). Every firing sign is
named, so the read is transparent and falsifiable.

A probabilistic cycle radar on a hypothetical research book. It reads
the historical signature and warns when a turn is primed; it does not
call the day, and it is not investment advice.
"""
import json
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/market-extremes.json"
HIST_KEY = "data/market-extremes-history.json"
SCHEMA = "1.0"

s3 = boto3.client("s3", region_name=REGION)


# --------------------------------------------------------------------------
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def num(v):
    try:
        return None if v is None else float(v)
    except Exception:
        return None


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def find_metric(metrics, *needles):
    """Find a metric dict whose name contains any needle."""
    for m in metrics or []:
        if not isinstance(m, dict):
            continue
        name = str(m.get("name") or "").lower()
        if any(n in name for n in needles):
            return m
    return None


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    vals = read_json("valuations-data.json") or {}
    internals = read_json("data/market-internals.json") or {}
    aaii = read_json("data/aaii-sentiment.json") or {}
    credit = read_json("data/credit-stress.json") or {}
    insider = read_json("data/insider-aggregate.json") or {}
    retail = read_json("data/retail-sentiment.json") or {}
    vrp = read_json("data/vrp.json") or {}
    capit = read_json("data/capitulation.json") or {}

    # ---- TOP-RISK canaries (signs that cluster near major tops) ------
    top = []          # (id, label, points, max, firing, detail)

    def tc(cid, label, pts, mx, detail):
        top.append({"id": cid, "label": label, "points": pts, "max": mx,
                    "firing": pts >= mx * 0.5 and pts > 0, "detail": detail})

    # T1 valuation extreme -- Shiller CAPE stretched vs its own average
    all_metrics = vals.get("all_metrics") or []
    sp = vals.get("sp500") or {}
    cape_m = find_metric(all_metrics, "cape", "shiller")
    cape_pct = num(cape_m.get("pct_above_avg")) if cape_m else None
    cape_val = num(cape_m.get("value")) if cape_m else num(sp.get("cape"))
    if cape_pct is not None:
        p = 2 if cape_pct > 40 else 1 if cape_pct > 20 else 0
        tc("valuation_extreme", "Equity valuations stretched", p, 2,
           "Shiller CAPE %s, %+.0f%% vs its historical average"
           % (cape_val if cape_val is not None else "?", cape_pct))
    else:
        ov = (vals.get("summary") or {}).get("overvalued_count")
        if isinstance(ov, (int, float)):
            p = 2 if ov >= 6 else 1 if ov >= 3 else 0
            tc("valuation_extreme", "Equity valuations stretched", p, 2,
               "%d valuation gauges reading overvalued" % ov)

    # T2 breadth divergence -- the index riding fewer and fewer names
    breadth = num(internals.get("breadth_score"))
    if breadth is not None:
        p = 2 if breadth < 40 else 1 if breadth < 52 else 0
        tc("breadth_divergence", "Breadth narrowing under the index", p, 2,
           "market-internals breadth score %.0f/100 (%s)"
           % (breadth, internals.get("state") or "?"))

    # T3 sentiment euphoria -- AAII bulls crowded
    latest = aaii.get("latest") or {}
    extremes = aaii.get("extremes") or {}
    spread = num(latest.get("bull_bear_spread"))
    bull_ext = bool(extremes.get("is_bullish_extreme"))
    if spread is not None or bull_ext:
        if bull_ext:
            p = 2
        elif spread is not None and spread > 18:
            p = 1
        else:
            p = 0
        tc("sentiment_euphoria", "Investor sentiment euphoric", p, 2,
           "AAII bull-bear spread %s%s"
           % ("%+.0f" % spread if spread is not None else "?",
              ", bullish extreme" if bull_ext else ""))

    # T4 complacency -- a rich, crowded volatility risk premium
    vrp_regime = vrp.get("regime")
    vrp_pctl = num((vrp.get("vrp") or {}).get("vrp_30d_percentile_1y"))
    if vrp_regime or vrp_pctl is not None:
        if vrp_regime == "RICH":
            p = 2
        elif vrp_pctl is not None and vrp_pctl > 65:
            p = 1
        else:
            p = 0
        tc("complacency", "Volatility priced for calm (complacency)", p, 2,
           "VRP regime %s%s" % (vrp_regime or "?",
           ", %.0fth pctl" % vrp_pctl if vrp_pctl is not None else ""))

    # T5 credit complacency -- HY spreads pinned tight
    cred_z = None
    cmet = credit.get("metrics") or credit.get("current") or {}
    if isinstance(cmet, dict):
        zs = [v["z_score_60d"] for v in cmet.values()
              if isinstance(v, dict)
              and isinstance(v.get("z_score_60d"), (int, float))]
        if zs:
            cred_z = min(zs)   # most-negative = tightest
    if cred_z is not None:
        p = 2 if cred_z < -1.2 else 1 if cred_z < -0.4 else 0
        tc("credit_complacency", "Credit spreads pinned tight", p, 2,
           "HY credit z-score %.2f (negative = tight)" % cred_z)

    # T6 insider distribution -- insiders selling into strength
    ins_30 = ((insider.get("windows") or {}).get("last_30d") or {})
    ins_ratio = num(ins_30.get("buy_sell_ratio_dollar"))
    if ins_ratio is not None:
        p = 2 if ins_ratio < 0.30 else 1 if ins_ratio < 0.60 else 0
        tc("insider_distribution", "Insiders distributing into strength",
           p, 2, "30d insider buy/sell dollar ratio %.2f" % ins_ratio)

    # T7 retail FOMO
    r_ratio = num(retail.get("bull_bear_ratio"))
    r_bull = num(retail.get("bull_pct"))
    if r_ratio is not None or r_bull is not None:
        if r_ratio is not None and r_ratio > 2.5:
            p = 1
        elif r_bull is not None and r_bull > 70:
            p = 1
        else:
            p = 0
        tc("retail_fomo", "Retail crowd in FOMO", p, 1,
           "retail bull/bear ratio %s"
           % (round(r_ratio, 2) if r_ratio is not None else
              ("%.0f%% bulls" % r_bull if r_bull is not None else "?")))

    top_max = sum(c["max"] for c in top) or 1
    top_risk = round(100.0 * sum(c["points"] for c in top) / top_max)
    top_firing = [c["label"] for c in top if c["firing"]]

    # ---- BOTTOM side -- consume the Capitulation engine --------------
    capit_score = num(capit.get("capitulation_score"))
    capit_signal = capit.get("signal")
    capit_stabilising = capit.get("stabilising")

    # ---- cycle position + posture ------------------------------------
    feeds_seen = sum(1 for x in (vals, internals, aaii, credit, insider,
                                 retail, vrp, capit) if x)
    cs = capit_score if capit_score is not None else 0.0
    cycle_position = round(clamp(50.0 + top_risk / 2.0 - cs / 2.0, 0, 100))

    if feeds_seen < 4 or (top_risk == 0 and capit_score is None):
        posture, pcolor = "INSUFFICIENT DATA", "dim"
        headline = ("Cycle radar cannot resolve -- only %d of 8 feeds "
                    "available." % feeds_seen)
        precedes = None
    elif capit_score is not None and capit_score >= 60:
        posture, pcolor = "CAPITULATION", "green"
        headline = ("Market in CAPITULATION -- the washout score is "
                    "%.0f/100. Panic has reached the extremes that, "
                    "historically, mark generational bottoms."
                    % capit_score)
        precedes = ("Washout extremes -- forced selling, breadth wipeout, "
                    "sentiment despair -- have clustered at the best "
                    "entries of the cycle. Not a date; a historic zone.")
    elif capit_score is not None and capit_score >= 38:
        posture, pcolor = "ACCUMULATION", "cyan"
        headline = ("Market in ACCUMULATION -- the washout score is "
                    "%.0f/100 and building. The contraction is maturing "
                    "toward a capitulation low." % capit_score)
        precedes = ("Partial washout -- the bottoming process is under "
                    "way but not yet at a panic extreme.")
    elif top_risk >= 65:
        posture, pcolor = "EUPHORIA", "red"
        headline = ("Market in EUPHORIA -- top-risk %d/100, with %d "
                    "top-signature signs firing. The configuration that "
                    "clusters near major tops is in place."
                    % (top_risk, len(top_firing)))
        precedes = ("Stretched valuations, narrowing breadth and euphoric "
                    "sentiment together have marked major distribution "
                    "tops -- this is the signature, not the timestamp.")
    elif top_risk >= 45:
        posture, pcolor = "DISTRIBUTION", "yellow"
        headline = ("Market in DISTRIBUTION -- top-risk %d/100, %d "
                    "top-signature signs firing. Euphoria signs are "
                    "accumulating but not yet decisive."
                    % (top_risk, len(top_firing)))
        precedes = ("The top signature is forming -- worth tracking "
                    "closely, not yet a complete cluster.")
    else:
        posture, pcolor = "EXPANSION", "mute"
        headline = ("Market in mid-cycle EXPANSION -- top-risk %d/100, "
                    "washout %s. Neither a euphoric top nor a washout "
                    "bottom is primed."
                    % (top_risk, "%.0f/100" % capit_score
                       if capit_score is not None else "n/a"))
        precedes = ("The cycle radar sees no extreme setting up -- the "
                    "market sits between the turns.")

    bits = [headline]
    if precedes:
        bits.append(precedes)
    if posture in ("EUPHORIA", "DISTRIBUTION") and top_firing:
        bits.append("Top signs firing: " + "; ".join(top_firing) + ".")
    if posture in ("CAPITULATION", "ACCUMULATION"):
        if capit_signal:
            bits.append("Capitulation engine signal: %s%s."
                        % (capit_signal,
                           " (stabilising)" if capit_stabilising else ""))
    radar_note = " ".join(bits)

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-market-extremes",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),

        "posture": posture,
        "posture_color": pcolor,
        "headline": headline,
        "radar_note": radar_note,

        "cycle_position": cycle_position,
        "cycle_position_scale": "0 = deep capitulation / 100 = deep euphoria",
        "scores": {
            "top_risk": top_risk,
            "capitulation": capit_score,
        },

        "top_canaries": top,
        "top_canaries_firing": top_firing,

        "bottom": {
            "capitulation_score": capit_score,
            "signal": capit_signal,
            "stabilising": capit_stabilising,
            "source": "justhodl-capitulation (consumed, not recomputed)",
        },

        "inputs": {
            "cape_pct_above_avg": cape_pct,
            "breadth_score": breadth,
            "aaii_bull_bear_spread": spread,
            "vrp_regime": vrp_regime,
            "hy_credit_z": cred_z,
            "insider_buy_sell_ratio_30d": ins_ratio,
            "feeds_available": "%d/8" % feeds_seen,
        },
        "how_to_read": (
            "A conditional market cycle radar. Top-risk scores how "
            "complete the historical top signature is -- stretched "
            "valuations, narrowing breadth, euphoric sentiment, "
            "complacent vol and credit, insider distribution, retail "
            "FOMO. The capitulation score is read straight from the "
            "dedicated Capitulation engine. The cycle-position dial puts "
            "the market between a washout bottom (0) and a euphoric top "
            "(100). It reads the signature and warns when a turn is "
            "primed -- it does not call the day."),
        "disclaimer": (
            "A probabilistic cycle radar on a hypothetical research "
            "book, built from historical turn signatures. It is not a "
            "market-timing oracle and not investment advice."),
    }

    try:
        s3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                      Body=json.dumps(out, default=str).encode("utf-8"),
                      ContentType="application/json")
    except Exception as e:
        print("output write fail: %s" % e)

    try:
        hist = read_json(HIST_KEY)
        hsnaps = hist.get("snapshots") if isinstance(hist, dict) else []
        today = now.date().isoformat()
        hsnaps = [x for x in (hsnaps or []) if x.get("date") != today]
        hsnaps.append({
            "date": today, "generated_at": now.isoformat(),
            "posture": posture, "top_risk": top_risk,
            "capitulation": capit_score, "cycle_position": cycle_position,
        })
        hsnaps = hsnaps[-365:]
        s3.put_object(
            Bucket=BUCKET, Key=HIST_KEY,
            Body=json.dumps({"schema_version": SCHEMA, "engine":
                             "justhodl-market-extremes",
                             "updated_at": now.isoformat(),
                             "snapshots": hsnaps},
                            default=str).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print("history write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": posture != "INSUFFICIENT DATA", "posture": posture,
        "top_risk": top_risk, "capitulation": capit_score,
        "cycle_position": cycle_position,
        "feeds": "%d/8" % feeds_seen})}
