"""justhodl-vol-radar -- the Volatility Turning-Point Radar.

The honest starting point: no engine reliably predicts the exact day
a volatility spike begins. If one could, it would own the market. The
naive version of "predict vol" -- VIX is low so it will spike -- is
just mean reversion wearing a costume.

What a real volatility desk does instead is read the tape for
PRIMING. Volatility spikes and volatility peaks are both preceded, far
more often than chance, by a recognisable cluster of leading
conditions -- canaries. This engine scores those canaries. It does
not promise a date; it says how primed the tape is, and it gives that
warning while vol is still calm (or still climbing), before the move.

It scores two sides, each from independent leading signals:

  SPIKE-RISK -- vol primed to break OUT. Canaries that cluster while
  vol is still low: VIX pinned at a low percentile, a steep VIX
  contango, a rich and crowded volatility risk premium, compressed
  realized vol, a VVIX divergence (the options market quietly bidding
  vol-of-vol), dealers short or thin on gamma, and credit or funding
  stress creeping up while equity vol has not noticed yet.

  EXHAUSTION -- vol primed to PEAK and mean-revert down. Canaries that
  cluster at spike climaxes: a deeply inverted VIX term structure,
  VIX and VVIX at panic extremes, a deeply negative VRP (realized
  having blown past implied), a firing capitulation signal, and
  credit stress fully priced.

The two scores plus the current vol level resolve to a posture --
COILED, WATCH, CALM, TRANSITIONAL, ELEVATED, TOPPING, PEAKING -- and
the engine names every canary that is firing, so the call is
transparent and falsifiable.

A probabilistic vulnerability radar on a hypothetical research book.
It shifts the odds and warns early; it is not a timing oracle, and it
is not investment advice.
"""
import json
import time
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/vol-radar.json"
HIST_KEY = "data/vol-radar-history.json"
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


def pctl(series, value):
    xs = [x for x in series if isinstance(x, (int, float))]
    if not xs or value is None:
        return None
    return round(100.0 * sum(1 for x in xs if x <= value) / len(xs), 1)


# --------------------------------------------------------------------------
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    vc = read_json("data/vix-curve.json") or {}
    vch = read_json("data/vix-curve-history.json") or {}
    vrp = read_json("data/vrp.json") or {}
    volreg = read_json("data/vol-regime.json") or {}
    dix = read_json("data/dix.json") or {}
    credit = read_json("data/credit-stress.json") or {}
    euro = read_json("data/eurodollar-stress.json") or {}
    capit = read_json("data/capitulation.json") or {}

    cur = vc.get("current") or {}
    spreads = vc.get("spreads") or {}
    vix = num(cur.get("vix"))
    vix9d = num(cur.get("vix9d"))
    vix3m = num(cur.get("vix3m"))
    vvix = num(cur.get("vvix"))
    vvix_ratio = num(cur.get("vvix_vix_ratio"))

    # VIX percentile vs its own history
    vix_series = (vch.get("series") or {}).get("vix") or []
    vix_pctl = pctl(vix_series, vix)

    # term-structure steepness: vix3m - vix9d.  positive = contango,
    # negative = backwardation (panic).
    if vix3m is not None and vix9d is not None:
        curve_steep = round(vix3m - vix9d, 2)
    else:
        curve_steep = num(spreads.get("3m_vs_6m"))  # weak fallback

    vrp_block = vrp.get("vrp") or {}
    vrp_30d = num(vrp_block.get("vrp_30d"))
    vrp_pctl = num(vrp_block.get("vrp_30d_percentile_1y"))
    rv_21d = num((vrp.get("realized") or {}).get("rv_21d"))

    # DIX / GEX -- dealer gamma sign
    dix_cur = dix.get("current") or {}
    gex_b = num(dix_cur.get("gex_billions"))
    if gex_b is None:
        gex_b = num(dix_cur.get("gex"))

    # credit stress -- pull the strongest HY z-score available
    cred_z = None
    cred_regime = ((credit.get("regimes") or {}).get("hy_regime")
                   if isinstance(credit.get("regimes"), dict) else None)
    cmet = credit.get("metrics") or credit.get("current") or {}
    if isinstance(cmet, dict):
        zs = []
        for v in cmet.values():
            if isinstance(v, dict) and isinstance(
                    v.get("z_score_60d"), (int, float)):
                zs.append(v["z_score_60d"])
        if zs:
            cred_z = max(zs)

    euro_score = num(euro.get("composite_score"))
    capit_score = num(capit.get("capitulation_score"))
    capit_signal = capit.get("signal")

    vol_low = (vix_pctl is not None and vix_pctl <= 45)
    vol_high = (vix_pctl is not None and vix_pctl >= 68)

    # ---- SPIKE-RISK canaries (vol primed to break out) ---------------
    spike = []          # (id, label, points, max, firing, detail)

    def sc(cid, label, pts, mx, detail):
        spike.append({"id": cid, "label": label, "points": pts,
                      "max": mx, "firing": pts >= mx * 0.5 and pts > 0,
                      "detail": detail})

    # C1 VIX pinned low
    if vix_pctl is not None:
        p = 2 if vix_pctl < 20 else 1 if vix_pctl < 38 else 0
        sc("vix_low", "VIX pinned at a low percentile", p, 2,
           "VIX %s at %.0fth pctl of its history"
           % (vix if vix is not None else "?", vix_pctl))
    # C2 steep contango
    if curve_steep is not None:
        p = 2 if curve_steep > 3.0 else 1 if curve_steep > 1.5 else 0
        sc("contango", "Steep VIX contango (curve pricing calm)", p, 2,
           "3m-9d spread %+.2f" % curve_steep)
    # C3 VRP rich / crowded
    if vrp_pctl is not None:
        p = 2 if vrp_pctl > 80 else 1 if vrp_pctl > 65 else 0
        sc("vrp_rich", "Volatility risk premium rich (crowded short)", p,
           2, "VRP at %.0fth pctl" % vrp_pctl)
    # C4 realized vol compressed
    if rv_21d is not None:
        p = 2 if rv_21d < 8 else 1 if rv_21d < 12 else 0
        sc("rv_compressed", "Realized vol compressed to the floor", p, 2,
           "21d realized %.1f" % rv_21d)
    # C5 VVIX divergence -- vol-of-vol bid while spot vol calm
    if vvix_ratio is not None and vol_low:
        p = 2 if vvix_ratio > 7.0 else 1 if vvix_ratio > 6.0 else 0
        sc("vvix_div", "VVIX divergence (quiet bid for tail vol)", p, 2,
           "VVIX/VIX ratio %.1f with vol low" % vvix_ratio)
    elif vvix_ratio is not None:
        sc("vvix_div", "VVIX divergence (quiet bid for tail vol)", 0, 2,
           "VVIX/VIX %.1f (vol not low -- n/a)" % vvix_ratio)
    # C6 dealer short / thin gamma
    if gex_b is not None:
        p = 2 if gex_b < 0 else 1 if gex_b < 0.5 else 0
        sc("short_gamma", "Dealers short or thin on gamma (amplifier)", p,
           2, "dealer GEX %.2fbn" % gex_b)
    # C7 credit creeping under a calm tape
    if cred_z is not None:
        if cred_z > 1.5 and vol_low:
            p = 2
        elif cred_z > 1.0:
            p = 1
        else:
            p = 0
        sc("credit_creep", "Credit stress creeping under calm equity vol",
           p, 2, "HY credit z-score %.2f" % cred_z)
    # C8 funding stress creeping
    if euro_score is not None and vol_low:
        p = 1 if euro_score > 40 else 0
        sc("funding_creep", "USD funding stress creeping", p, 1,
           "eurodollar stress %.0f/100" % euro_score)

    # ---- EXHAUSTION canaries (vol primed to peak and revert) ---------
    exh = []

    def ec(cid, label, pts, mx, detail):
        exh.append({"id": cid, "label": label, "points": pts,
                    "max": mx, "firing": pts >= mx * 0.5 and pts > 0,
                    "detail": detail})

    # E1 inverted term structure -- the strongest vol-top tell
    if curve_steep is not None:
        if curve_steep < -4.0:
            p = 3
        elif curve_steep < -2.0:
            p = 2
        elif curve_steep < 0.0:
            p = 1
        else:
            p = 0
        ec("backwardation", "VIX term structure inverted (panic)", p, 3,
           "3m-9d spread %+.2f" % curve_steep)
    # E2 VIX at a spike extreme
    if vix_pctl is not None:
        p = 2 if vix_pctl > 90 else 1 if vix_pctl > 80 else 0
        ec("vix_extreme", "VIX at a spike extreme", p, 2,
           "VIX %s at %.0fth pctl" % (vix if vix is not None else "?",
                                      vix_pctl))
    # E3 VVIX panic extreme
    if vvix is not None:
        p = 2 if vvix > 140 else 1 if vvix > 120 else 0
        ec("vvix_extreme", "VVIX at a panic extreme", p, 2,
           "VVIX %.0f" % vvix)
    # E4 VRP deeply negative
    if vrp_30d is not None:
        p = 2 if vrp_30d < -3.0 else 1 if vrp_30d < 0.0 else 0
        ec("vrp_negative", "VRP inverted (realized has overrun implied)",
           p, 2, "VRP %.1f" % vrp_30d)
    # E5 capitulation firing -- the climax tell
    if capit_score is not None:
        p = 3 if capit_score > 70 else 2 if capit_score > 50 else \
            1 if capit_score > 35 else 0
        ec("capitulation", "Capitulation / washout signal firing", p, 3,
           "capitulation %.0f/100 (%s)" % (capit_score,
                                           capit_signal or "n/a"))
    # E6 credit stress fully priced
    if cred_z is not None:
        p = 2 if cred_z > 2.5 else 1 if cred_z > 1.8 else 0
        ec("credit_priced", "Credit stress fully priced (extreme z)", p, 2,
           "HY credit z-score %.2f" % cred_z)

    spike_max = sum(c["max"] for c in spike) or 1
    exh_max = sum(c["max"] for c in exh) or 1
    spike_risk = round(100.0 * sum(c["points"] for c in spike) / spike_max)
    exhaustion = round(100.0 * sum(c["points"] for c in exh) / exh_max)
    spike_firing = [c["label"] for c in spike if c["firing"]]
    exh_firing = [c["label"] for c in exh if c["firing"]]

    # ---- posture -------------------------------------------------------
    feeds_seen = sum(1 for x in (vc, vrp, dix, credit, euro, capit, volreg)
                     if x)
    if vix_pctl is None or feeds_seen < 3:
        posture, pcolor = "INSUFFICIENT DATA", "dim"
        headline = ("Vol radar cannot resolve -- only %d of 7 canary "
                    "feeds available." % feeds_seen)
        precedes = watch = None
    elif vol_low and spike_risk >= 60:
        posture, pcolor = "COILED", "orange"
        headline = ("Volatility COILED -- vol sits low but %d spike "
                    "canaries are firing (spike-risk %d/100). The tape is "
                    "primed for a volatility expansion."
                    % (len(spike_firing), spike_risk))
        precedes = ("Configurations like this -- low vol, steep curve, a "
                    "crowded vol-risk premium -- precede vol spikes far "
                    "more often than a calm tape does. Not a date; a "
                    "loaded spring.")
        watch = "the first uptick in VIX or a VVIX break higher"
    elif vol_low and spike_risk >= 40:
        posture, pcolor = "WATCH", "yellow"
        headline = ("Volatility low but on WATCH -- early spike canaries "
                    "are appearing (spike-risk %d/100, %d firing)."
                    % (spike_risk, len(spike_firing)))
        precedes = ("Not yet a loaded spring, but the leading signals "
                    "have started to cluster. Worth watching closely.")
        watch = "whether more canaries join over the next sessions"
    elif vol_high and exhaustion >= 60:
        posture, pcolor = "PEAKING", "green"
        headline = ("Volatility PEAKING -- vol is elevated and %d "
                    "exhaustion canaries are firing (exhaustion %d/100). "
                    "The spike is likely near its climax."
                    % (len(exh_firing), exhaustion))
        precedes = ("Inverted term structure, VVIX extremes and "
                    "capitulation cluster at vol PEAKS -- this is the "
                    "configuration that precedes mean-reversion lower in "
                    "vol, not further escalation.")
        watch = "the VIX curve starting to un-invert (9d falling fastest)"
    elif vol_high and exhaustion >= 40:
        posture, pcolor = "TOPPING", "cyan"
        headline = ("Volatility elevated and TOPPING -- exhaustion "
                    "canaries are building (exhaustion %d/100, %d firing)."
                    % (exhaustion, len(exh_firing)))
        precedes = ("Exhaustion is accumulating but not yet decisive -- "
                    "vol can still extend before it rolls.")
        watch = "a capitulation print or a VVIX blow-off to confirm"
    elif vol_high:
        posture, pcolor = "ELEVATED", "red"
        headline = ("Volatility ELEVATED with no exhaustion yet "
                    "(exhaustion %d/100). High vol that is not primed to "
                    "peak can still extend." % exhaustion)
        precedes = ("Elevated vol without exhaustion canaries has no "
                    "mean-reversion signal -- do not fade it yet.")
        watch = "term-structure inversion or a capitulation washout"
    elif spike_risk >= 55:
        posture, pcolor = "COILED", "orange"
        headline = ("Volatility COILED at mid-range -- spike canaries are "
                    "firing (spike-risk %d/100, %d firing)."
                    % (spike_risk, len(spike_firing)))
        precedes = ("Mid-range vol with primed spike canaries still "
                    "skews toward expansion.")
        watch = "a VVIX break or curve flattening"
    elif exhaustion >= 55:
        posture, pcolor = "TOPPING", "cyan"
        headline = ("Volatility TOPPING from mid-range -- exhaustion "
                    "canaries firing (exhaustion %d/100)." % exhaustion)
        precedes = "Exhaustion signals skew toward vol rolling over."
        watch = "confirmation from capitulation or the VIX curve"
    else:
        posture, pcolor = "CALM", "green"
        headline = ("Volatility CALM -- few canaries firing on either "
                    "side (spike-risk %d, exhaustion %d). No turning "
                    "point primed." % (spike_risk, exhaustion))
        precedes = ("Neither a loaded spring nor an exhausted spike -- "
                    "the radar sees no turning point setting up.")
        watch = "the first cluster of spike canaries to appear"

    bits = [headline]
    if precedes:
        bits.append(precedes)
    dominant_firing = (spike_firing if spike_risk >= exhaustion
                       else exh_firing)
    if dominant_firing:
        bits.append("Canaries firing: " + "; ".join(dominant_firing) + ".")
    if watch:
        bits.append("Watch next: " + watch + ".")
    radar_note = " ".join(bits)

    out = {
        "schema_version": SCHEMA,
        "engine": "justhodl-vol-radar",
        "generated_at": now.isoformat(),
        "build_seconds": round(time.time() - t0, 2),

        "posture": posture,
        "posture_color": pcolor,
        "headline": headline,
        "radar_note": radar_note,

        "scores": {
            "spike_risk": spike_risk,
            "exhaustion": exhaustion,
            "dominant": ("SPIKE-RISK" if spike_risk > exhaustion
                         else "EXHAUSTION" if exhaustion > spike_risk
                         else "BALANCED"),
        },
        "vol_state": {
            "vix": vix,
            "vix_percentile": vix_pctl,
            "vix_level": ("LOW" if vol_low else "HIGH" if vol_high
                          else "MID"),
            "vix9d": vix9d, "vix3m": vix3m, "vvix": vvix,
            "term_structure": (
                "INVERTED" if (curve_steep is not None and curve_steep < 0)
                else "CONTANGO" if curve_steep is not None else None),
            "curve_3m_minus_9d": curve_steep,
        },
        "spike_canaries": spike,
        "exhaustion_canaries": exh,
        "spike_canaries_firing": spike_firing,
        "exhaustion_canaries_firing": exh_firing,
        "watch_next": watch,

        "inputs": {
            "vrp_regime": vrp.get("regime"),
            "vol_regime": volreg.get("composite_regime"),
            "credit_regime": cred_regime,
            "eurodollar_severity": euro.get("severity"),
            "capitulation_signal": capit_signal,
            "feeds_available": "%d/7" % feeds_seen,
        },
        "how_to_read": (
            "A conditional volatility turning-point radar. Spike-risk "
            "scores how primed the tape is for a volatility expansion "
            "from leading canaries that cluster before spikes; exhaustion "
            "scores how primed vol is to peak and mean-revert from "
            "canaries that cluster at climaxes. COILED warns of a spike "
            "while vol is still low; PEAKING warns of a top while vol is "
            "still high. It shifts probabilities and warns early -- it "
            "does not predict a date."),
        "disclaimer": (
            "A probabilistic vulnerability radar on a hypothetical "
            "research book, built from leading indicators. It is not a "
            "timing oracle and not investment advice."),
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
            "posture": posture, "spike_risk": spike_risk,
            "exhaustion": exhaustion, "vix": vix,
            "vix_percentile": vix_pctl,
        })
        hsnaps = hsnaps[-365:]
        s3.put_object(
            Bucket=BUCKET, Key=HIST_KEY,
            Body=json.dumps({"schema_version": SCHEMA, "engine":
                             "justhodl-vol-radar",
                             "updated_at": now.isoformat(),
                             "snapshots": hsnaps},
                            default=str).encode("utf-8"),
            ContentType="application/json")
    except Exception as e:
        print("history write fail: %s" % e)

    return {"statusCode": 200, "body": json.dumps({
        "ok": posture != "INSUFFICIENT DATA", "posture": posture,
        "spike_risk": spike_risk, "exhaustion": exhaustion,
        "vix": vix, "vix_percentile": vix_pctl,
        "feeds": "%d/7" % feeds_seen})}
