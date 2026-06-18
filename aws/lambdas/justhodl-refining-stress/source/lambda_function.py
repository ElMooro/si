"""
justhodl-refining-stress
Refining-margin / physical-energy stress engine.

Computes the signals NO other engine in the fleet covers (verified 2026-06-18):
  - 3:2:1 crack spread (the benchmark US refining margin)
  - gasoline crack (RBOB - WTI) and distillate crack (ULSD - WTI)
  - Brent-WTI spread (transatlantic arb / US-glut tell)
  - Cushing, OK crude stocks (physical storage tightness)

Why it matters (institutional read):
  - Crack spreads are refiners' gross margin. Collapsing cracks => refiners cut
    runs => a clean *demand-destruction / recession* lead, and a margin signal for
    refiner equities (VLO/MPC/PSX). Very high cracks => tight products / strong demand.
  - Cushing stocks near tank lows => physical tightness => backwardation/bullish crude;
    near highs => glut. Percentile vs trailing history.
  - Brent-WTI wide => US oversupply / export-capacity constraint.

Free data: EIA Open Data v2 (petroleum spot + weekly stocks). Daily cracks, weekly Cushing.
Writes data/refining-stress.json to S3 (consumed by pages / signal-board). Auto-updates daily.
"""
import json, urllib.request, urllib.parse, datetime, os
import boto3

EIA_KEY = os.environ.get("EIA_API_KEY", "trvQDpg2GdvBixLeieVMyaQwsnkFQlYSuecVm4Pl")
BUCKET = os.environ.get("DASH_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/refining-stress.json"
GAL_PER_BBL = 42.0

# EIA v2 series ids
S_WTI   = "RWTC"                              # WTI Cushing spot $/bbl (daily)
S_BRENT = "RBRTE"                             # Brent spot $/bbl (daily)
S_RBOB  = "EER_EPMRU_PF4_Y35NY_DPG"           # NY Harbor RBOB gasoline spot $/gal (daily)
S_ULSD  = "EER_EPD2DXL0_PF4_Y35NY_DPG"        # NY Harbor ULSD (diesel) spot $/gal (daily)
S_CUSH  = "W_EPC0_SAX_YCUOK_MBBL"             # Cushing crude stocks, thousand bbl (weekly)


def _eia(route, series, freq, length=600):
    url = ("https://api.eia.gov/v2/%s/data/?api_key=%s&frequency=%s&data[0]=value"
           "&facets[series][]=%s&sort[0][column]=period&sort[0][direction]=desc&length=%d"
           % (route, EIA_KEY, freq, series, length))
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        d = json.loads(r.read().decode())
    rows = d.get("response", {}).get("data", [])
    # return ascending oldest->newest [(period, value)]
    out = []
    for rr in reversed(rows):
        v = rr.get("value")
        if v is None:
            continue
        try:
            out.append((rr.get("period"), float(v)))
        except (TypeError, ValueError):
            continue
    return out


def pctile(value, hist):
    h = sorted([x for x in hist if x is not None])
    if not h or value is None:
        return None
    below = sum(1 for x in h if x <= value)
    return round(below / len(h) * 100.0, 1)


def chg(series, n):
    """% change over last n observations (series is asc oldest->newest)."""
    if len(series) <= n or series[-1][1] is None or series[-(n + 1)][1] in (None, 0):
        return None
    return round((series[-1][1] / series[-(n + 1)][1] - 1) * 100, 2)


def metric(mid, label, value, unit, status, detail, pctl=None, asof=None):
    return {"id": mid, "label": label, "value": value, "unit": unit,
            "status": status, "detail": detail, "percentile": pctl, "asof": asof}


def lambda_handler(event, context):
    errs = []

    def safe(route, series, freq, length=600):
        try:
            s = _eia(route, series, freq, length)
            if not s:
                errs.append("%s empty" % series)
            return s
        except Exception as e:
            errs.append("%s %s" % (series, type(e).__name__))
            return []

    wti   = safe("petroleum/pri/spt", S_WTI,  "daily")
    brent = safe("petroleum/pri/spt", S_BRENT, "daily")
    rbob  = safe("petroleum/pri/spt", S_RBOB, "daily")
    ulsd  = safe("petroleum/pri/spt", S_ULSD, "daily")
    cush  = safe("petroleum/stoc/wstk", S_CUSH, "weekly")

    metrics = []

    # ---- build the daily crack history by aligning on common dates ----
    wmap = dict(wti); gmap = dict(rbob); dmap = dict(ulsd)
    common = sorted(set(wmap) & set(gmap) & set(dmap))
    crack_hist = []     # [(date, 321), ...]
    gcrack_hist, dcrack_hist = [], []
    for dt in common:
        w = wmap[dt]; g = gmap[dt] * GAL_PER_BBL; d = dmap[dt] * GAL_PER_BBL
        c321 = (2 * g + d - 3 * w) / 3.0
        crack_hist.append((dt, round(c321, 2)))
        gcrack_hist.append((dt, round(g - w, 2)))
        dcrack_hist.append((dt, round(d - w, 2)))

    if crack_hist:
        cd, cv = crack_hist[-1]
        hist = [v for _, v in crack_hist]
        pc = pctile(cv, hist)
        d1m = chg(crack_hist, 21)
        # LOW crack = refining/demand stress
        st = "green" if cv >= 18 else "yellow" if cv >= 9 else "red"
        if d1m is not None and d1m <= -30 and st == "green":
            st = "yellow"  # rapid margin collapse from a high base = early warning
        metrics.append(metric(
            "crack_321", "3:2:1 crack spread (refining margin)", round(cv, 2), "$/bbl", st,
            "Benchmark US refining margin (2 gasoline + 1 distillate vs 3 WTI). "
            "Collapsing cracks => refiners cut runs => demand-destruction / recession lead; "
            "very high => tight products. 21d chg %s%%." % ("n/a" if d1m is None else d1m),
            pc, cd))

    if gcrack_hist:
        gd, gv = gcrack_hist[-1]
        metrics.append(metric("crack_gasoline", "Gasoline crack (RBOB - WTI)", round(gv, 2), "$/bbl",
                              "green" if gv >= 12 else "yellow" if gv >= 5 else "red",
                              "Gasoline refining margin; seasonal peak summer driving.",
                              pctile(gv, [v for _, v in gcrack_hist]), gd))

    if dcrack_hist:
        dd, dv = dcrack_hist[-1]
        metrics.append(metric("crack_distillate", "Distillate crack (ULSD - WTI)", round(dv, 2), "$/bbl",
                              "green" if dv >= 18 else "yellow" if dv >= 8 else "red",
                              "Diesel/heating-oil margin; tracks freight, industrial & heating demand.",
                              pctile(dv, [v for _, v in dcrack_hist]), dd))

    # ---- Brent-WTI ----
    if wti and brent:
        bw = round(brent[-1][1] - wti[-1][1], 2)
        # build spread history on common dates
        bmap = dict(brent)
        bw_hist = [bmap[dt] - wmap[dt] for dt in sorted(set(bmap) & set(wmap))]
        metrics.append(metric("brent_wti", "Brent - WTI spread", bw, "$/bbl",
                              "green" if bw <= 6 else "yellow" if bw <= 10 else "red",
                              "Transatlantic spread. Wide (Brent >> WTI) => US crude oversupply / "
                              "export-capacity constraint; narrow/negative => tight US balances.",
                              pctile(bw, bw_hist), brent[-1][0]))

    # ---- Cushing stocks ----
    if cush:
        ckd, ckv = cush[-1]
        ckv_mb = round(ckv / 1000.0, 1)  # thousand bbl -> million bbl
        hist = [v / 1000.0 for _, v in cush]
        pc = pctile(ckv_mb, hist)
        # low stocks = physical tightness (flag both extremes)
        st = "yellow" if (pc is not None and (pc <= 10 or pc >= 90)) else "green"
        tl = "tank-bottom tight" if (pc is not None and pc <= 10) else \
             "glut" if (pc is not None and pc >= 90) else "normal"
        metrics.append(metric("cushing_stocks", "Cushing, OK crude stocks", ckv_mb, "M bbl", st,
                              "WTI delivery-point inventory (%s). Low => physical tightness / "
                              "backwardation pressure; high => oversupply." % tl, pc, ckd))

    # ---- composite regime (deterministic) ----
    reds = sum(1 for m in metrics if m["status"] == "red")
    yellows = sum(1 for m in metrics if m["status"] == "yellow")
    if reds >= 2:
        regime = "DEMAND STRESS"
    elif reds == 1 or yellows >= 2:
        regime = "SOFTENING"
    else:
        regime = "HEALTHY"

    c321 = next((m["value"] for m in metrics if m["id"] == "crack_321"), None)
    summary = ("Refining margins %s (3:2:1 crack $%s/bbl)." %
               ({"HEALTHY": "firm", "SOFTENING": "softening", "DEMAND STRESS": "stressed"}.get(regime, "—"),
                "n/a" if c321 is None else c321))

    out = {
        "engine": "justhodl-refining-stress",
        "generated": datetime.datetime.utcnow().isoformat() + "Z",
        "regime": regime,
        "summary": summary,
        "metrics": metrics,
        "errors": errs,
        "source": "EIA Open Data v2 (petroleum spot + weekly stocks)",
    }

    try:
        boto3.client("s3").put_object(
            Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
            ContentType="application/json", CacheControl="no-cache")
        out["s3"] = "s3://%s/%s" % (BUCKET, OUT_KEY)
    except Exception as e:
        out["s3_error"] = "%s: %s" % (type(e).__name__, e)

    return {"statusCode": 200,
            "body": json.dumps({"ok": True, "regime": regime, "metrics": len(metrics), "errors": errs})}
