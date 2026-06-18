"""
justhodl-refining-stress
Refining-margin / physical-energy stress engine.

Fills verified fleet white-space (2026-06-18): NO other engine computes crack
spreads / refining margins. Signals:
  - 3:2:1 crack spread (benchmark US refining margin)
  - gasoline crack (RBOB - WTI), distillate crack (ULSD/HO - WTI)
  - Brent-WTI spread (transatlantic arb / US-glut tell)

Institutional read: crack spreads are refiners' gross margin. Collapsing cracks
=> refiners cut runs => a clean demand-destruction / recession lead, and a margin
signal for refiner equities (VLO/MPC/PSX). Very high cracks => tight products /
strong demand. Brent-WTI wide => US oversupply / export-capacity constraint.

Data: FMP commodity futures (CL/BZ/RB/HO), single-source so the crack is
internally consistent (same-exchange, same-timestamp NYMEX) — the conventionally
correct way to compute a futures crack. History via FMP EOD for percentiles.
FRED WTI is pulled only as a data-quality cross-check (FRED daily spot lags ~2-3d).
Cushing physical stocks intentionally omitted: the EIA API key is dead fleet-wide
(separate issue) — will add when a valid EIA key is restored.

Writes data/refining-stress.json daily. Auto-updates.
"""
import json, urllib.request, datetime, os
import boto3

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
BUCKET = os.environ.get("DASH_BUCKET", "justhodl-dashboard-live")
OUT_KEY = "data/refining-stress.json"
GAL = 42.0


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl/1.0"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read().decode()


def fmp_hist(symbol, days=760):
    """FMP EOD history -> asc [(date, close)]."""
    frm = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    url = ("https://financialmodelingprep.com/stable/historical-price-eod/full"
           "?symbol=%s&from=%s&apikey=%s" % (symbol, frm, FMP_KEY))
    d = json.loads(_get(url))
    rows = d.get("historical", d) if isinstance(d, dict) else d
    out = []
    for rr in rows:
        dt = rr.get("date")
        c = rr.get("close", rr.get("price", rr.get("adjClose")))
        if dt and c is not None:
            try:
                out.append((dt[:10], float(c)))
            except (TypeError, ValueError):
                pass
    out.sort()
    return out


def fred_latest(sid):
    try:
        url = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s"
               "&api_key=%s&file_type=json&sort_order=desc&limit=1" % (sid, FRED_KEY))
        o = json.loads(_get(url))["observations"][0]
        return o["date"], float(o["value"])
    except Exception:
        return None, None


def pctile(value, hist):
    h = sorted([x for x in hist if x is not None])
    if not h or value is None:
        return None
    return round(sum(1 for x in h if x <= value) / len(h) * 100.0, 1)


def chg(series, n):
    if len(series) <= n or series[-1][1] is None or series[-(n + 1)][1] in (None, 0):
        return None
    return round((series[-1][1] / series[-(n + 1)][1] - 1) * 100, 2)


def metric(mid, label, value, unit, status, detail, pctl=None, asof=None):
    return {"id": mid, "label": label, "value": value, "unit": unit,
            "status": status, "detail": detail, "percentile": pctl, "asof": asof}


def lambda_handler(event, context):
    errs = []

    def safe(sym):
        try:
            s = fmp_hist(sym)
            if not s:
                errs.append("%s empty" % sym)
            return s
        except Exception as e:
            errs.append("%s %s" % (sym, type(e).__name__))
            return []

    wti = safe("CLUSD"); brent = safe("BZUSD"); rbob = safe("RBUSD"); ulsd = safe("HOUSD")
    metrics = []

    wmap, gmap, dmap, bmap = dict(wti), dict(rbob), dict(ulsd), dict(brent)
    common = sorted(set(wmap) & set(gmap) & set(dmap))
    crack_hist, gcrack_hist, dcrack_hist = [], [], []
    for dt in common:
        w = wmap[dt]; g = gmap[dt] * GAL; d = dmap[dt] * GAL
        crack_hist.append((dt, round((2 * g + d - 3 * w) / 3.0, 2)))
        gcrack_hist.append((dt, round(g - w, 2)))
        dcrack_hist.append((dt, round(d - w, 2)))

    if crack_hist:
        cd, cv = crack_hist[-1]
        pc = pctile(cv, [v for _, v in crack_hist]); d1m = chg(crack_hist, 21)
        st = "green" if cv >= 18 else "yellow" if cv >= 9 else "red"
        if d1m is not None and d1m <= -30 and st == "green":
            st = "yellow"
        metrics.append(metric(
            "crack_321", "3:2:1 crack spread (refining margin)", round(cv, 2), "$/bbl", st,
            "Benchmark US refining margin (2 gasoline + 1 distillate vs 3 WTI). Collapsing => "
            "refiners cut runs => demand-destruction / recession lead. 21d chg %s%%."
            % ("n/a" if d1m is None else d1m), pc, cd))

    if gcrack_hist:
        gd, gv = gcrack_hist[-1]
        metrics.append(metric("crack_gasoline", "Gasoline crack (RBOB - WTI)", round(gv, 2), "$/bbl",
                              "green" if gv >= 12 else "yellow" if gv >= 5 else "red",
                              "Gasoline refining margin; peaks in summer driving season.",
                              pctile(gv, [v for _, v in gcrack_hist]), gd))

    if dcrack_hist:
        dd, dv = dcrack_hist[-1]
        metrics.append(metric("crack_distillate", "Distillate crack (heating oil - WTI)", round(dv, 2), "$/bbl",
                              "green" if dv >= 18 else "yellow" if dv >= 8 else "red",
                              "Diesel/heating-oil margin; tracks freight, industrial & heating demand.",
                              pctile(dv, [v for _, v in dcrack_hist]), dd))

    if wti and brent:
        common2 = sorted(set(wmap) & set(bmap))
        bw = round(brent[-1][1] - wti[-1][1], 2)
        bw_hist = [bmap[dt] - wmap[dt] for dt in common2]
        metrics.append(metric("brent_wti", "Brent - WTI spread", bw, "$/bbl",
                              "green" if bw <= 6 else "yellow" if bw <= 10 else "red",
                              "Transatlantic spread. Wide (Brent >> WTI) => US crude oversupply / "
                              "export-capacity constraint; narrow/negative => tight US balances.",
                              pctile(bw, bw_hist), brent[-1][0]))

    # data-quality cross-check: FMP front-month vs FRED spot (FRED lags ~2-3d)
    dq = {}
    if wti:
        fwd, fwv = fred_latest("DCOILWTICO")
        if fwv:
            dev = round((wti[-1][1] / fwv - 1) * 100, 1)
            dq = {"fmp_wti": wti[-1][1], "fred_wti": fwv, "fred_asof": fwd, "deviation_pct": dev,
                  "note": ("OK" if abs(dev) <= 8 else
                           "FMP front-month vs FRED spot diverge %s%% — FRED likely lagged to %s" % (dev, fwd))}

    reds = sum(1 for m in metrics if m["status"] == "red")
    yellows = sum(1 for m in metrics if m["status"] == "yellow")
    regime = "DEMAND STRESS" if reds >= 2 else "SOFTENING" if (reds == 1 or yellows >= 2) else "HEALTHY"
    c321 = next((m["value"] for m in metrics if m["id"] == "crack_321"), None)
    summary = ("Refining margins %s (3:2:1 crack $%s/bbl)." % (
        {"HEALTHY": "firm", "SOFTENING": "softening", "DEMAND STRESS": "stressed"}.get(regime, "—"),
        "n/a" if c321 is None else c321))

    out = {"engine": "justhodl-refining-stress",
           "generated": datetime.datetime.utcnow().isoformat() + "Z",
           "regime": regime, "summary": summary, "metrics": metrics,
           "data_quality": dq, "errors": errs,
           "source": "FMP commodity futures (CL/BZ/RB/HO); FRED WTI cross-check",
           "note_cushing": "Cushing physical-stocks leg pending valid EIA key (dead fleet-wide)"}
    try:
        boto3.client("s3").put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                                      ContentType="application/json", CacheControl="no-cache")
    except Exception as e:
        out["s3_error"] = "%s: %s" % (type(e).__name__, e)
    return {"statusCode": 200, "body": json.dumps({"ok": True, "regime": regime,
            "metrics": len(metrics), "errors": errs, "dq": dq.get("note", "")})}
