"""justhodl-freight-pulse v1.0 — US freight canary composite.

Khalid's directive: freight as a leading canary for trade/manufacturing —
anticipate slowdown or acceleration before it prints in GDP. Sources (all
FRED, monthly): DOT Freight TSI, Cass Freight shipments & expenditures,
ATA truck tonnage, AAR rail carloads & intermodal. Per series: level, yoy,
6m annualized slope, z vs 5y; composite FREIGHT_PULSE −100..+100 with
ACCELERATING / STABLE / DECELERATING verdict + inflection flag when slope
sign diverges from yoy (turning points). Feeds data/freight-pulse.json.
Pairs with portwatch exporters pulse (origin gateways) for the full chain:
foreign port -> US freight -> economy. stdlib-only; never fabricates.
"""
import json
import os
import urllib.request
from datetime import datetime, timezone

import boto3

from impact_mapper import (build as impact_build,
                           structural_row, beta_impact)

VERSION = "2.0.0"
BUCKET = "justhodl-dashboard-live"
KEY = "data/freight-pulse.json"
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
S3 = boto3.client("s3", region_name="us-east-1")

SERIES = {
    "tsi_freight": ("TSIFRGHT", "DOT Freight Transportation Services Index"),
    "cass_shipments": ("FRGSHPUSM649NCIS", "Cass Freight Index: Shipments"),
    "cass_expend": ("FRGEXPUSM649NCIS", "Cass Freight Index: Expenditures"),
    "truck_tonnage": ("TRUCKD11", "ATA Truck Tonnage Index"),
    "rail_carloads": ("RAILFRTCARLOADSD11", "Rail Freight Carloads"),
    "rail_intermodal": ("RAILFRTINTERMODALD11", "Rail Freight Intermodal"),
}


_EIA = {}


def _eia_key():
    if _EIA.get("k") is not None:
        return _EIA["k"]
    k = os.environ.get("EIA_API_KEY", "")
    if not k:
        try:
            k = boto3.client("ssm", region_name="us-east-1").get_parameter(
                Name="/justhodl/eia-api-key",
                WithDecryption=True)["Parameter"]["Value"]
        except Exception:
            k = ""
    _EIA["k"] = k
    return k


def _eia_weekly(sid):
    """EIA v2 seriesid route → [(period, value)] ascending. Real weekly data
    or an error string — never a fabricated series."""
    k = _eia_key()
    if not k:
        return [], "no EIA key (env EIA_API_KEY / ssm /justhodl/eia-api-key)"
    u = ("https://api.eia.gov/v2/seriesid/%s?api_key=%s" % (sid, k))
    try:
        r = urllib.request.urlopen(u, timeout=25)
        data = ((json.loads(r.read()).get("response") or {}).get("data")) or []
        pts = []
        for o in data:
            v = o.get("value")
            d = o.get("period")
            if v is None or not d:
                continue
            try:
                pts.append((str(d), float(v)))
            except Exception:
                continue
        pts.sort(key=lambda x: x[0])
        return pts[-170:], None
    except Exception as e:
        return [], "%s: %s" % (type(e).__name__, str(e)[:80])


def _fred(sid):
    u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
         f"&api_key={FRED_KEY}&file_type=json&observation_start=2015-01-01")
    try:
        r = urllib.request.urlopen(u, timeout=25)
        obs = json.loads(r.read()).get("observations") or []
        pts = [(o["date"], float(o["value"])) for o in obs
               if o.get("value") not in (None, ".", "")]
        return pts, None
    except Exception as e:
        return [], str(e)[:100]


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"ok": False, "version": VERSION, "generated_at": now.isoformat(),
           "engine_class": "physical_trade_slow_confirmation",
           "composite_role": "slow_confirmation_leg",
           "lag_months": -2,
           "role_note": ("ops-4559 BUG-13: six US monthly FRED series on a 2-3mo "
                         "lag cannot lead anything — this is the CONFIRMATION leg. "
                         "The fast layer is port-cargo (daily tons), Korea 20-day "
                         "exports and Kiel port calls."),
           "series": {}, "errors": []}
    scores = []
    for key, (sid, name) in SERIES.items():
        pts, err = _fred(sid)
        if err or len(pts) < 30:
            out["errors"].append(f"{key}: {err or 'short'}")
            out["series"][key] = {"name": name, "ok": False, "err": err}
            continue
        vals = [v for _, v in pts]
        latest_d, latest = pts[-1]
        yoy = (100 * (latest / vals[-13] - 1)) if len(vals) >= 13 and vals[-13] else None
        m6 = (100 * ((latest / vals[-7]) ** 2 - 1)) if len(vals) >= 7 and vals[-7] else None
        base = vals[-60:]
        mean = sum(base) / len(base)
        sd = (sum((x - mean) ** 2 for x in base) / len(base)) ** 0.5 or 1e-9
        z = round((latest - mean) / sd, 2)
        d = {"name": name, "ok": True, "date": latest_d,
             "level": round(latest, 2 if latest < 100 else 0),
             "spark": [[p[0][:7], round(p[1], 2)] for p in pts[-13:]],
             "yoy_pct": round(yoy, 1) if yoy is not None else None,
             "m6_ann_pct": round(m6, 1) if m6 is not None else None,
             "z_5y": z}
        d["inflection"] = (yoy is not None and m6 is not None
                           and ((yoy < 0 < m6) or (yoy > 0 > m6)))
        out["series"][key] = d
        if yoy is not None and m6 is not None:
            scores.append(max(-100, min(100, yoy * 4 + m6 * 3 + z * 10)))
    if scores:
        comp = round(sum(scores) / len(scores), 1)
        out["composite"] = comp
        out["verdict"] = ("ACCELERATING" if comp >= 15 else
                          "DECELERATING" if comp <= -15 else "STABLE")
        out["inflections"] = [k for k, v in out["series"].items()
                              if v.get("inflection")]
        out["n_live"] = len(scores)
        out["ok"] = len(scores) >= 4
    # ── wo4580 (a): EIA weekly distillate product-supplied — the WEEKLY US
    # leg (diesel demand proxies trucking activity at ~1wk lag vs 2-3mo for
    # the monthly composite). Published as fast_leg, kept OUT of the monthly
    # composite (different frequency; mixing would fake precision).
    pts, err = _eia_weekly("PET.WDIUPUS2.W")
    if err or len(pts) < 60:
        out["fast_leg"] = {"status": "UNAVAILABLE",
                           "err": err or "short series (%d)" % len(pts)}
        out["errors"].append("eia_distillate: %s" % (err or "short"))
    else:
        v = [x for _, x in pts]
        a4 = sum(v[-4:]) / 4.0
        a13 = sum(v[-13:]) / 13.0
        a4_yr = sum(v[-56:-52]) / 4.0 if len(v) >= 56 else None
        yoy_w = (round((a4 / a4_yr - 1) * 100, 1)
                 if a4_yr and a4_yr > 0 else None)
        mom = round((a4 / a13 - 1) * 100, 1) if a13 else None
        out["fast_leg"] = {
            "status": "OK", "series": "EIA WDIUPUS2 (distillate product "
            "supplied, kbbl/d, weekly)",
            "latest_week": pts[-1][0], "avg_4w_kbd": round(a4, 1),
            "yoy_pct_4w": yoy_w, "mom_4w_vs_13w_pct": mom,
            "read": ("ACCELERATING" if (mom or 0) > 1.5 else
                     "DECELERATING" if (mom or 0) < -1.5 else "STABLE"),
            "spark": [[d[:10], round(x, 0)] for d, x in pts[-13:]],
            "note": ("diesel burn is the weekly shadow of trucking activity; "
                     "this leg leads the monthly composite by ~6-10 weeks")}

    # ── wo4580 (b): rate-vs-volume separation. Cass expenditures/shipments
    # = implied freight RATE per shipment. Same tonnage decline means
    # opposite trades depending on the rate's direction.
    rv = {"status": "UNAVAILABLE"}
    try:
        sp_s = dict((out["series"].get("cass_shipments") or {}).get("spark") or [])
        sp_e = dict((out["series"].get("cass_expend") or {}).get("spark") or [])
        common = sorted(set(sp_s) & set(sp_e))
        ratio = [(m, sp_e[m] / sp_s[m]) for m in common if sp_s[m]]
        if len(ratio) >= 13:
            r_now, r_ago = ratio[-1][1], ratio[-13][1]
            rate_yoy = round((r_now / r_ago - 1) * 100, 1) if r_ago else None
            vol_yoy = (out["series"].get("cass_shipments") or {}).get("yoy_pct")
            read = "MIXED"
            if vol_yoy is not None and rate_yoy is not None:
                if vol_yoy < -1 and rate_yoy < -1:
                    read = "DEMAND_WEAKNESS"       # both falling
                elif vol_yoy < -1 and rate_yoy > 1:
                    read = "CAPACITY_WITHDRAWAL"   # carriers exiting
                elif vol_yoy > 1 and rate_yoy > 1:
                    read = "DEMAND_ACCELERATION"   # tight market
                elif vol_yoy > 1 and rate_yoy < -1:
                    read = "CAPACITY_GLUT"
                else:
                    read = "FLAT"
            rv = {"status": "OK",
                  "implied_rate_latest": round(r_now, 3),
                  "rate_yoy_pct": rate_yoy, "volume_yoy_pct": vol_yoy,
                  "read": read,
                  "spark": [[m, round(x, 3)] for m, x in ratio[-13:]],
                  "method": ("Cass expenditures / Cass shipments = implied "
                             "rate per shipment. vol↓+rate↓ = demand "
                             "weakness; vol↓+rate↑ = capacity withdrawal — "
                             "opposite trucker trades for the same tonnage "
                             "print")}
    except Exception as e:
        rv = {"status": "ERROR", "err": str(e)[:90]}
    out["rate_vs_volume"] = rv

    # ── wo4580 (c): composite archive → the substrate for a MEASURED
    # freight-leads-port answer. Accrues from today; nothing backfilled.
    prev_comp = None
    try:
        prev = json.loads(S3.get_object(
            Bucket=BUCKET, Key=KEY)["Body"].read())
        prev_comp = prev.get("composite")
    except Exception:
        pass
    n_arch = 0
    try:
        today_s = now.date().isoformat()
        S3.put_object(Bucket=BUCKET,
                      Key="data/archive/freight-pulse/%s.json" % today_s,
                      Body=json.dumps({
                          "date": today_s, "composite": out.get("composite"),
                          "verdict": out.get("verdict"),
                          "fast_leg_mom": (out.get("fast_leg") or {}).get(
                              "mom_4w_vs_13w_pct"),
                          "rate_read": rv.get("read")}).encode(),
                      ContentType="application/json")
        resp = S3.list_objects_v2(Bucket=BUCKET,
                                  Prefix="data/archive/freight-pulse/",
                                  MaxKeys=500)
        n_arch = resp.get("KeyCount", 0)
    except Exception as e:
        out["errors"].append("archive: %s" % str(e)[:80])
    out["lead_vs_port"] = {
        "status": "PENDING_HISTORY", "n_archived": n_arch,
        "note": ("the freight→port lead is a MEASURED cross-correlation once "
                 ">=120 joint archive days exist (freight-pulse + port-cargo "
                 "both archive daily from wo4580); asserting a lead before "
                 "the sample exists would be fabrication")}

    # ── wo4580 (d): impact_map — structural rails/truckers/logistics with
    # honest direction; beta-estimated sectors once factor history earns n.
    sign = 1 if (out.get("composite") or 0) >= 0 else -1
    vb = out.get("verdict") or "UNKNOWN"
    struct = [
        structural_row("Railroads", "industry",
                       "rail carloads + intermodal legs of the composite "
                       "(verdict %s)" % vb, sign),
        structural_row("Trucking", "industry",
                       "truck tonnage + Cass legs; rate_vs_volume read %s "
                       "disambiguates the trade" % rv.get("read"), sign),
        structural_row("Integrated Freight & Logistics", "industry",
                       "Cass expenditures = the pricing line of the whole "
                       "chain", sign),
    ]
    shock = 0.0
    if prev_comp not in (None, 0) and out.get("composite") is not None:
        shock = (out["composite"] - prev_comp) / abs(prev_comp)
    est_rows, est_missing = beta_impact(
        ["Industrials", "Consumer Cyclical", "Energy"],
        "freight_composite_z", shock, kind="industry")
    out["impact_map"] = impact_build(
        "freight-pulse", "freight_composite_z",
        [r for r in struct if r["direction"] == "benefit"]
        + [r for r in est_rows if r["pp"] > 0],
        [r for r in struct if r["direction"] == "suffer"]
        + [r for r in est_rows if r["pp"] <= 0],
        "Structural rows: the industries this composite is literally built "
        "from, signed by the verdict (no pp asserted without measurement). "
        "Estimated sector rows appear once nightly factor history reaches "
        "n_obs>=8 for freight_composite_z.",
        insufficient_rows=est_missing[:6],
        basis_note="composite %s (%s); rate read %s; fast leg %s"
                   % (out.get("composite"), vb, rv.get("read"),
                      (out.get("fast_leg") or {}).get("read",
                                                      "UNAVAILABLE")))

    out["method"] = ("per series: yoy, 6m annualized slope, z vs 5y; "
                     "composite = mean(yoy*4 + m6*3 + z*10) clamped ±100; "
                     "inflection = slope sign diverges from yoy (turning point)")
    S3.put_object(Bucket=BUCKET, Key=KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=3600")
    print(f"[freight] live={out.get('n_live')} comp={out.get('composite')} "
          f"verdict={out.get('verdict')} infl={out.get('inflections')} "
          f"errs={out['errors']}")
    return {"ok": out["ok"], "composite": out.get("composite"),
            "verdict": out.get("verdict")}


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2)[:1200])
