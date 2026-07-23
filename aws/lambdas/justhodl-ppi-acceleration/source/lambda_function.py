"""justhodl-ppi-acceleration v1.0 — CANARY #13.

An aggregate PPI print tells you nothing useful: it averages a heating input
against a cooling one and reports calm. The signal lives in NARROW lines —
one 6-digit industry whose price is not just rising but rising FASTER — because
that is a bottleneck forming in a specific supply chain, and it shows up months
before it reaches a company's cost line or an earnings call.

AUDIT (ops 3758/3759 — why this is not a duplicate):
  · supply-inflection-scanner pins 4 HAND-PICKED series (PCU33443344 semis,
    WPU101 steel, PCU334112334112 storage, WPU117409).
  · bottleneck-boom pins WPU101.
  Nobody SWEEPS the tree. A narrow input can heat for months and no engine
  sees it, because no engine is looking anywhere except at the 5 lines
  somebody chose in advance. This engine ranks the whole discovered universe
  by acceleration so the mover surfaces on its own.

SOURCE (probed, not assumed): 198 lines in config/ppi-lines.json, discovered
via FRED search (BLS bulk .series files are 403), all monthly, all fresh
within 120 days, aggregate parents dropped wherever a narrower child exists.
FRED and the BLS v2 batch return identical values (PCU334413334413 = 29.695
both ways, ops 3758), so FRED is used for the pull.

MATH — the 2nd derivative is the point
  yoy       12m % change (seasonality-neutral)
  prior_yoy the same 12m change measured a quarter ago
  accel_pp  yoy - prior_yoy  ->  is the rise SPEEDING UP or just high?
  m3_ann    last 3m annualised — the fast read that turns first
  z         vs the line's own 5y history of yoy, so a structurally volatile
            line is not mistaken for a moving one

LADDER (a level alone is never enough)
  ACCELERATING_CONFIRMED  yoy>0, accel>=THRESH, m3_ann>yoy  (all three agree)
  ACCELERATING_RAW        yoy>0 and accel>=THRESH
  INFLECTING_UP           accel>=THRESH but yoy still <=0 (turning off a base)
  DECELERATING / COOLING  the mirror cases — a cost tailwind, equally tradeable
GUARD: z requires >=36 monthly observations; below that the line reports its
level honestly and is excluded from the ranked board rather than being scored
against a base rate that does not exist yet.
"""
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3
import urllib.request

VERSION = "1.0.0"
BUCKET = "justhodl-dashboard-live"
LINES_KEY = "config/ppi-lines.json"
OUT_KEY = "data/ppi-acceleration.json"
S3 = boto3.client("s3", region_name="us-east-1")
FRED_KEY = "2f057499936072679d8843d7fce99989"
UA = {"User-Agent": "JustHodl ppi-acceleration"}

ACCEL_THRESH_PP = 2.0     # percentage points of 2nd derivative
MIN_OBS_FOR_Z = 36
WORKERS = 8


def _obs(series_id):
    u = ("https://api.stlouisfed.org/fred/series/observations?series_id="
         + series_id + "&api_key=" + FRED_KEY
         + "&file_type=json&sort_order=desc&limit=80")
    try:
        j = json.loads(urllib.request.urlopen(
            urllib.request.Request(u, headers=UA), timeout=25).read())
        out = []
        for o in (j.get("observations") or []):
            v = o.get("value")
            if v in (".", None, ""):
                continue
            try:
                out.append((o.get("date"), float(v)))
            except ValueError:
                continue
        return out            # newest first
    except Exception as e:
        return {"_err": str(e)[:90]}


def _pct(a, b):
    if b in (None, 0):
        return None
    return round(100 * (a / b - 1), 2)


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    degraded = []

    try:
        cfg = json.loads(S3.get_object(Bucket=BUCKET,
                                       Key=LINES_KEY)["Body"].read())
        lines = cfg.get("lines") or []
    except Exception as e:
        return {"statusCode": 500,
                "body": json.dumps({"error": "line universe missing: %s"
                                    % str(e)[:120]})}

    rows, errs = [], 0
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(_obs, m["id"]): m for m in lines}
        for f in as_completed(futs):
            m = futs[f]
            try:
                o = f.result()
            except Exception:
                errs += 1
                continue
            if isinstance(o, dict) or len(o) < 15:
                errs += 1
                continue

            latest_d, latest_v = o[0]
            yoy = _pct(latest_v, o[12][1]) if len(o) > 12 else None
            prior_yoy = (_pct(o[3][1], o[15][1]) if len(o) > 15 else None)
            accel = (round(yoy - prior_yoy, 2)
                     if (yoy is not None and prior_yoy is not None) else None)
            m3 = (_pct(latest_v, o[3][1]) if len(o) > 3 else None)
            m3_ann = round(m3 * 4, 2) if m3 is not None else None

            # z of the CURRENT yoy vs this line's own yoy history
            z = None
            if len(o) >= MIN_OBS_FOR_Z + 12:
                hist = []
                for i in range(0, len(o) - 12):
                    h = _pct(o[i][1], o[i + 12][1])
                    if h is not None:
                        hist.append(h)
                if len(hist) >= MIN_OBS_FOR_Z and yoy is not None:
                    mu = sum(hist) / len(hist)
                    sd = (sum((x - mu) ** 2 for x in hist) / len(hist)) ** 0.5
                    if sd > 0:
                        z = round((yoy - mu) / sd, 2)

            sig = "FLAT"
            if accel is not None:
                if accel >= ACCEL_THRESH_PP and (yoy or 0) > 0:
                    sig = ("ACCELERATING_CONFIRMED"
                           if (m3_ann is not None and yoy is not None
                               and m3_ann > yoy)
                           else "ACCELERATING_RAW")
                elif accel >= ACCEL_THRESH_PP:
                    sig = "INFLECTING_UP"
                elif accel <= -ACCEL_THRESH_PP and (yoy or 0) < 0:
                    sig = ("DECELERATING_CONFIRMED"
                           if (m3_ann is not None and yoy is not None
                               and m3_ann < yoy)
                           else "DECELERATING_RAW")
                elif accel <= -ACCEL_THRESH_PP:
                    sig = "COOLING"

            rows.append({
                "series_id": m["id"],
                "title": (m.get("title") or "").replace(
                    "Producer Price Index by Industry: ", ""),
                "period": latest_d,
                "level": round(latest_v, 3),
                "yoy_pct": yoy,
                "prior_yoy_pct": prior_yoy,
                "accel_pp": accel,
                "m3_ann_pct": m3_ann,
                "z_vs_own_history": z,
                "n_obs": len(o),
                "base_rate_ready": bool(z is not None),
                "signal": sig,
            })

    if errs:
        degraded.append("%d of %d lines failed to fetch" % (errs, len(lines)))

    order = {"ACCELERATING_CONFIRMED": 0, "ACCELERATING_RAW": 1,
             "INFLECTING_UP": 2, "FLAT": 3, "COOLING": 4,
             "DECELERATING_RAW": 5, "DECELERATING_CONFIRMED": 6}
    rows.sort(key=lambda r: (order.get(r["signal"], 9),
                             -(r["accel_pp"] or -999)))

    accelerating = [r for r in rows if r["signal"].startswith("ACCELERATING")]
    decelerating = [r for r in rows if r["signal"].startswith("DECELERATING")]

    out = {
        "version": VERSION,
        "generated_at": now.isoformat(),
        "n_lines": len(rows),
        "n_accelerating": len(accelerating),
        "n_decelerating": len(decelerating),
        "n_base_rate_ready": sum(1 for r in rows if r["base_rate_ready"]),
        "top_accelerating": accelerating[:15],
        "top_decelerating": decelerating[:10],
        "lines": rows,
        "degraded": degraded,
        "thresholds": {"accel_pp": ACCEL_THRESH_PP,
                       "min_obs_for_z": MIN_OBS_FOR_Z},
        "method": ("Ranks 198 DISCOVERED narrow PPI lines by the 2nd "
                   "derivative of price — accel_pp = yoy minus the yoy "
                   "measured a quarter ago — because a high level is old news "
                   "while an ACCELERATING narrow line is a bottleneck forming. "
                   "CONFIRMED requires level, acceleration and the 3m "
                   "annualised read to agree. z is computed against each "
                   "line's OWN yoy history so a structurally volatile series "
                   "is not mistaken for a moving one, and lines with fewer "
                   "than 36 observations are reported but excluded from "
                   "base-rate scoring rather than scored against a history "
                   "that does not exist."),
        "attribution": "BLS Producer Price Index via FRED; line universe "
                       "discovered ops 3759 (config/ppi-lines.json)",
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":"), default=str),
                  ContentType="application/json")
    print("[ppi-accel] lines=%d accel=%d decel=%d errs=%d"
          % (len(rows), len(accelerating), len(decelerating), errs))
    return {"statusCode": 200,
            "body": json.dumps({"lines": len(rows),
                                "accelerating": len(accelerating)})}
