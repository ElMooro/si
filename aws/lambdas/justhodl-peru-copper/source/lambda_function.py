"""justhodl-peru-copper — Peru copper production (Dr. Copper's supply side).

Peru is the world's #2 copper producer; with Chile (#1) it is ~40% of global mined
copper. The platform already tracks Chile exports and the copper price — this closes
the supply-side gap with Peru's actual mined output.

Source: Banco Central de Reserva del Perú (BCRP) BCRPData API — free, no key, clean
JSON (FRED-equivalent). Series RD12951DM = "Cobre - Total (tm.f)" (fine metric tons),
monthly. Falling mined output = supply tightening / mining-activity contraction; the
supply half of the copper balance that price alone can't separate from demand.

OUTPUT  data/peru-copper.json      SCHEDULE  daily 04:00 UTC (BCRP updates monthly)
Real BCRP data — not investment advice.
"""
import json
import re
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/peru-copper.json"
SERIES = "RD12951DM"   # Cobre - Total (tm.f), monthly
API = "https://estadisticas.bcrp.gob.pe/estadisticas/series/api/%s/json/2004-1/2027-12/ing"

_MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
           "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


def _iso(period):
    m = re.match(r"([A-Za-z]{3})\.?(\d{4})", str(period).strip())
    if not m:
        return None
    mo = _MONTHS.get(m.group(1)[:3].title())
    return "%04d-%02d" % (int(m.group(2)), mo) if mo else None


def fetch_bcrp():
    req = urllib.request.Request(API % SERIES, headers={"User-Agent": "Mozilla/5.0 (justhodl-peru)", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=45) as r:
        d = json.loads(r.read().decode("utf-8", "ignore"))
    pairs = []
    for p in d.get("periods", []):
        iso = _iso(p.get("name"))
        vals = p.get("values") or []
        if not iso or not vals:
            continue
        try:
            v = float(vals[0])
        except (TypeError, ValueError):
            continue
        pairs.append((iso, v))
    return sorted(pairs)


def series_stats(pairs):
    if len(pairs) < 14:
        return {"error": "insufficient history", "n": len(pairs)}
    by = dict(pairs)

    def yoy_at(i):
        y, m = pairs[i][0].split("-")
        pv = by.get("%04d-%s" % (int(y) - 1, m))
        return (pairs[i][1] / pv - 1.0) * 100 if pv else None

    yoy = yoy_at(len(pairs) - 1)
    y3 = [yoy_at(i) for i in range(len(pairs) - 3, len(pairs))]
    y3 = [x for x in y3 if x is not None]
    hist_yoy = [x for x in (yoy_at(i) for i in range(max(0, len(pairs) - 60), len(pairs))) if x is not None]
    z = None
    if len(hist_yoy) >= 12 and yoy is not None:
        mu = sum(hist_yoy) / len(hist_yoy)
        sd = (sum((x - mu) ** 2 for x in hist_yoy) / (len(hist_yoy) - 1)) ** 0.5
        z = round((yoy - mu) / sd, 2) if sd > 0 else None
    return {"latest_period": pairs[-1][0], "latest_value": round(pairs[-1][1], 1),
            "yoy_pct": round(yoy, 2) if yoy is not None else None,
            "yoy_3mma_pct": round(sum(y3) / len(y3), 2) if y3 else None,
            "yoy_z_5y": z, "n": len(pairs),
            "history": [{"p": p, "v": round(v, 1)} for p, v in pairs[-60:]]}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"engine": "justhodl-peru-copper", "generated_at": now.isoformat(),
           "source": "Banco Central de Reserva del Perú (BCRP) BCRPData — series RD12951DM Cobre Total (tm.f), free",
           "unit": "fine metric tons (tm.f), monthly",
           "note": "Peru = #2 copper producer; with Chile ~40% of world mined copper — Dr. Copper's supply side. Real BCRP data, not advice."}
    try:
        d = series_stats(fetch_bcrp())
        out["copper_production"] = d
        y = d.get("yoy_3mma_pct")
        if y is not None:
            out["read"] = ("EXPANDING — mined supply rising" if y > 3
                           else "CONTRACTING — mined supply falling (supply tightening / mining-activity weakness)" if y < -3
                           else "FLAT")
        out["headline"] = "Peru copper production %s%% YoY (3mma) — the supply half of the copper balance." % (
            "n/a" if y is None else y)
    except Exception as e:
        out["copper_production"] = {"error": str(e)[:120]}
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, ensure_ascii=False, default=str).encode("utf-8"),
                  ContentType="application/json; charset=utf-8", CacheControl="max-age=3600")
    return {"ok": True, "yoy_3mma": (out.get("copper_production") or {}).get("yoy_3mma_pct")}
