"""justhodl-singapore-nodx — Singapore non-oil domestic exports (NODX).

Singapore is a trade + semiconductor testing/assembly hub, so its NODX — and
especially the electronics and integrated-circuits slices — is a clean, very
timely global-trade + chip-cycle canary (published ~2 weeks after month-end).

Source: Singapore Dept. of Statistics (SingStat) Table Builder API, table
M450981 "Domestic Exports Of Major Non-Oil Products, Monthly" — free, no key,
JSON. NODX total = Total Electronic Products + Non-Electronic Products; the
Integrated Circuits row is the pure semiconductor read.

OUTPUT  data/singapore-nodx.json    SCHEDULE  daily 04:20 UTC
Real SingStat data — not investment advice.
"""
import json
import urllib.request
from datetime import datetime, timezone

import boto3

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/singapore-nodx.json"
TABLE = "M450981"
API = "https://tablebuilder.singstat.gov.sg/api/table/tabledata/%s"

_MONTHS = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
           "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}


def _iso(key):
    # "2026 May" -> "2026-05"
    parts = str(key).strip().split()
    if len(parts) != 2:
        return None
    yr, mon = parts
    mo = _MONTHS.get(mon[:3].title())
    return "%s-%02d" % (yr, mo) if (mo and yr.isdigit()) else None


def fetch_table():
    req = urllib.request.Request(API % TABLE, headers={"User-Agent": "Mozilla/5.0 (justhodl-singapore)", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=50) as r:
        d = json.loads(r.read().decode("utf-8", "ignore"))
    rows = (d.get("Data") or {}).get("row") or []
    series = {}
    for rw in rows:
        name = (rw.get("rowText") or "").strip()
        pts = {}
        for c in rw.get("columns") or []:
            iso = _iso(c.get("key"))
            try:
                v = float(c.get("value"))
            except (TypeError, ValueError):
                v = None
            if iso and v is not None:
                pts[iso] = v
        series[name] = pts
    return series


def _stats(pts_dict):
    pairs = sorted(pts_dict.items())
    if len(pairs) < 14:
        return {"error": "insufficient", "n": len(pairs)}
    by = dict(pairs)

    def yoy_at(i):
        y, m = pairs[i][0].split("-")
        pv = by.get("%04d-%s" % (int(y) - 1, m))
        return (pairs[i][1] / pv - 1.0) * 100 if pv else None

    yoy = yoy_at(len(pairs) - 1)
    y3 = [x for x in (yoy_at(i) for i in range(len(pairs) - 3, len(pairs))) if x is not None]
    hist = [x for x in (yoy_at(i) for i in range(max(0, len(pairs) - 60), len(pairs))) if x is not None]
    z = None
    if len(hist) >= 12 and yoy is not None:
        mu = sum(hist) / len(hist)
        sd = (sum((x - mu) ** 2 for x in hist) / (len(hist) - 1)) ** 0.5
        z = round((yoy - mu) / sd, 2) if sd > 0 else None
    return {"latest_period": pairs[-1][0], "latest_value": round(pairs[-1][1], 1),
            "yoy_pct": round(yoy, 2) if yoy is not None else None,
            "yoy_3mma_pct": round(sum(y3) / len(y3), 2) if y3 else None,
            "yoy_z_5y": z, "n": len(pairs),
            "history": [{"p": p, "v": round(v, 1)} for p, v in pairs[-60:]]}


def lambda_handler(event=None, context=None):
    now = datetime.now(timezone.utc)
    out = {"engine": "justhodl-singapore-nodx", "generated_at": now.isoformat(),
           "source": "Singapore SingStat Table Builder M450981 (free)", "unit": "S$ thousand, monthly",
           "note": "Singapore = trade + chip test/assembly hub; NODX electronics & ICs are a timely global tech-cycle canary. Real SingStat data, not advice."}
    try:
        s = fetch_table()
        elec = s.get("Total Electronic Products") or {}
        nonelec = s.get("Non-Electronic Products") or {}
        ic = s.get("Integrated Circuits") or {}
        # NODX total = electronic + non-electronic (aligned by period)
        total = {}
        for p in set(elec) & set(nonelec):
            total[p] = elec[p] + nonelec[p]
        out["nodx_total"] = _stats(total)
        out["electronics"] = _stats(elec)
        out["integrated_circuits"] = _stats(ic)
        yt = out["nodx_total"].get("yoy_3mma_pct")
        ye = out["electronics"].get("yoy_3mma_pct")
        if yt is not None:
            out["nodx_total"]["read"] = ("EXPANDING — global trade demand firm" if yt > 3
                                         else "CONTRACTING — global trade-demand slowdown" if yt < -3 else "FLAT")
        if ye is not None:
            out["electronics"]["read"] = ("UP-LEG — tech/chip export cycle expanding" if ye > 5
                                          else "DOWN-LEG — tech export cycle rolling over" if ye < -3 else "FLAT")
        out["headline"] = "Singapore NODX %s%% YoY, electronics %s%% YoY — a timely global trade + chip-cycle read." % (
            "n/a" if yt is None else yt, "n/a" if ye is None else ye)
    except Exception as e:
        out["error"] = str(e)[:150]
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(out, ensure_ascii=False, default=str).encode("utf-8"),
                  ContentType="application/json; charset=utf-8", CacheControl="max-age=3600")
    return {"ok": "error" not in out, "nodx_yoy": (out.get("nodx_total") or {}).get("yoy_3mma_pct"),
            "electronics_yoy": (out.get("electronics") or {}).get("yoy_3mma_pct")}
