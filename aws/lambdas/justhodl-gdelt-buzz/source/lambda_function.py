"""
justhodl-gdelt-buzz — WHICH AI NARRATIVES ARE HEATING UP (free, no key)
=======================================================================
GDELT indexes global news in near-real-time. This tracks the news-VOLUME momentum of
the AI-buildout sub-narratives — when coverage of "HBM memory" or "datacenter power"
accelerates, capital and price tend to follow the story. Surfaces which bottleneck
themes are heating (and which are cooling, incl. the "AI bubble" contra-narrative).

Uses GDELT DOC 2.0 timelinevol (volume intensity = % of global coverage). Throttled to
1 request / 5s per GDELT's free limit.

OUTPUT data/gdelt-buzz.json   SCHEDULE daily 12:45 UTC. Real data, research only.
"""
import json
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/gdelt-buzz.json"
THROTTLE = 5.2
s3 = boto3.client("s3", region_name="us-east-1")

# AI-buildout sub-narratives mapped to the stack layers + a contra-signal
THEMES = [
    ("AI datacenter", "datacenter_buildout"),
    ("HBM memory chip", "memory"),
    ("datacenter power grid", "power_grid"),
    ("small modular reactor nuclear", "power_grid"),
    ("liquid cooling datacenter", "cooling"),
    ("optical transceiver AI", "optical"),
    ("bitcoin miner AI datacenter", "miners_to_ai"),
    ("neocloud GPU cloud", "neocloud"),
    ("AI capex spending", "_macro"),
    ("AI bubble", "_contra"),
]


def _get(url):
    try:
        return urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "jh-gdelt"}), timeout=25).read().decode("utf-8", "ignore")
    except Exception:
        return None


def timelinevol(query):
    url = ("https://api.gdeltproject.org/api/v2/doc/doc?query="
           + urllib.parse.quote('"%s"' % query) + "&mode=timelinevol&timespan=21d&format=json")
    raw = _get(url)
    time.sleep(THROTTLE)
    if not raw:
        return None
    try:
        tl = json.loads(raw).get("timeline", [])
        if tl and tl[0].get("data"):
            return [d.get("value", 0.0) for d in tl[0]["data"]]
    except Exception:
        pass
    return None


def lambda_handler(event, context):
    t0 = time.time()
    out_themes = []
    for theme, layer in THEMES:
        vals = timelinevol(theme)
        if not vals or len(vals) < 7:
            out_themes.append({"theme": theme, "layer": layer, "status": "no_data"})
            continue
        recent = sum(vals[-3:]) / 3.0
        baseline = sum(vals[:-3]) / max(len(vals) - 3, 1)
        accel = round((recent / baseline - 1) * 100, 1) if baseline > 0 else None
        status = ("heating" if (accel or 0) >= 25 else "cooling" if (accel or 0) <= -25 else "steady")
        out_themes.append({"theme": theme, "layer": layer,
                           "recent_vol": round(recent, 4), "baseline_vol": round(baseline, 4),
                           "accel_pct": accel, "status": status})
        if time.time() - t0 > 100:
            break

    rated = [t for t in out_themes if t.get("accel_pct") is not None]
    rated.sort(key=lambda t: t["accel_pct"], reverse=True)
    heating = [t for t in rated if t["status"] == "heating" and t["layer"] not in ("_contra",)]
    out = {
        "engine": "gdelt-buzz", "version": VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "News-volume momentum of AI-buildout narratives — heating coverage tends to precede "
                  "capital and price into the corresponding bottleneck layer.",
        "themes": rated, "heating": heating,
        "bubble_narrative": next((t for t in out_themes if t["layer"] == "_contra"), None),
        "source": "GDELT DOC 2.0 timelinevol (free)",
        "caveats": "News volume is attention, not fundamentals — it can mark tops as well as starts, and the "
                   "'AI bubble' narrative heating is a caution flag, not a buy. Coverage proxy, research only.",
        "elapsed_s": round(time.time() - t0, 1),
    }
    s3.put_object(Bucket=S3_BUCKET, Key=OUT_KEY, Body=json.dumps(out).encode(),
                  ContentType="application/json")
    print(f"[gdelt] themes={len(rated)} heating={len(heating)} {out['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "themes": len(rated),
            "heating": len(heating)})}
