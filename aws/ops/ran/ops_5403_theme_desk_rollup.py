"""ops_5403 -- theme rollups + desk flag diagnosis.

Does NOT walk 9.7M warehouse keys. Uses provider-catalog + delimiter
listings + existing JSON feeds. Writes:
  data/theme-eurostat.json
  data/theme-gdelt.json
  data/desk-rollup.json
so brief/command/data.html can join engines without a new fleet.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"

DESK_FEEDS = [
    "data/provider-catalog.json",
    "data/provider-consumption.json",
    "data/import-health.json",
    "data/fed-nowcast-join.json",
    "data/activity-nowcast.json",
    "data/ciss-stress.json",
    "data/bond-warroom.json",
    "data/khalid.json",
    "data/brief.json",
    "data/decisive-call.json",
]


def _get(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        raw = obj["Body"].read()
        try:
            body = json.loads(raw)
        except Exception:
            body = {"_not_json": True, "bytes": len(raw)}
        return True, body, obj["LastModified"].isoformat(), len(raw)
    except Exception as e:
        return False, {"_error": type(e).__name__}, None, 0


def _prefixes(s3, prefix, limit=40):
    out = []
    token = None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "Delimiter": "/", "MaxKeys": 50}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        for p in resp.get("CommonPrefixes") or []:
            out.append(p.get("Prefix"))
            if len(out) >= limit:
                return out, resp.get("KeyCount")
        for it in resp.get("Contents") or []:
            out.append(it["Key"])
            if len(out) >= limit:
                return out, resp.get("KeyCount")
        token = resp.get("NextContinuationToken")
        if not token:
            return out, resp.get("KeyCount")


def _slim_provider(cat, slug):
    for p in cat.get("providers") or []:
        if p.get("slug") == slug:
            return {
                "slug": slug,
                "name": p.get("name"),
                "datasets": p.get("datasets"),
                "series_count": p.get("series_count"),
                "mb": p.get("total_mb"),
                "freshest_h": p.get("freshest_h"),
                "hot_feeds": p.get("hot_feeds"),
                "note": (p.get("catalog_note") or "")[:400],
            }
    return {"slug": slug, "status": "DATA_HOLD"}


def main():
    with report("ops_5403_theme_desk_rollup") as R:
        R.heading("ops 5403 -- Eurostat/GDELT themes + desk diagnosis")
        s3 = boto3.client("s3", region_name=REGION)
        now = datetime.now(timezone.utc).isoformat()

        feeds = {}
        for key in DESK_FEEDS:
            ok, body, lm, n = _get(s3, key)
            feeds[key] = {"ok": ok, "last_modified": lm, "bytes": n,
                          "error": None if ok else body.get("_error"),
                          "top_keys": list(body.keys())[:20] if isinstance(body, dict) else []}
            R.log("%s ok=%s bytes=%s" % (key, ok, n))

        ok_c, cat, _, _ = _get(s3, "data/provider-catalog.json")
        if not ok_c:
            R.fail("catalog missing")
            sys.exit(1)

        eu = _slim_provider(cat, "eurostat")
        gd = _slim_provider(cat, "gdelt")
        eu_prefs, eu_n = _prefixes(s3, "data/warm/eurostat/")
        gd_prefs, gd_n = _prefixes(s3, "data/warm/gdelt/")
        eu["warm_prefixes"] = eu_prefs
        eu["list_key_count"] = eu_n
        gd["warm_prefixes"] = gd_prefs
        gd["list_key_count"] = gd_n
        eu["generated_at"] = now
        eu["schema_version"] = 1
        eu["rollup"] = "theme -- not a raw 312GB dump"
        gd["generated_at"] = now
        gd["schema_version"] = 1
        gd["rollup"] = "theme -- not a raw 92GB event dump"

        cons_ok, cons, _, _ = _get(s3, "data/provider-consumption.json")
        cancel = (cons or {}).get("cancel_candidates") if cons_ok else []
        orphans = (cons or {}).get("orphans") if cons_ok else []

        ih_ok, ih, _, _ = _get(s3, "data/import-health.json")
        pipes = []
        if ih_ok:
            raw = ih.get("pipelines") or []
            if isinstance(raw, dict):
                raw = [dict(v or {}, name=k) for k, v in raw.items()]
            pipes = [{"name": p.get("name"), "status": p.get("status"),
                      "detail": str(p.get("detail") or p.get("note") or "")[:160]}
                     for p in raw if isinstance(p, dict)]

        red_pipes = [p for p in pipes if str(p.get("status") or "").upper() in
                     ("STALE", "WEDGED", "BLOCKED", "ACTION_REQUIRED", "KEY_INVALID", "STALLED")]

        desk = {
            "generated_at": now,
            "schema_version": 1,
            "source": "ops_5403",
            "how_to_read_command_desk_flags": (
                "home.js counts RED/AMBER across embedded page sections and engine "
                "rows. Those flags are market-state plus freshness, not a single bug. "
                "Stale/dead collector lanes are listed under import_health_alerts."
            ),
            "import_health_overall": (ih or {}).get("overall") if ih_ok else None,
            "import_health_alerts": red_pipes,
            "feeds": feeds,
            "cancel_candidates": cancel,
            "orphans": orphans,
            "confluence_path": [
                "data/activity-nowcast.json",
                "data/fed-nowcast-join.json",
                "data/theme-eurostat.json",
                "data/theme-gdelt.json",
                "data/provider-consumption.json",
                "data/ciss-stress.json",
            ],
        }

        for key, payload in (
            ("data/theme-eurostat.json", eu),
            ("data/theme-gdelt.json", gd),
            ("data/desk-rollup.json", desk),
        ):
            s3.put_object(Bucket=B, Key=key,
                          Body=json.dumps(payload, default=str).encode("utf-8"),
                          ContentType="application/json")
            bok, back, _, _ = _get(s3, key)
            if not bok:
                R.fail("read-back failed %s" % key)
                sys.exit(1)
            R.ok("wrote %s" % key)

        R.section("verdict")
        R.log("eurostat prefixes=%s gdelt prefixes=%s alerts=%s" % (
            len(eu_prefs), len(gd_prefs), len(red_pipes)))
        R.ok("GREEN -- theme rollups + desk-rollup live")


if __name__ == "__main__":
    main()
