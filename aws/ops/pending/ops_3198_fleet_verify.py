"""ops 3198 — fleet verification the 3197 poll window missed.

3197 kicked justhodl-wl-engines on the final map but the runner outlasted
the 400s in-ops poll. Doctrine: every deploy is verified, never assumed.
This ops reads data/wl-engines.json, confirms it was regenerated AFTER the
3197 kick (03:15 UTC), and reports the real ACTIVE/DORMANT/FIRING split
and series cache on the widened map. Fails if the index is stale.
"""
import json
import sys
import time
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
KICKED_AT = "2026-07-13T03:15:28"          # 3197 kick timestamp (UTC)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3198_fleet_verify") as rep:
    fails, warns = [], []
    rep.heading("ops 3198 — verify the fleet on the final map")

    idx = s3_json("data/wl-engines.json") or {}
    gen = str(idx.get("generated_at", ""))
    if gen <= KICKED_AT:
        rep.log(f"  index generated_at={gen or 'MISSING'} — re-kicking and "
                "waiting")
        try:
            LAM.invoke(FunctionName="justhodl-wl-engines",
                       InvocationType="Event", Payload=b"{}")
        except Exception as e:
            fails.append(f"invoke: {str(e)[:80]}")
        mark = datetime.now(timezone.utc).isoformat()
        for _ in range(60):
            time.sleep(10)
            idx = s3_json("data/wl-engines.json") or {}
            gen = str(idx.get("generated_at", ""))
            if gen > mark:
                break
    if gen <= KICKED_AT:
        fails.append(f"wl-engines index still stale (generated_at={gen})")
    else:
        eng = idx.get("engines") or []
        active = sum(1 for e in eng if str(e.get("state")) == "ACTIVE")
        dormant = sum(1 for e in eng if str(e.get("state")) == "DORMANT")
        firing = sum(1 for e in eng
                     if any(str(e.get(k)) == "FIRING"
                            for k in ("signal", "fire", "status", "panel")))
        rep.kv(generated_at=gen[:19], engines=len(eng), active=active,
               dormant=dormant, firing=firing,
               series_cached=idx.get("series_cached"))
        samp = [e.get("name") or e.get("engine_id") for e in eng
                if str(e.get("state")) == "ACTIVE"][:4]
        for s in samp:
            rep.log(f"  active e.g. {s}")
        rep.ok("fleet verified on the widened map")
        if active < 100:
            warns.append(f"active {active} < 100 — map widening should "
                         "not have reduced actives")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
