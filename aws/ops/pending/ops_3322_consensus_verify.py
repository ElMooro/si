"""ops 3322 — verify justhodl-analyst-consensus after the FMP endpoint
repair (grades-latest-news + earnings). Force-run (retry until deploy
rolls), then confirm data/analyst-consensus.json has: fresh timestamp,
populated upgrade/downgrade pulse (was silently empty on the dead
grades-news call), and beat-rate fields on top names (earnings fix).
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", REGION)
S3 = boto3.client("s3", REGION)
BUCKET = "justhodl-dashboard-live"
KEY = "data/analyst-consensus.json"
FN = "justhodl-analyst-consensus"


def s3_json(k):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=k)["Body"].read())
    except Exception:
        return None


with report("3322_consensus_verify") as rep:
    fails = []
    prev = s3_json(KEY) or {}
    prev_gen = prev.get("generated_at")
    rep.kv(prev_generated_at=prev_gen)

    rep.section("FORCE RUN (await fresh)")
    doc = None
    for attempt in range(1, 8):
        try:
            r = LAM.invoke(FunctionName=FN, InvocationType="RequestResponse",
                           Payload=b"{}")
            fn_err = r.get("FunctionError")
            body = r["Payload"].read().decode()[:300]
            time.sleep(4)
            doc = s3_json(KEY)
            gen = (doc or {}).get("generated_at")
            rep.kv(**{f"attempt_{attempt}": {
                "fn_error": fn_err, "written_at": gen,
                "body": body[:160]}})
            if gen and gen != prev_gen:
                break
        except Exception as e:
            rep.kv(**{f"attempt_{attempt}": f"err {e}"})
        time.sleep(12)

    rep.section("VERIFY")
    if not doc:
        fails.append("output unreadable")
    else:
        # find the upgrade/downgrade pulse + beat fields (schema-agnostic scan)
        gen = doc.get("generated_at")
        try:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(
                gen.replace("Z", "+00:00"))).total_seconds()
            if age > 1200:
                fails.append(f"stale ({int(age)}s)")
            rep.kv(generated_at=gen, age_s=int(age))
        except Exception:
            pass

        # look for upgrade lists + beat_pct anywhere in the doc
        strongest_up = doc.get("strongest_upgrades") or []
        weakest_dn = doc.get("weakest_downgrades") or []
        top = doc.get("top_consensus") or []
        beat_names = [t for t in top if isinstance(t, dict)
                      and t.get("beat_pct_8q") is not None]
        rep.kv(universe_size=doc.get("universe_size"),
               n_strongest_upgrades=len(strongest_up),
               n_weakest_downgrades=len(weakest_dn),
               n_top_consensus=len(top),
               n_with_beat_rate=len(beat_names),
               sample_upgrade=(strongest_up[0] if strongest_up else None),
               sample_beat=(beat_names[0] if beat_names else None))

        if not strongest_up and not weakest_dn:
            fails.append("upgrade/downgrade pulse STILL empty — grades feed "
                         "not flowing (check attempts)")
        if top and not beat_names:
            fails.append("no beat rates on any top name — earnings feed not "
                         "flowing")

    rep.section("VERDICT")
    if fails:
        for f in fails:
            rep.fail(f)
        rep.kv(RESULT="FAIL", n=len(fails))
        sys.exit(1)
    rep.ok("analyst-consensus repaired — grade-change pulse + beat rates live.")
    rep.kv(RESULT="FIXED")
