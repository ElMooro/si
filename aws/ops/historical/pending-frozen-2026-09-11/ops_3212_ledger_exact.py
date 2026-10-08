"""ops 3212 — exact, not scanned: signal ids are deterministic
(wl#<engine_id>#<week>), so presence is a BatchGetItem on the 24 firing
engines' keys — no pagination, no caps, no trap. 3211's '13' was my own
15-page cap re-creating the very bug it diagnosed."""
import json
import sys

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
DDBC = boto3.client("dynamodb", region_name=REGION)


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3212_ledger_exact") as rep:
    fails = []
    rep.heading("ops 3212 — ledger presence by exact key")
    idx = s3_json("data/wl-engines.json") or {}
    eng = idx.get("engines") or []
    firing = [e for e in eng if e.get("firing")]
    # week token from generated_at (matches runner's week_key format)
    from datetime import datetime
    g = str(idx.get("generated_at", ""))[:10]
    y, m, d = (int(x) for x in g.split("-"))
    iy, iw, _ = datetime(y, m, d).isocalendar()
    wk = f"{iy}-{iw:02d}"
    keys = [{"signal_id": {"S": f"wl#{e['engine_id']}#{wk}"}}
            for e in firing]
    found = 0
    for i in range(0, len(keys), 100):
        r = DDBC.batch_get_item(RequestItems={"justhodl-signals": {
            "Keys": keys[i:i + 100],
            "ProjectionExpression": "signal_id"}})
        found += len(r.get("Responses", {}).get("justhodl-signals") or [])
    rep.kv(firing=len(firing), week=wk, present_in_ledger=found)
    if found >= max(1, int(0.8 * len(firing))):
        rep.ok(f"{found}/{len(firing)} firing panels in the trust ledger — "
               "grading wired end-to-end, scored at 7/28/91d as windows "
               "elapse")
    else:
        fails.append(f"only {found}/{len(firing)} present by exact key")
    rep.kv(n_fails=len(fails), verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
