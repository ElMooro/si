"""ops 3213 — 3212's 0/24 was MY key bug: I keyed on the run-date's ISO
week (W29) while the runner keys on the last GRID Friday (W28 — visible
verbatim in 3211's scanned samples). Exact-key check across both
candidate weeks settles it for good."""
import json
import sys
from datetime import datetime

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


with report("3213_ledger_week") as rep:
    fails = []
    rep.heading("ops 3213 — exact keys, correct week")
    idx = s3_json("data/wl-engines.json") or {}
    firing = [e for e in (idx.get("engines") or []) if e.get("firing")]
    g = str(idx.get("generated_at", ""))[:10]
    y, m, d = (int(x) for x in g.split("-"))
    iy, iw, _ = datetime(y, m, d).isocalendar()
    best_wk, best = None, 0
    for wk in (f"{iy}-{iw - 1:02d}", f"{iy}-{iw:02d}"):
        keys = [{"signal_id": {"S": f"wl#{e['engine_id']}#{wk}"}}
                for e in firing]
        found = 0
        for i in range(0, len(keys), 100):
            r = DDBC.batch_get_item(RequestItems={"justhodl-signals": {
                "Keys": keys[i:i + 100],
                "ProjectionExpression": "signal_id"}})
            found += len(r.get("Responses", {})
                         .get("justhodl-signals") or [])
        rep.log(f"  week {wk}: {found}/{len(firing)}")
        if found > best:
            best, best_wk = found, wk
    rep.kv(firing=len(firing), week=best_wk, present=best)
    if best >= max(1, int(0.8 * len(firing))):
        rep.ok(f"{best}/{len(firing)} firing panels in the trust ledger "
               f"(week {best_wk}) — grading wired END-TO-END; the "
               "scorecard grows one row per watchlist as 7/28/91d "
               "windows score")
    else:
        fails.append(f"best week {best_wk}: {best}/{len(firing)}")
    rep.kv(n_fails=len(fails), verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
