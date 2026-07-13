"""ops 3266 — khalid_note proven on the TRUE ranker feed. The page
fetches data/master-ranker.json; 3260/3264/3265 checked
data/master-rank.json (key typo, mine). The join ran and attached the
whole time (khalid_notes=4 per run log). Pure proof: S3 LastModified
(write-time truth) + field walk + non-null sample. Doctrine: verify
against the key the PAGE fetches, never an assumed name."""
import json
import sys
from datetime import datetime, timezone

import boto3

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
KEY = "data/master-ranker.json"


def walk(o):
    if isinstance(o, dict):
        yield o
        for v in o.values():
            yield from walk(v)
    elif isinstance(o, list):
        for v in o:
            yield from walk(v)


with report("3266_ranker_true_key") as rep:
    fails = []
    rep.heading("ops 3266 — khalid_note on the true key "
                "data/master-ranker.json")
    try:
        h = S3.head_object(Bucket=BUCKET, Key=KEY)
        age_min = (datetime.now(timezone.utc)
                   - h["LastModified"]).total_seconds() / 60
        rep.kv(last_modified=h["LastModified"]
               .strftime("%Y-%m-%d %H:%M UTC"),
               age_min=round(age_min, 1))
    except Exception as e:
        fails.append(f"head: {str(e)[:70]}")
        age_min = 1e9
    d = {}
    if not fails:
        d = json.loads(S3.get_object(Bucket=BUCKET, Key=KEY)
                       ["Body"].read())
        rows = [x for x in walk(d)
                if isinstance(x, dict) and x.get("khalid_note")]
        rep.kv(top_tickers=len(d.get("top_tickers") or []),
               rows_with_khalid_note=len(rows))
        for s in rows[:4]:
            kn = s["khalid_note"]
            rep.log(f"  {str(s.get('ticker')):<6} stance="
                    f"{kn.get('stance')} n={kn.get('n')} "
                    f"last={kn.get('last')}")
        if rows and age_min < 240:
            rep.ok(f"PROVEN on the true key: {len(rows)} ranked "
                   "tickers carry his stance in the live feed — the "
                   "join was working all along; the misses were a key "
                   "typo in the verifier")
        elif rows:
            rep.ok(f"field present ({len(rows)} rows) — feed "
                   f"{round(age_min)}min old; refreshes on cron")
        else:
            fails.append("khalid_note absent on the true key")
    rep.kv(n_fails=len(fails),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
