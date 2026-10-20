"""ops 6564 - the blackswan family runs in sequence (Khalid, 2026-10-20: "merge the blackswan seven").

The seven engines share one warm cache (data/warm/blackswan/, 20 h TTL, cache-first).  Each had its own rate(24 hours)
schedule with an unknown phase, so whichever ran first each day re-downloaded every series and the others sometimes did
too.  Now: blackswan-watch (the root fork) at 02:10 UTC fills the cache; the six run 03:10-04:25, inside the TTL, and
read it.  Schedules are retimed with every live field passed back; backups first; S3 schedule manifest updated.
Code side: aws/shared/blackswan_resolver.py (one copy of the 23 identical resolver functions) deploys separately.
"""
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import cadence_apply as ca  # noqa: E402
import cadence_plan as cp  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE, LIVE = "justhodl-ops-private-%s" % ACCOUNT, "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, max_pool_connections=20)
PLAN = json.loads((ROOT / "config/blackswan-sequence-2026-10-20.json").read_text(encoding="utf-8"))


def main():
    with report("ops_6564_blackswan_sequence") as r:
        r.heading("ops 6564 - blackswan family: root at 02:10 UTC, six readers 03:10-04:25")
        s3 = boto3.client("s3", region_name=REGION, config=CFG)
        events = boto3.client("events", region_name=REGION, config=CFG)
        scheduler = boto3.client("scheduler", region_name=REGION, config=CFG)
        store = ca.Store(s3, PRIVATE, "backups/blackswan-sequence/%s" % ca.stamp())
        results = ca.apply_actions(PLAN["actions"], events, scheduler, store, dry_run=False, workers=2, canary=True, log=r.log)
        r.kv(**ca.summarise(results))
        for res in results:
            if res["status"] not in ("applied", "already"):
                r.warn("%s: %s %s" % (res["name"], res["status"], res.get("live") or res.get("error") or ""))
        final = {(a["kind"], a["name"]): (a["after"]["expr"], a["after"]["state"]) for a in PLAN["actions"]}
        key = "config/schedule-manifest.json"
        old = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
        new, n = cp.apply_schedule_manifest(old, PLAN["actions"], final)
        if new != old:
            store.put("manifest/schedule-manifest.json", {"text": old})
            s3.put_object(Bucket=LIVE, Key=key, Body=new.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
        r.log("S3 schedule manifest: %d entries updated" % n)
        if any(res["status"] in ("failed", "drift", "missing") for res in results):
            r.fail("not every schedule moved")
            raise SystemExit(1)
        r.ok("sequence set")


if __name__ == "__main__":
    main()
