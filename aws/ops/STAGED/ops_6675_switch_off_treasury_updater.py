"""ops 6675 - switch off treasury-auto-updater (Khalid, 2026-10-26: "fix these ... treasury-auto-updater ... erroring on
their own schedules").

Finding (ops 6672 / 6674): the function's only job is `lambda.invoke(treasury-api, RequestResponse)`; treasury-api has
been held at reserved concurrency 0 since the 2026-10-20 retire plan (switch_off, hold=true: a broken writer), so the
invoke is throttled, botocore retries, and the updater hits its 3 s timeout - 3 errors at every daily-morn tick (11:00
UTC).  The updater is the orphaned invoker of a function Khalid switched off; it is switched off the same way:
removed from the daily-morn fan-out tick in the LIVE fan-out manifest (config/fanout-manifest.json in the live bucket,
read by justhodl-scheduler), full previous text backed up to the private bucket first.  The repo copy, the retire plan
and config/retired-engines.json carry the same change in the commit that ships this script
(tests/deployment/test_retired_engines.py keeps it off).  Nothing is deleted; re-enabling is putting the name back in
ticks.daily-morn together with releasing treasury-api's hold.  No key involved.
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
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
FN, TICK, KEY = "treasury-auto-updater", "daily-morn", "config/fanout-manifest.json"


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6675_switch_off_treasury_updater") as r:
        r.heading("ops 6675 - switch off %s (leave the %s fan-out tick)" % (FN, TICK))
        try:
            rc = lam.get_function_concurrency(FunctionName="treasury-api").get("ReservedConcurrentExecutions")
        except Exception as e:  # noqa: BLE001
            rc = "error %s" % type(e).__name__
        r.log("treasury-api reserved concurrency (the 2026-10-20 operator hold): %s" % rc)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        fan = json.loads(old_text)
        before = [t for t, fs in fan.get("ticks", {}).items() if FN in fs]
        r.log("live manifest (generated_at %s): %s in ticks %s" % (fan.get("generated_at"), FN, before))
        if not before and FN not in fan.get("assignment", {}):
            r.ok("already off the live manifest - nothing to change")
        else:
            removed = cp.apply_fanout(fan, {TICK: [FN]})
            fan.setdefault("retired_2026_10_26", {"why": "Khalid, 2026-10-26: fix the fleet-health failures - treasury-auto-updater only invoked "
                                                          "treasury-api (held at concurrency 0 since 2026-10-20) and timed out at 3 s every day",
                                                   "removed": [FN]})
            stamp = ca.stamp()
            store = ca.Store(s3, PRIVATE, "backups/retire-2026-10-26/%s" % stamp)
            store.put("manifest/fanout-manifest.json", {"text": old_text})
            new_text = json.dumps(fan, indent=1, ensure_ascii=False) + "\n"
            s3.put_object(Bucket=LIVE, Key=KEY, Body=new_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            back = json.loads(s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8"))
            still = [t for t, fs in back.get("ticks", {}).items() if FN in fs]
            if still or FN in back.get("assignment", {}):
                r.fail("read-back still lists %s (%s)" % (FN, still))
                raise SystemExit(1)
            r.ok("%s left %s on the live manifest (backup s3://%s/backups/retire-2026-10-26/%s/manifest/fanout-manifest.json)" % (FN, removed.get(FN), PRIVATE, stamp))
        # drift check: the repo copy's ticks vs live (report only)
        repo = json.loads((ROOT / KEY).read_text(encoding="utf-8"))
        live = json.loads(s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8"))
        diffs = []
        for tick in sorted(set(repo["ticks"]) | set(live["ticks"])):
            a, b = set(repo["ticks"].get(tick, [])), set(live["ticks"].get(tick, []))
            if a != b:
                diffs.append("%s: repo-only %s live-only %s" % (tick, sorted(a - b), sorted(b - a)))
        if diffs:
            r.warn("repo vs live tick drift (not touched here): %s" % "; ".join(diffs)[:1500])
        else:
            r.log("repo and live tick memberships agree")
        r.ok("ops 6675 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
