"""ops 6552 - read-only snapshot of every scheduled EventBridge rule and Scheduler schedule (Khalid, 2026-10-20).

Why: "every engine runs at most once every 24 hours".  The plan (scripts/cadence_plan.py) is made from this snapshot, so
it reflects the account as it is right now, and the apply step (ops 6553/6554) can refuse to touch anything that has
moved since.  Payloads (target Input) are reduced to hashes before anything is written: an Input can hold a secret and
this repository is public.

READ-ONLY.  Reads rules, targets and schedules on every event bus / schedule group.  Writes one gzip blob to the private
bucket and a gzip+base64 copy next to the report.  Cost: a few hundred API reads, well under $0.01.
"""
from pathlib import Path
import base64
import gzip
import json
import os
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import cadence_apply as ca  # noqa: E402

REGION = "us-east-1"
PRIVATE = "justhodl-ops-private-857687956942"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=30)


def main():
    with report("ops_6552_cadence_snapshot") as r:
        r.heading("ops 6552 - live trigger snapshot for the once-a-day plan (read-only)")
        events = boto3.client("events", region_name=REGION, config=CFG)
        scheduler = boto3.client("scheduler", region_name=REGION, config=CFG)
        snap = ca.snapshot_triggers(events, scheduler, log=r.log)
        floor = json.loads((ROOT / "config" / "cadence-floor.json").read_text(encoding="utf-8"))
        bad = ca.violations(snap, floor.get("exempt_triggers", []))
        errors = [t for t in snap if t.get("err")]
        kinds = {}
        for t in snap:
            k = (t["kind"], t.get("state"))
            kinds[k] = kinds.get(k, 0) + 1
        r.log("records: %d  %s" % (len(snap), {"%s/%s" % k: v for k, v in sorted(kinds.items(), key=str)}))
        r.log("enabled triggers that can fire more than once on a day (exempt excluded): %d" % len(bad))
        no_lambda = sorted({t["name"] for t in snap if t.get("state") == "ENABLED" and not t.get("fn")})
        r.log("enabled triggers without a Lambda target: %d (%s)" % (len(no_lambda), ", ".join(no_lambda[:12])))
        if errors:
            r.warn("%d schedules could not be read: %s" % (len(errors), [(t["name"], t["err"]) for t in errors[:10]]))
        blob = gzip.compress(json.dumps({"generated_at": ca.stamp(), "triggers": snap}, separators=(",", ":")).encode(), 9)
        boto3.client("s3", region_name=REGION).put_object(Bucket=PRIVATE, Key="audit/cadence-snapshot-%s.json.gz" % ca.stamp(),
                                                          Body=blob, ServerSideEncryption="AES256")
        out = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws" / "ops" / "reports" / "latest" / "ops_6552_cadence_snapshot_data.md"
        out.parent.mkdir(parents=True, exist_ok=True)
        b64 = base64.b64encode(blob).decode()
        out.write_text("# ops 6552 trigger snapshot (gzip+base64 JSON, payloads hashed)\n\n```\n" +
                       "\n".join(b64[i:i + 120] for i in range(0, len(b64), 120)) + "\n```\n", encoding="utf-8")
        r.log("snapshot: %d KB compressed -> %s" % (len(blob) // 1024, out.name))
        if len(snap) < 400 or errors and len(errors) > 20:
            r.fail("snapshot looks incomplete (%d records, %d errors)" % (len(snap), len(errors)))
            raise SystemExit(1)
        r.ok("snapshot complete")


if __name__ == "__main__":
    main()
