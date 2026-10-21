"""ops 6594 - make the deployed schedule manifest equal the committed one after the repo declared the 21 live triggers
the reconciler reported as UNDECLARED (ops 6593: 4 "residual cost control" rules + the disabled katlin rule, the
2026-10-09 risk / CDS-desk schedules, the governor's own tick).  Uploads only when every difference between the S3
copy and the repo copy is an ADDED entry whose live definition matches (name, expression, state, target function) -
no entry changes or disappears.  Previous S3 copy backed up to the private bucket.  Nothing live is touched; the
schedule-drift alarm clears at the reconciler's next 07:30 UTC run.
"""
from pathlib import Path
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def main():
    s3, evb, sch = c("s3"), c("events"), c("scheduler")
    with report("ops_6594_manifest_declare_sync") as r:
        r.heading("ops 6594 - deployed schedule manifest := committed (21 declared triggers)")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        old = json.loads(old_text)
        a, b = entries(old), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 copy: %d entries; repo: %d; added %d, removed %d, changed %d" % (len(a), len(b), len(added), len(removed), len(changed)))
        if removed or changed:
            r.fail("not an additions-only difference: removed %s changed %s" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        bad = []
        for kind, name in added:
            e = b[(kind, name)]
            try:
                if kind == "events":
                    live = evb.describe_rule(Name=name)
                    tg = evb.list_targets_by_rule(Rule=name).get("Targets", [])
                    ok = live.get("ScheduleExpression") == e["expr"] and live.get("State") == e["state"] and {fn_of(t["Arn"]) for t in tg} == {fn_of(t["arn"]) for t in e["targets"]}
                else:
                    live = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
                    ok = live.get("ScheduleExpression") == e["expr"] and live.get("State") == e["state"] and fn_of((live.get("Target") or {}).get("Arn")) == fn_of(e["targets"][0]["arn"])
            except Exception as ex:  # noqa: BLE001
                ok = False
                r.warn("%s %s: %s" % (kind, name, type(ex).__name__))
            if not ok:
                bad.append((kind, name))
        if bad:
            r.fail("declared entries that do not match live: %s" % bad)
            raise SystemExit(1)
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
        s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
        back = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        if back != repo_text:
            r.fail("read-back differs")
            raise SystemExit(1)
        r.kv(added=len(added), entries=len(b), backup="s3://%s/backups/schedule-manifest/%s/" % (PRIVATE, stamp))
        r.ok("S3 manifest equals the repo (%d entries, %d newly declared); the reconciler's 07:30 UTC run clears the drift alarm" % (len(b), len(added)))


if __name__ == "__main__":
    main()
