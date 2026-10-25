"""ops 6656 - fmp-walker v1.2.1: the daily price lane gets its own run (schedule justhodl-fmp-walker-prices, 22:40 UTC).

The 23:27 UTC run of 2026-10-25 (walker v1.2.0 at 28 req/s) drew 503 x 429 from FMP in 741 s - 18 req/s is this key's
proven ceiling, and at 18 req/s the ~18k-name daily price lane needs ~1,000 s, more than one 780 s budget.  So: (1) create
the EventBridge Scheduler schedule justhodl-fmp-walker-prices - cron(40 22 * * ? *) UTC, input
{"only":["historical-price-eod-full-daily"]} (the walker's `only` event runs just that dataset with the whole budget;
a second payload is a second job under the cadence floor, as tv-bars' three schedules are), same role and retry policy
as justhodl-fmp-walker-daily, declared in the deployed manifest (additions-only, live-verified); (2) run it once now
(lease-guarded) and read the dataset's cursor/entities/lap and the run's http verdicts back - 429s must be ~0 at 18 req/s.
Keys never printed.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
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
FN = "justhodl-fmp-walker"
SCHED = "justhodl-fmp-walker-prices"
REF = "justhodl-fmp-walker-daily"
EXPR = "cron(40 22 * * ? *)"
STATE = "data/warm/fmp/_state/state.json"
MAN = "data/warm/fmp/manifest.json"
PAYLOAD = {"only": ["historical-price-eod-full-daily"]}
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=600)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def bank(s3, key):
    raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as g:
        return json.loads(g.read())


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    with report("ops_6656_walker_price_lane_schedule") as r:
        r.heading("ops 6656 - fmp-walker price lane: its own daily run (22:40 UTC), declaration, first run at 18 req/s")
        r.section("1. the deployed function carries v1.2.1 (18 req/s)")
        cf = lam.get_function_configuration(FunctionName=FN)
        env = (cf.get("Environment") or {}).get("Variables") or {}
        r.log("%s modified %s, timeout %s, memory %s, FMP_WALKER_RPS %s WORKERS %s" % (FN, (cf.get("LastModified") or "")[:19], cf.get("Timeout"), cf.get("MemorySize"), env.get("FMP_WALKER_RPS"), env.get("FMP_WALKER_WORKERS")))
        if str(env.get("FMP_WALKER_RPS")) != "18":
            r.fail("deployed walker still carries FMP_WALKER_RPS=%s - run again after deploy-lambdas ships v1.2.1" % env.get("FMP_WALKER_RPS"))
            raise SystemExit(1)

        r.section("2. schedule")
        ref = sch.get_schedule(Name=REF, GroupName="default")
        role = ref["Target"]["RoleArn"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule exists: %s %s" % (live.get("ScheduleExpression"), live.get("State")))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=SCHED, GroupName="default", ScheduleExpression=EXPR, ScheduleExpressionTimezone="UTC", State="ENABLED",
                                FlexibleTimeWindow={"Mode": "OFF"}, Description="fmp-walker v1.2.1: the daily price lane's own run (only historical-price-eod-full-daily), 22:40 UTC before the 23:27 full run (ops 6656, 2026-10-25)",
                                Target={"Arn": cf["FunctionArn"], "RoleArn": role, "Input": json.dumps(PAYLOAD, separators=(",", ":")),
                                        "RetryPolicy": ref["Target"].get("RetryPolicy") or {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}})
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.ok("created %s: %s %s target %s role %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), fn_of(live["Target"]["Arn"]), role.split("/")[-1]))
        if live.get("ScheduleExpression") != EXPR or live.get("State") != "ENABLED" or fn_of(live["Target"]["Arn"]) != FN:
            r.fail("schedule does not match the declaration")
            raise SystemExit(1)
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        for kind, name in added:
            e = b[(kind, name)]
            lv = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of(lv["Target"]["Arn"]) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s differs from live" % name)
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest declared %s (backup backups/schedule-manifest/%s/)" % ([n for _, n in added], stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. first price-lane run now (lease-guarded, async; waits for the state write)")
        st = json.loads(s3.get_object(Bucket=LIVE, Key=STATE)["Body"].read())
        if float(st.get("lease_until") or 0) > time.time():
            r.warn("lease held until %s - another walker run is in flight; the 22:40 schedule takes it from here" % datetime.fromtimestamp(st["lease_until"], timezone.utc).strftime("%H:%M:%S"))
        else:
            prev = st.get("updated_at")
            before = (st.get("datasets") or {}).get("historical-price-eod-full-daily") or {}
            resp = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps(PAYLOAD).encode())
            r.log("async invoke %s at %s; lane cursor before: %s" % (resp.get("StatusCode"), datetime.now(timezone.utc).strftime("%H:%M:%S"), json.dumps({k: before.get(k) for k in ("cursor", "lap", "entities_last_run", "universe_size")})))
            t_end = time.time() + 16 * 60
            new = None
            while time.time() < t_end:
                time.sleep(20)
                cur = json.loads(s3.get_object(Bucket=LIVE, Key=STATE)["Body"].read())
                if cur.get("updated_at") != prev and not float(cur.get("lease_until") or 0) > time.time():
                    new = cur
                    break
            if not new:
                r.warn("no state write within 16 min - read CloudWatch /aws/lambda/%s" % FN)
            else:
                run = new.get("last_run") or {}
                lane = (new.get("datasets") or {}).get("historical-price-eod-full-daily") or {}
                r.log("run: calls %s puts %s seconds %s http %s steps %s" % (run.get("calls"), run.get("puts"), run.get("seconds"), json.dumps(run.get("http")), json.dumps(run.get("steps"))[:500]))
                r.log("lane after: cursor %s lap %s entities_last_run %s universe_size %s" % (lane.get("cursor"), lane.get("lap"), lane.get("entities_last_run"), lane.get("universe_size")))
                r.kv(step="price lane dedicated run", calls=run.get("calls"), ok=(run.get("http") or {}).get("ok"), http429=(run.get("http") or {}).get("http429"), entities=lane.get("entities_last_run"), universe=lane.get("universe_size"))
                if (run.get("http") or {}).get("http429", 0) > 50:
                    r.warn("429s still high at 18 req/s - the key's limit is lower than the fleet assumed; lower FMP_WALKER_RPS further")
        r.ok("ops 6656 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
