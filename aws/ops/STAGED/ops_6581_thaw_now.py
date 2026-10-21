"""ops 6581 - thaw the fleet now, under the full guardrails (Khalid, 2026-10-20: "do what you think is best").

Why now rather than Nov 1: every brake is live and proven (cadence floor, $50/month per-engine cap, runaway / fast /
diffuse fuses, anomaly sentinel, storage fuse, per-service spend lines, sealed IAM denies, SageMaker at zero, the
archive), the site has shown two-week-old data since Oct 17, and a ten-day shakedown before the month rolls is worth
far more than the ~$5/day the freeze saves.  The governor's operator restore lifts every BRAKE / FREEZE entry (the
CAP holds - the October loop pair, the retired API handlers - and any FUSE / ANOM holds stay), puts the 102 public-URL
functions back at reserved concurrency 2 (ops 6558), sets stage NORMAL and suppresses the stage machine until the
Nov 1 rollover (October's $2,803 would otherwise re-freeze it on the next tick).  Preconditions: governor >= 1.5.1,
no SageMaker endpoint, the sealed deny policy attached to the runner group.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
ROLLOVER = datetime(2026, 11, 1, 0, 30, tzinfo=timezone.utc)
SAMPLE = ["justhodl-market-tape", "justhodl-fundamental-census", "justhodl-share-flows", "justhodl-backlog", "justhodl-miss-calibrator",
          "justhodl-event-coordinator", "justhodl-insider-cluster-scanner", "justhodl-symdir"]


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def gov(payload):
    lam = c("lambda")
    for attempt in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:400])
            return body
        except lam.exceptions.TooManyRequestsException:
            time.sleep(20)
    raise RuntimeError("governor busy")


def main():
    lam, sm, iam = c("lambda"), c("sagemaker"), c("iam")
    with report("ops_6581_thaw_now") as r:
        r.heading("ops 6581 - thaw now under the full guardrails (suppressed until the Nov 1 rollover)")
        st = gov({"action": "status"})
        r.log("before: version %s, stage %s, held %s, capped %s" % (st.get("version"), st.get("stage"), json.dumps(st.get("held")), st.get("capped")))
        eps = sm.list_endpoints(MaxResults=10).get("Endpoints", [])
        attached = {p["PolicyName"] for p in iam.list_attached_group_policies(GroupName="justhodl-runner-ecr")["AttachedPolicies"]}
        ok = tuple(int(x) for x in str(st.get("version", "0")).split(".")[:3]) >= (1, 5, 1) and not eps and "justhodl-deny-rogue-compute" in attached
        r.log("preconditions: governor %s, endpoints %d, runner sealed %s -> %s" % (st.get("version"), len(eps), "justhodl-deny-rogue-compute" in attached, ok))
        if not ok:
            r.fail("preconditions not met - no thaw")
            raise SystemExit(1)
        if st.get("stage") != "FREEZE":
            r.ok("stage is %s - nothing to thaw" % st.get("stage"))
            return
        hours = max(1.0, (ROLLOVER - datetime.now(timezone.utc)).total_seconds() / 3600.0)
        res = gov({"action": "restore", "suppress_hours": round(hours, 1),
                   "reason": "Khalid 2026-10-20: thaw now under the full guardrails; the stage machine sleeps until the Nov 1 rollover"})
        r.log("restore: %s" % json.dumps(res)[:600])
        time.sleep(10)
        after = gov({"action": "status"})
        r.log("after: stage %s, suppress_until %s, held %s, capped %s" % (after.get("stage"), after.get("suppress_until"), json.dumps(after.get("held")), after.get("capped")))
        conc = {}
        for fn in SAMPLE:
            try:
                conc[fn] = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            except Exception as ex:  # noqa: BLE001
                conc[fn] = "err:%s" % type(ex).__name__
        r.log("reserved concurrency after the thaw: %s" % json.dumps(conc))
        bad = [fn for fn in ("justhodl-miss-calibrator", "justhodl-event-coordinator") if conc.get(fn) != 0]
        bad += [fn for fn in ("justhodl-market-tape", "justhodl-fundamental-census", "justhodl-share-flows") if conc.get(fn) == 0]
        r.kv(stage=after.get("stage"), suppress_until=after.get("suppress_until"), restored=json.dumps(res.get("restored")),
             remaining_in_ledger=res.get("remaining_in_ledger"), capped=len(after.get("capped") or []))
        if bad or after.get("stage") != "NORMAL":
            r.fail("thaw not as designed: %s stage %s" % (bad, after.get("stage")))
            raise SystemExit(1)
        r.ok("fleet thawed; CAP holds kept; brakes live")


if __name__ == "__main__":
    main()
