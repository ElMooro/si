"""ops 6718 - the trigger cap, proven on the real account (Khalid, 2026-11-02: "a max of how many engines get triggered;
past it it gets shut down and I have to manually turn it back on").

After deploy-lambdas shipped commit 6fde07603 (chain_guard v2 in every engine that starts engines, the coordinator,
system_events; spend-governor v1.6.1):
  A. deployed code: chain_guard v2 markers in the function zips of a sample (coordinator, governor, a2a-bus, tv-bars,
     calibrator, symdir, equity-prewarm); the governor answers status with version 1.6.1.
  B. the ledger, for real: a 2-hop chain is admitted and counted in DynamoDB (then a revisit is refused as a loop -
     reported=False so nothing is held); the counters carry a TTL.
  C. the shutdown, end to end, on the canary justhodl-spend-governor-canary (a no-op function kept for exactly this):
     chain_guard.admit with the canary as the source at the depth cap -> breach record written -> the governor's
     trigger_breach holds the canary (reserved concurrency 0, ledger stage CAP, operator_hold, trigger_breach id) ->
     the record is marked applied -> a trigger-breach event record exists -> governor-alerts.yml, dispatched, opens
     the GitHub issue -> {"action":"release_cap"} restores the canary exactly (reserved None) - the manual path.
     Nothing but the canary is touched; the synthetic chain's counters expire with their TTL.
No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import io
import json
import os
import subprocess
import sys
import time
import urllib.request
import zipfile

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import chain_guard as cg  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
GOVERNOR, CANARY = "justhodl-spend-governor", "justhodl-spend-governor-canary"
SAMPLE = ["justhodl-event-coordinator", "justhodl-spend-governor", "justhodl-a2a-bus", "justhodl-tv-bars", "justhodl-calibrator", "justhodl-symdir", "justhodl-equity-prewarm"]
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=120)
ONCE = Config(retries={"max_attempts": 1}, read_timeout=300, connect_timeout=10)


def gov(lam, payload):
    for _ in range(10):
        try:
            resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
            return json.loads(resp["Payload"].read() or b"{}")
        except lam.exceptions.TooManyRequestsException:
            time.sleep(30)
    raise RuntimeError("governor busy")


def zip_text(lam, fn, member):
    url = lam.get_function(FunctionName=fn)["Code"]["Location"]
    with urllib.request.urlopen(url, timeout=60) as resp:
        z = zipfile.ZipFile(io.BytesIO(resp.read()))
    return z.read(member).decode("utf-8", "replace") if member in z.namelist() else ""


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    lam1 = boto3.client("lambda", region_name=REGION, config=ONCE)
    ddb = boto3.client("dynamodb", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "ops-6718-runner"
    with report("ops_6718_trigger_cap_live_proof") as r:
        r.heading("ops 6718 - the trigger cap on the real account: deployed code, the ledger, the shutdown and the manual release")
        r.section("A. deployed code")
        for fn in SAMPLE:
            guard = zip_text(lam, fn, "chain_guard.py")
            handler = zip_text(lam, fn, "lambda_function.py")
            v2 = 'VERSION = "2.0.0"' in guard and "def admit(" in guard
            wired = ("chain_guard" in handler) or fn == GOVERNOR
            (r.ok if v2 and wired else r.fail)("%s: chain_guard v2 in zip %s | source uses it %s | modified %s" % (
                fn, v2, wired, lam.get_function_configuration(FunctionName=fn)["LastModified"][:19]))
        st = gov(lam1, {"action": "status"})
        (r.ok if st.get("version") == "1.6.1" else r.fail)("governor version %s stage %s mode %s" % (st.get("version"), st.get("stage"), st.get("mode")))
        if st.get("version") != "1.6.1":
            raise SystemExit(1)

        r.section("B. the ledger, for real")
        lin = cg.fresh("justhodl-ops6718-root")
        v1 = cg.admit("justhodl-ops6718-root", "justhodl-ops6718-a", lin, "handoff")
        v2 = cg.admit("justhodl-ops6718-a", "justhodl-ops6718-b", v1["lineage"], "handoff")
        r.log("hop 1 %s | hop 2 %s path %s" % (v1["allow"], v2["allow"], v2["lineage"]["path"]))
        v3 = cg.admit("justhodl-ops6718-b", "justhodl-ops6718-root", v2["lineage"], "handoff", report=False)
        (r.ok if v1["allow"] and v2["allow"] and not v3["allow"] and v3["reason"] == "loop" else r.fail)("2 hops admitted, the revisit refused as a loop (not reported): %s" % v3["reason"])
        item = ddb.get_item(TableName=cg.TABLE, Key={"pk": {"S": "chain#%s" % lin["id"]}}).get("Item") or {}
        eng = ddb.get_item(TableName=cg.TABLE, Key={"pk": {"S": "chain#%s#engines" % lin["id"]}}).get("Item") or {}
        ttl_h = (int(item.get("expires_at", {}).get("N", "0")) - time.time()) / 3600 if item else None
        (r.ok if item.get("n", {}).get("N") == "2" and sorted(eng.get("engines", {}).get("SS", [])) == ["justhodl-ops6718-a", "justhodl-ops6718-b"] else r.fail)(
            "DynamoDB: chain counter n=%s, engines %s, expires in %.0f h" % (item.get("n", {}).get("N"), eng.get("engines", {}).get("SS"), ttl_h or 0))

        r.section("C. the shutdown on the canary, end to end")
        before = lam.get_function_concurrency(FunctionName=CANARY).get("ReservedConcurrentExecutions")
        r.log("canary reserved concurrency before: %s" % before)
        if before == 0:
            r.fail("the canary is already held - leaving everything alone")
            raise SystemExit(1)
        deep = {"id": "ops6718-%d" % int(time.time()), "root": "justhodl-ops6718-root", "hop": 3,
                "path": ["justhodl-ops6718-root", "justhodl-ops6718-a", "justhodl-ops6718-b", CANARY], "self_hop": 0, "n": 3}
        t0 = time.time()
        v = cg.admit(CANARY, "justhodl-ops6718-target", deep, "handoff")
        r.log("admit at the depth cap: allow %s reason %s hold %s breach_id %s (%.1f s incl. the governor call)" % (v["allow"], v["reason"], v["hold"], v.get("breach_id"), time.time() - t0))
        bid = v.get("breach_id")
        rec = ddb.get_item(TableName=cg.TABLE, Key={"pk": {"S": bid}}).get("Item") or {}
        status = rec.get("status", {}).get("S")
        if status != "applied":
            r.log("record status %s - asking the governor directly (the engine-side call may have found it busy)" % status)
            r.log("governor: %s" % json.dumps(gov(lam1, {"action": "trigger_breach", "breach_id": bid}))[:400])
            rec = ddb.get_item(TableName=cg.TABLE, Key={"pk": {"S": bid}}).get("Item") or {}
            status = rec.get("status", {}).get("S")
        held = lam.get_function_concurrency(FunctionName=CANARY).get("ReservedConcurrentExecutions")
        st = gov(lam1, {"action": "status"})
        state = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
        entry = (state.get("ledger") or {}).get("lambda:" + CANARY) or {}
        (r.ok if held == 0 and status == "applied" and entry.get("stage") == "CAP" and entry.get("operator_hold") and entry.get("trigger_breach") == bid else r.fail)(
            "SHUT DOWN: canary reserved %s | record %s | ledger stage %s operator_hold %s trigger_breach %s | capped now %s" % (
                held, status, entry.get("stage"), entry.get("operator_hold"), entry.get("trigger_breach"), st.get("capped")))
        events = [o["Key"] for o in s3.list_objects_v2(Bucket=PRIVATE, Prefix="spend-governor/events/2026110").get("Contents", []) if o["Key"].endswith("-trigger-breach.json")]
        (r.ok if events else r.fail)("trigger-breach event records: %s" % events[-2:])
        # the GitHub issue: run the relay now instead of waiting for its hour
        try:
            subprocess.run(["gh", "api", "-X", "POST", "repos/ElMooro/si/actions/workflows/governor-alerts.yml/dispatches", "-f", "ref=main"], check=True, capture_output=True, timeout=60)
            r.log("governor-alerts.yml dispatched (the issue 'TRIGGER CAP: engines SHUT DOWN (manual release)' follows within minutes)")
        except Exception as e:  # noqa: BLE001
            r.log("governor-alerts dispatch not possible from here (%s) - the hourly run opens the issue" % type(e).__name__)
        # the manual release - the only way back
        rel = gov(lam1, {"action": "release_cap", "function": CANARY, "confirm": CANARY, "reason": "ops 6718: trigger-cap live proof complete"})
        after = lam.get_function_concurrency(FunctionName=CANARY).get("ReservedConcurrentExecutions")
        (r.ok if rel.get("status") == "restored" and after == before else r.fail)("RELEASED by release_cap: %s | canary reserved now %s (was %s)" % (json.dumps(rel), after, before))
        second = cg.admit(CANARY, "justhodl-ops6718-target", deep, "handoff")
        (r.ok if second.get("breach_id") == bid else r.warn)("a second breach the same day for the same rule is not re-reported (record %s)" % second.get("breach_id"))
        r.ok("ops 6718 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
