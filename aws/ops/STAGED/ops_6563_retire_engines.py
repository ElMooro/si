"""ops 6563 - retire engines per config/retire-plan-2026-10-20.json (Khalid, 2026-10-20: "switch off the orphans and
the broken writers" (11) and "delete the idle functions" (40; 5 of the 45 are kept because a page calls their URL)).

Switch off (reversible): every enabled rule / schedule of the function is set DISABLED (full-definition backup first,
put_rule / update_schedule pass every live field back); a fan-out tick membership is removed from the S3 fan-out
manifest (the repo copy is committed alongside); API / URL-only functions are held at concurrency 0 through the
governor's operator hold (stage CAP: nothing automatic lifts it, {"action":"release_cap"} does).

Delete (final, backed up): the deployment package (zip) + configuration + URL config + tags of each function are saved
to s3://justhodl-ops-private-.../backups/deleted-functions/<stamp>/<fn>.{zip,json} before delete_function; a rule whose
only target was the deleted function is deleted too (classic-rule quota is saturated); API Gateway integrations that
pointed at a deleted function are listed in the report, not touched.  The repo moves aws/lambdas/<fn> to
aws/lambdas-retired/<fn> in the same change, so no deploy can recreate them.
"""
from pathlib import Path
import concurrent.futures as cf
import json
import sys
import time
import urllib.request

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
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=20)
PLAN = json.loads((ROOT / "config/retire-plan-2026-10-20.json").read_text(encoding="utf-8"))


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def gov(payload):
    lam = c("lambda")
    for attempt in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:300])
            return body
        except lam.exceptions.TooManyRequestsException:
            time.sleep(20)
    raise RuntimeError("governor busy")


def switch_off(r, store, events, scheduler, s3):
    actions = []
    for fn, spec in PLAN["switch_off"].items():
        for t in spec["triggers"]:
            if t["state"] != "ENABLED":
                continue
            actions.append({"kind": t["kind"], "name": t["name"], "bus": t.get("bus"), "group": t.get("group"), "functions": [fn],
                            "op": "disable", "before": {"expr": t["expr"], "state": "ENABLED"},
                            "after": {"expr": t["expr"], "state": "DISABLED"}})
    results = ca.apply_actions(actions, events, scheduler, store, dry_run=False, workers=4, canary=True, log=r.log)
    r.log("triggers disabled: %s" % json.dumps(ca.summarise(results)))
    for res in results:
        if res["status"] not in ("applied", "already"):
            r.warn("%s %s: %s %s" % (res["kind"], res["name"], res["status"], res.get("live") or res.get("error") or ""))
    # fan-out memberships
    removals = {}
    for fn, spec in PLAN["switch_off"].items():
        if spec.get("tick"):
            removals.setdefault(spec["tick"], []).append(fn)
    for fn, spec in PLAN["delete"].items():
        if spec.get("tick"):
            removals.setdefault(spec["tick"], []).append(fn)
    if removals:
        key = "config/fanout-manifest.json"
        old = s3.get_object(Bucket=LIVE, Key=key)["Body"].read().decode("utf-8")
        fan = json.loads(old)
        removed = cp.apply_fanout(fan, removals)
        fan.setdefault("retired_2026_10_20", {"why": PLAN["reason"], "removed": sorted(removed)})
        new = json.dumps(fan, indent=1, ensure_ascii=False) + "\n"
        store.put("manifest/fanout-manifest.json", {"text": old})
        s3.put_object(Bucket=LIVE, Key=key, Body=new.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
        r.ok("fan-out manifest: %s left their tick (%s)" % (len(removed), ", ".join(sorted(removed))))
    # operator holds for the API / URL-only ones
    for fn, spec in PLAN["switch_off"].items():
        if spec.get("hold"):
            res = gov({"action": "hold", "function": fn, "confirm": fn, "reason": "retired 2026-10-20 (ops 6563): %s" % PLAN["reason"][:80]})
            r.log("hold %s: %s" % (fn, res.get("status") or res.get("error")))
    return results


def backup_and_delete(r, store, s3, events, stamp):
    lam = c("lambda")
    done, failed = [], []

    def one(fn):
        try:
            f = lam.get_function(FunctionName=fn)
        except lam.exceptions.ResourceNotFoundException:
            return fn, "already gone", None
        try:
            meta = {"configuration": f.get("Configuration"), "tags": f.get("Tags"), "concurrency": f.get("Concurrency")}
            try:
                meta["url"] = lam.get_function_url_config(FunctionName=fn)
            except lam.exceptions.ResourceNotFoundException:
                meta["url"] = None
            loc = (f.get("Code") or {}).get("Location")
            if loc:
                with urllib.request.urlopen(loc, timeout=120) as h:
                    blob = h.read()
                s3.put_object(Bucket=PRIVATE, Key="backups/deleted-functions/%s/%s.zip" % (stamp, fn), Body=blob, ServerSideEncryption="AES256")
                meta["zip_bytes"] = len(blob)
            s3.put_object(Bucket=PRIVATE, Key="backups/deleted-functions/%s/%s.json" % (stamp, fn),
                          Body=json.dumps(meta, default=str, indent=1).encode(), ServerSideEncryption="AES256")
            lam.delete_function(FunctionName=fn)
            return fn, "deleted", meta.get("zip_bytes")
        except Exception as e:  # noqa: BLE001
            return fn, "failed: %s" % type(e).__name__, None
    with cf.ThreadPoolExecutor(max_workers=4) as ex:
        for fn, status, nbytes in ex.map(one, sorted(PLAN["delete"])):
            (done if status in ("deleted", "already gone") else failed).append((fn, status, nbytes))
    r.log("deleted %d function(s) (packages backed up under backups/deleted-functions/%s/); failed %d %s" % (
        len(done), stamp, len(failed), failed[:5]))
    # rules whose only target was a deleted function
    dropped = []
    for fn, spec in PLAN["delete"].items():
        for t in spec["triggers"]:
            if t["kind"] != "rule":
                continue
            bus = t.get("bus") or "default"
            try:
                targets = ca.pages(events, "list_targets_by_rule", "Targets", Rule=t["name"], EventBusName=bus)
            except Exception:  # noqa: BLE001
                continue
            mine = [x for x in targets if ca.fn_name(x.get("Arn")) == fn]
            if not mine:
                continue
            store.put(ca.safe_key("rule-targets", bus, t["name"] + ".json"), {"removed": mine, "all": targets})
            events.remove_targets(Rule=t["name"], EventBusName=bus, Ids=[x["Id"] for x in mine])
            if len(mine) == len(targets):
                live = events.describe_rule(Name=t["name"], EventBusName=bus)
                store.put(ca.safe_key("rule", bus, t["name"] + ".json"), {"rule": {k: v for k, v in live.items() if k != "ResponseMetadata"}})
                events.delete_rule(Name=t["name"], EventBusName=bus)
                dropped.append(t["name"])
    r.log("rules deleted with their only target: %s" % (dropped or "none"))
    return done, failed


def main():
    with report("ops_6563_retire_engines") as r:
        r.heading("ops 6563 - retire engines: switch off %d, delete %d (backed up)" % (len(PLAN["switch_off"]), len(PLAN["delete"])))
        stamp = ca.stamp()
        s3, events, scheduler = c("s3"), c("events"), c("scheduler")
        store = ca.Store(s3, PRIVATE, "backups/retire-2026-10-20/%s" % stamp)
        switch_off(r, store, events, scheduler, s3)
        done, failed = backup_and_delete(r, store, s3, events, stamp)
        lam = c("lambda")
        n = 0
        for p in lam.get_paginator("list_functions").paginate():
            n += len(p["Functions"])
        r.kv(switched_off=len(PLAN["switch_off"]), deleted=len([d for d in done if d[1] == "deleted"]), delete_failed=len(failed),
             functions_now=n, backups="s3://%s/backups/deleted-functions/%s/" % (PRIVATE, stamp))
        if failed:
            r.fail("%d deletions failed" % len(failed))
            raise SystemExit(1)
        r.ok("retired; fleet is %d functions" % n)


if __name__ == "__main__":
    main()
