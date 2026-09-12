"""Runner-only: attach exactly six declared schedules; prove all other wires unchanged.

Uses the deployed reconciler's explicit attach-brief-compiler mode, never broad
enforcement. The only manifest migration is six classic declarations to the six
reviewed Scheduler records in git. No vendor calls or importer invocations.
"""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "aws/lambdas/justhodl-schedule-reconciler/source"
sys.path[:0] = [str(ROOT / "aws/ops"), str(SOURCE)]
from ops_report import report
from brief_schedules import FUNCTION_ARN, ROLE_ARN, SPECS, requests_from_manifest, verify_schedule

B = "justhodl-dashboard-live"
MANIFEST = "config/schedule-manifest.json"
RECONCILER = "justhodl-schedule-reconciler"
ROLE = ROLE_ARN.rsplit("/", 1)[1]


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, default=str).encode()).hexdigest()


def others(manifest):
    return {k:[r for r in manifest.get(k, []) if r.get("name") not in SPECS]
            for k in ("rules", "schedules")}


def main():
    assert os.environ.get("GITHUB_ACTIONS") == "true", "Runner only"
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=330, retries={"max_attempts": 1}))
    ev = boto3.client("events", region_name="us-east-1")
    sch = boto3.client("scheduler", region_name="us-east-1")
    iam = boto3.client("iam", region_name="us-east-1")
    proof = {"at": datetime.now(timezone.utc).isoformat(), "objects": {}, "actions": []}

    def read(key):
        obj = s3.get_object(Bucket=B, Key=key)
        proof["objects"][key] = {"last_modified": obj["LastModified"].isoformat()}
        return json.loads(obj["Body"].read()), obj

    def invoke(fn, payload):
        inv = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
        body = json.loads(inv["Payload"].read())
        if inv.get("FunctionError") or body.get("ok") is not True:
            raise RuntimeError("Invocation failed: %s %s" % (fn, str(body)[:250]))
        return body

    def package(fn, members):
        full = lam.get_function(FunctionName=fn)
        with urllib.request.urlopen(full["Code"]["Location"], timeout=60) as res:
            z = zipfile.ZipFile(io.BytesIO(res.read()))
        hashes = {}
        for name in members:
            raw = z.read(name)
            local = ROOT / "aws/lambdas" / fn / "source" / name
            if not local.exists(): local = ROOT / "aws/shared" / name
            assert raw == local.read_bytes(), "Deployed package differs from checkout: " + name
            hashes[name] = hashlib.sha256(raw).hexdigest()
        cfg = full["Configuration"]
        return {"code_sha256": cfg["CodeSha256"], "last_modified": cfg["LastModified"],
                "role": cfg["Role"], "files": hashes}

    def live_snapshot():
        # Full target payloads remain in memory; only hashes enter public reports.
        out = {}
        for page in ev.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                if not r.get("ScheduleExpression"): continue
                targets = []
                for tp in ev.get_paginator("list_targets_by_rule").paginate(Rule=r["Name"]):
                    targets.extend(tp["Targets"])
                if any(t.get("Arn", "").split(":function:")[-1].split(":")[0] == "justhodl-brief-compiler" for t in targets):
                    assert r["Name"] in SPECS, "Unexpected compiler rule: " + r["Name"]
                out["events/" + r["Name"]] = {"rule": r, "targets": sorted(targets, key=lambda x:x["Id"])}
        for page in sch.get_paginator("list_schedules").paginate():
            for row in page["Schedules"]:
                g = row.get("GroupName", "default")
                d = sch.get_schedule(Name=row["Name"], GroupName=g)
                if d.get("Target", {}).get("Arn", "").split(":function:")[-1].split(":")[0] == "justhodl-brief-compiler":
                    assert g == "default" and row["Name"] in SPECS, "Unexpected compiler Scheduler target"
                out["scheduler/" + g + "/" + row["Name"]] = {k:v for k,v in d.items() if k not in ("ResponseMetadata", "CreationDate", "LastModificationDate")}
        return out

    def unrelated_live(snapshot):
        return {k:v for k,v in snapshot.items() if k not in
                {"events/" + n for n in SPECS} | {"scheduler/default/" + n for n in SPECS}}

    with report("ops_5448_attach_brief_compiler") as r:
        proof["reconciler_package"] = package(RECONCILER, ("lambda_function.py", "brief_schedules.py"))
        proof["compiler_package"] = package("justhodl-brief-compiler", ("lambda_function.py", "brief_compiler.py", "brief_contract.py"))
        before_manifest, obj = read(MANIFEST)
        reviewed = json.loads((ROOT / MANIFEST).read_text())
        desired_requests = requests_from_manifest(reviewed)
        desired_rows = [x for x in reviewed["schedules"] if x.get("name") in SPECS]
        existing_rows = [x for k in ("rules", "schedules") for x in before_manifest.get(k, []) if x.get("name") in SPECS]
        assert len(existing_rows) == 6 and {x["name"] for x in existing_rows} == set(SPECS)
        for x in existing_rows:
            expr, mode = SPECS[x["name"]]
            assert x["expr"] == expr and x["state"] == "ENABLED"
            assert len(x["targets"]) == 1 and x["targets"][0]["arn"] == FUNCTION_ARN
            assert json.loads(x["targets"][0]["input"]) == {"mode": mode}
        candidate = copy.deepcopy(before_manifest)
        candidate.update(others(candidate))
        candidate["schedules"].extend(desired_rows)
        requests_from_manifest(candidate)
        assert others(candidate) == others(before_manifest)
        before_live = live_snapshot()
        # Both transports were missing in 5447; refuse to retire any live rule.
        assert all("events/" + n not in before_live for n in SPECS), "Classic compiler rule exists"
        proof["audit_before"] = invoke(RECONCILER, {"mode": "audit"})
        drift_before, _ = read("data/schedule-drift.json")
        proof["drift_before"] = {k:drift_before[k] for k in ("drift_count", "by_class", "drifts")}

        trust = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
            "Principal": {"Service": "scheduler.amazonaws.com"}, "Action": "sts:AssumeRole",
            "Condition": {"StringEquals": {"aws:SourceAccount": "857687956942"},
                          "ArnEquals": {"aws:SourceArn": "arn:aws:scheduler:us-east-1:857687956942:schedule-group/default"}}}]}
        permission = {"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
            "Action": "lambda:InvokeFunction", "Resource": FUNCTION_ARN}]}
        try:
            role = iam.get_role(RoleName=ROLE)["Role"]
            assert role["AssumeRolePolicyDocument"] == trust, "Existing execution role has unexpected trust"
        except iam.exceptions.NoSuchEntityException:
            iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(trust), Description="Invoke only justhodl-brief-compiler from the six manifest schedules")
            proof["actions"].append("created dedicated compiler Scheduler execution role")
        try:
            old_policy = iam.get_role_policy(RoleName=ROLE, PolicyName="InvokeBriefCompiler")["PolicyDocument"]
            assert old_policy == permission, "Existing compiler invoke policy differs"
        except iam.exceptions.NoSuchEntityException:
            iam.put_role_policy(RoleName=ROLE, PolicyName="InvokeBriefCompiler", PolicyDocument=json.dumps(permission))
            proof["actions"].append("granted compiler-only invoke permission")
        reconciler_role = proof["reconciler_package"]["role"]
        resources = ["arn:aws:scheduler:us-east-1:857687956942:schedule/default/" + n for n in SPECS]
        decisions = iam.simulate_principal_policy(PolicySourceArn=reconciler_role,
            ActionNames=["scheduler:CreateSchedule", "scheduler:GetSchedule"], ResourceArns=resources)["EvaluationResults"]
        passrole = iam.simulate_principal_policy(PolicySourceArn=reconciler_role,
            ActionNames=["iam:PassRole"], ResourceArns=[ROLE_ARN], ContextEntries=[{
                "ContextKeyName": "iam:PassedToService", "ContextKeyValues": ["scheduler.amazonaws.com"], "ContextKeyType": "string"}])["EvaluationResults"]
        if not all(x["EvalDecision"] == "allowed" for x in decisions + passrole):
            assert not any(x["EvalDecision"] == "explicitDeny" for x in decisions + passrole), "Explicit IAM denial"
            scoped_policy = {"Version": "2012-10-17", "Statement": [
                {"Effect": "Allow", "Action": ["scheduler:CreateSchedule", "scheduler:GetSchedule"], "Resource": resources},
                {"Effect": "Allow", "Action": "iam:PassRole", "Resource": ROLE_ARN,
                 "Condition": {"StringEquals": {"iam:PassedToService": "scheduler.amazonaws.com"}}}]}
            # The shared execution role is already at IAM's 10,240-byte inline
            # policy quota. Use one exact, reviewed managed policy; do not edit
            # or compress any of its existing permissions to make room.
            policy_name = "justhodl-reconcile-six-brief-schedules"
            policy_arn = "arn:aws:iam::857687956942:policy/" + policy_name
            try:
                policy = iam.get_policy(PolicyArn=policy_arn)["Policy"]
                current_policy = iam.get_policy_version(PolicyArn=policy_arn,
                    VersionId=policy["DefaultVersionId"])["PolicyVersion"]["Document"]
                assert current_policy == scoped_policy, "Existing managed policy differs from reviewed six-schedule scope"
            except iam.exceptions.NoSuchEntityException:
                iam.create_policy(PolicyName=policy_name, PolicyDocument=json.dumps(scoped_policy),
                    Description="Reconcile only the six reviewed JustHodl brief compiler schedules")
            iam.attach_role_policy(RoleName=reconciler_role.rsplit("/", 1)[1], PolicyArn=policy_arn)
            proof["reconciler_managed_policy"] = policy_arn
            proof["actions"].append("granted reconciler create/get only for six schedules and pass only compiler execution role")
        # Role propagation only; no wait for cron and no vendor or Lambda warmup.
        if proof["actions"]: time.sleep(12)
        if candidate != before_manifest:
            backup = "config/ops/5448/schedule-manifest-" + digest(before_manifest) + ".json"
            try:
                s3.put_object(Bucket=B, Key=backup, Body=json.dumps(before_manifest).encode(), ContentType="application/json", IfNoneMatch="*")
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("PreconditionFailed", "412"): raise
            candidate["generated_at"] = datetime.now(timezone.utc).isoformat()
            candidate["source"] = "ops 5448: six compiler declarations migrated to Scheduler; all other declarations preserved"
            s3.put_object(Bucket=B, Key=MANIFEST, Body=json.dumps(candidate).encode(), ContentType="application/json", IfMatch=obj["ETag"])
            proof["manifest_backup_key"] = backup
            proof["actions"].append("migrated exactly six compiler manifest declarations to Scheduler")
        proof["attach_invoke"] = invoke(RECONCILER, {"mode": "attach-brief-compiler"})
        after_manifest, _ = read(MANIFEST)
        assert others(before_manifest) == others(after_manifest), "Unrelated manifest declaration changed"
        proof["other_manifest_sha256"] = digest(others(after_manifest))
        after_live = live_snapshot()
        assert unrelated_live(before_live) == unrelated_live(after_live), "An unrelated live schedule changed during this run"
        proof["other_live_sha256"] = digest(unrelated_live(after_live))
        proof["other_live_unchanged_count"] = len(unrelated_live(after_live))
        proof["schedules"] = []
        for req in desired_requests:
            current = sch.get_schedule(Name=req["Name"], GroupName=req["GroupName"])
            verify_schedule(current, req)
            proof["schedules"].append({k:current[k] for k in ("Name", "Arn", "State", "ScheduleExpression", "ScheduleExpressionTimezone", "LastModificationDate", "Target")})
        drift_after, _ = read("data/schedule-drift.json")
        proof["drift_after"] = {k:drift_after[k] for k in ("drift_count", "by_class", "drifts", "compiler_attachment")}
        assert drift_after["enforced"] == [], "General enforcement must remain empty"
        assert not any(x["key"].split("/")[-1] in SPECS for x in drift_after["drifts"])
        before_other = [d for d in drift_before["drifts"] if d["key"].split("/")[-1] not in SPECS]
        assert sorted(before_other, key=lambda d:(d["key"],d["drift"])) == sorted(drift_after["drifts"], key=lambda d:(d["key"],d["drift"]))
        r.ok("six ENABLED compiler schedules verified; drift %s -> %s; other live schedules unchanged=%s" % (drift_before["drift_count"], drift_after["drift_count"], proof["other_live_unchanged_count"]))

        started = datetime.now(timezone.utc)
        proof["compiler_all"] = invoke("justhodl-brief-compiler", {"mode": "all"})
        proof["briefs"] = {}
        for name in ("plumbing", "market-tape", "event", "official-stats", "positioning"):
            key = "data/" + name + "-brief.json"
            d, o = read(key)
            assert o["LastModified"] >= started.replace(microsecond=0), "Brief predates invocation"
            assert d["source"] == "justhodl-brief-compiler" and d["status"] == "LIVE", key
            proof["briefs"][key] = d
            r.ok("%s lm=%s source=%s %s" % (key, o["LastModified"].isoformat(), d["source"], d["why"]))
        pos = proof["briefs"]["data/positioning-brief.json"]["fields"]
        assert tuple(pos[k] for k in ("accumulating", "distributing", "flat", "n_with_inst_trans")) == (3200, 1832, 32, 5064)
        assert pos["breadth_basis"] == "uncapped"
        verdict, _ = read("data/verdict.json")
        assert verdict["shadow_mode"] is True and verdict["missing_families"] == ["CATALYST"]
        assert round(verdict["coverage"], 3) == 0.923
        proof["verdict"] = {k:verdict.get(k) for k in ("writer", "as_of", "coverage", "missing_families", "shadow_mode")}
        health, _ = read("data/import-health.json")
        proof["health"] = health
        imf, imf_obj = read("data/warm/imf-full/_state/state.json")
        proof["imf"] = {"phase": imf.get("phase"), "as_of": imf.get("as_of"), "n_banked": imf.get("n_banked"), "queue_left": len(imf.get("queue", [])), "have_count": len(imf.get("have", {})), "failure_count": len(imf.get("failures", {})), "lease_until": imf.get("lease_until"), "last_modified": imf_obj["LastModified"].isoformat()}
        proof["status"] = "PASS"
        proof["finished_at"] = datetime.now(timezone.utc).isoformat()
        (ROOT / "aws/ops/reports/5448_brief_compiler_attached.json").write_text(json.dumps(proof, indent=2, default=str) + "\n")
        r.ok("PASS: manifest, six schedules, unchanged other wires, compiler package, and five live briefs verified")


if __name__ == "__main__":
    main()
