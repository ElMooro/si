"""Runner-only IMF queue accounting; at most one explicitly authorized Event kick.

No source HTTP, Lambda deployment, timeout edits, or state/queue rewrites.
The invocation receipt prevents a fix-forward rerun from sending another kick.
"""
import json
import os
from pathlib import Path
import re
import sys
import time
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws/ops"))
from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-imf-full"
KEY = "data/warm/imf-full/_state/state.json"
RECEIPT = "config/ops/5450-imf-event-once.json"
IDS = ("PIP", "IMTS", "IMTS_2026_MAY_VINTAGE")


def main():
    if os.environ.get("GITHUB_ACTIONS") != "true":
        sys.exit(1)
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3", region_name="us-east-1")
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 0}))
    logs = boto3.client("logs", region_name="us-east-1")
    proof = {"op": 5450, "started_at": datetime.now(timezone.utc).isoformat(),
             "invocations_sent_this_run": 0, "snapshots": []}
    output = ROOT / "aws/ops/reports/5450_imf_queue_result.json"
    prior = json.loads((ROOT / "aws/ops/reports/5449_imf_progress_proof.json").read_text())
    old_attempts = dict(zip(prior["imf"]["queued_flow_ids"], prior["imf"]["queue_attempts"]))

    def snapshot():
        o = s3.get_object(Bucket=B, Key=KEY)
        d = json.loads(o["Body"].read())
        now = datetime.now(timezone.utc)
        queue = dict(d.get("queue", []))
        # A lease expires ten seconds before the existing 850s timeout. Leave
        # a margin rather than racing the execution that still owns the state.
        active = float(d.get("lease_until") or 0) + 45 > now.timestamp()
        rows = []
        for fid in IDS:
            failure = (d.get("failures") or {}).get(fid)
            banked = (d.get("have") or {}).get(fid)
            if banked:
                status = "BANKED"
            elif failure:
                status = "NAMED_SOURCE_FAILURE"
            elif not active and (queue.get(fid, 0) >= 3 or (fid not in queue and old_attempts.get(fid, 0) >= 3)):
                status = "THREE_ATTEMPT_QUARANTINE"
            else:
                status = "UNTRIED" if queue.get(fid) == 0 else "PENDING_OR_IN_FLIGHT"
            rows.append({"id": fid, "status": status, "queued_attempts": queue.get(fid),
                         "prior_5449_attempts": old_attempts.get(fid), "failure": failure,
                         "banked": banked,
                         "quarantine_basis": "existing importer skips queue attempts >=3; no source refusal inferred" if status == "THREE_ATTEMPT_QUARANTINE" else None})
        snap = {"observed_at": now.isoformat(), "last_modified": o["LastModified"].isoformat(),
                "state_age_h": (now-o["LastModified"]).total_seconds()/3600,
                "phase": d.get("phase"), "as_of": d.get("as_of"), "banked": len(d.get("have", {})),
                "queued": len(queue), "failure_count": len(d.get("failures", {})),
                "lease_until": d.get("lease_until"), "lease_active_with_margin": active,
                "ids": rows}
        proof["snapshots"].append(snap)
        return snap, o, d

    def finished(snap):
        return all(x["status"] in ("BANKED", "NAMED_SOURCE_FAILURE", "THREE_ATTEMPT_QUARANTINE") for x in snap["ids"])

    with report("ops_5450_finish_imf_queue") as r:
        try:
            verdict = json.loads(s3.get_object(Bucket=B, Key="data/verdict.json")["Body"].read())
            if verdict.get("shadow_mode") is not True:
                raise RuntimeError("Shadow gate is not true; refusing work")
            proof["shadow_mode"] = verdict["shadow_mode"]
            snap, obj, state = snapshot()
            proof["before"] = snap
            r.log("before=" + json.dumps(snap, default=str))
            # Respect an already-running invocation; one new kick is permitted
            # for genuinely untried entries, or for state older than 48 hours.
            deadline = time.monotonic() + 2100
            receipt = None
            try:
                receipt = json.loads(s3.get_object(Bucket=B, Key=RECEIPT)["Body"].read())
                proof["existing_invocation_receipt"] = receipt
            except ClientError as exc:
                if exc.response["Error"]["Code"] not in ("NoSuchKey", "404"): raise
            while not finished(snap):
                untried = any(x["status"] == "UNTRIED" for x in snap["ids"])
                if not snap["lease_active_with_margin"] and receipt is None and (untried or snap["state_age_h"] > 48):
                    receipt = {"op": 5450, "function": FN, "kicked_by": "ops_5450",
                               "claimed_at": datetime.now(timezone.utc).isoformat(),
                               "before_last_modified": snap["last_modified"], "status": "CLAIMED"}
                    s3.put_object(Bucket=B, Key=RECEIPT, Body=json.dumps(receipt).encode(),
                                  ContentType="application/json", IfNoneMatch="*")
                    inv = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b'{"kicked_by":"ops_5450"}')
                    if inv.get("StatusCode") != 202:
                        raise RuntimeError("IMF Event invocation not accepted")
                    receipt["status"] = "ACCEPTED_202"
                    receipt["accepted_at"] = datetime.now(timezone.utc).isoformat()
                    s3.put_object(Bucket=B, Key=RECEIPT, Body=json.dumps(receipt).encode(), ContentType="application/json")
                    proof["invocations_sent_this_run"] = 1
                    proof["invocation_receipt"] = receipt
                    r.ok("one Event invocation accepted; watching existing IMF state")
                if time.monotonic() > deadline:
                    raise RuntimeError("IMF IDs still unaccounted after bounded observation; no second kick")
                time.sleep(25)
                previous = snap
                snap, obj, state = snapshot()
                if snap["last_modified"] != previous["last_modified"]:
                    r.log("state write banked=%s queued=%s ids=%s" % (snap["banked"], snap["queued"], [(x["id"],x["status"],x["queued_attempts"]) for x in snap["ids"]]))
            proof["after"] = snap
            # Account for the physical banked blobs, not merely counters.
            for row in snap["ids"]:
                if row["status"] == "BANKED":
                    key = "data/warm/imf-full/src/" + row["id"] + ".xml.gz"
                    o = s3.head_object(Bucket=B, Key=key)
                    row["object"] = {"key": key, "last_modified": o["LastModified"].isoformat(), "bytes": o["ContentLength"], "mode": o.get("Metadata", {}).get("mode")}
                    if o["ContentLength"] <= 0: raise RuntimeError("Empty banked IMF object")
            reports = logs.filter_log_events(logGroupName="/aws/lambda/"+FN,
                startTime=int(datetime(2026,9,12,14,32,tzinfo=timezone.utc).timestamp()*1000),
                filterPattern='"REPORT RequestId:"', limit=100)
            proof["execution_reports"] = []
            for ev in reports.get("events", []):
                item = {"at": datetime.fromtimestamp(ev["timestamp"]/1000, timezone.utc).isoformat()}
                for label in ("Duration", "Status", "Error Type"):
                    m = re.search(r"(?:^|\t)"+re.escape(label)+r":\s*([^\t\n]+)", ev["message"])
                    if m: item[label] = m.group(1).strip()
                proof["execution_reports"].append(item)
            proof["status"] = "PASS_BANKED_OR_NAMED_FAILURE"
            r.ok("IMF accounted: banked=%s queued=%s lm=%s" % (snap["banked"], snap["queued"], snap["last_modified"]))
            for row in snap["ids"]:
                r.log("%s: %s failure=%s" % (row["id"], row["status"], row["failure"]))
        except Exception as exc:
            proof["status"] = "FAIL"
            proof["error"] = {"type": type(exc).__name__, "message": str(exc)[:350]}
            r.fail(json.dumps(proof["error"]))
            output.write_text(json.dumps(proof, indent=2, default=str)+"\n")
            sys.exit(1)
        proof["finished_at"] = datetime.now(timezone.utc).isoformat()
        output.write_text(json.dumps(proof, indent=2, default=str)+"\n")


if __name__ == "__main__":
    main()
