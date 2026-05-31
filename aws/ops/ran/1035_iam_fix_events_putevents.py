#!/usr/bin/env python3
"""Step 1035 — CRITICAL IAM FIX.

ROOT CAUSE FOUND
════════════════
Master-ranker logs revealed:
  '[system_events] publish_many err: AccessDeniedException: ... PutEvents ...'

The lambda-execution-role lacks events:PutEvents permission for the
custom bus justhodl-system-events. This means ALL of this session's
event producers (signal-scorecard, cross-asset-regime, calibrator,
crisis-plumbing.engine.error, signal-board, master-ranker, miss-detector,
miss-calibrator, outcome-checker) have been silently failing to publish
events.

The 1022 synthetic test worked only because it ran from an ops script
using personal admin credentials, not from inside a Lambda.

This is THE bug. Fixing it makes the entire event-bus architecture come
to life.

FIX
═══
Attach an inline policy to lambda-execution-role granting:
  - events:PutEvents on:
      arn:aws:events:us-east-1:857687956942:event-bus/default
      arn:aws:events:us-east-1:857687956942:event-bus/justhodl-system-events

After the fix:
  - Redeploys master-ranker (no code change, just re-warm the function)
  - Deletes master-ranker.json.prev
  - Invokes master-ranker
  - Waits 75s
  - Reads audit log — expects ≥38 convergence.tier_up events
"""
import json, os, pathlib, time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1035_iam_fix_events_putevents.json"
REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"
ROLE_NAME = "lambda-execution-role"
POLICY_NAME = "lambda-publish-events"

iam = boto3.client("iam", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


POLICY_DOC = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPublishToCustomBus",
            "Effect": "Allow",
            "Action": [
                "events:PutEvents",
            ],
            "Resource": [
                f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/default",
                f"arn:aws:events:{REGION}:{ACCOUNT_ID}:event-bus/justhodl-system-events",
            ],
        },
    ],
}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    # 1. Capture current role policies (for record)
    print("[1035] capturing current role state…")
    try:
        attached = iam.list_attached_role_policies(RoleName=ROLE_NAME)
        inline = iam.list_role_policies(RoleName=ROLE_NAME)
        out["before"] = {
            "attached_policies": [p["PolicyName"] for p in attached.get("AttachedPolicies", [])],
            "inline_policies":   inline.get("PolicyNames", []),
        }
    except Exception as e:
        out["before_err"] = str(e)[:200]
    
    # 2. Attach the inline policy
    print(f"[1035] adding inline policy '{POLICY_NAME}' for events:PutEvents…")
    try:
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(POLICY_DOC),
        )
        out["policy_attached"] = True
    except Exception as e:
        out["policy_attached"] = False
        out["policy_err"] = str(e)[:200]
        # If we can't fix IAM, abort
        pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
        pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
        return
    
    # 3. Verify policy attached
    try:
        policy = iam.get_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
        out["policy_verified"] = json.loads(policy["PolicyDocument"]) \
                                    if isinstance(policy.get("PolicyDocument"), str) \
                                    else policy.get("PolicyDocument")
    except Exception as e:
        out["policy_verify_err"] = str(e)[:200]
    
    # 4. Wait for IAM propagation (can take 30-60s)
    print("[1035] waiting 45s for IAM eventual consistency…")
    time.sleep(45)
    
    # 5. Force-fire master-ranker
    print("[1035] deleting master-ranker.json.prev…")
    try:
        s3.delete_object(Bucket=BUCKET, Key="data/master-ranker.json.prev")
    except Exception:
        pass
    
    print("[1035] invoking master-ranker…")
    try:
        r = lam.invoke(FunctionName="justhodl-master-ranker",
                          InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode("utf-8", errors="replace")
        try:
            p = json.loads(body)
            out["invoke"] = json.loads(p["body"]) if isinstance(p.get("body"), str) else p
        except Exception:
            out["invoke_raw"] = body[:400]
    except Exception as e:
        out["invoke_err"] = str(e)[:200]
    
    # 6. Wait for pipeline + read audit log
    print("[1035] waiting 75s for events → coordinator → audit log…")
    time.sleep(75)
    
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        entries = [json.loads(l) for l in lines]
        conv = [e for e in entries if e.get("event") == "convergence.tier_up"]
        out["audit"] = {
            "n_total":           len(entries),
            "n_convergence":     len(conv),
            "tier_5_count":      sum(1 for e in conv
                                       if (e.get("detail") or {}).get("new_tier") == 5),
            "tier_3_count":      sum(1 for e in conv
                                       if (e.get("detail") or {}).get("new_tier") == 3),
            "sample_first_5":    [{
                "ts":       e.get("ts", "")[:19],
                "ticker":   (e.get("detail") or {}).get("ticker"),
                "new_tier": (e.get("detail") or {}).get("new_tier"),
                "n_systems": (e.get("detail") or {}).get("n_systems"),
                "systems":  (e.get("detail") or {}).get("systems", [])[:4],
                "alpha_compass": next(
                    (i.get("ok") for i in
                     (e.get("route", {}) or {}).get("invokes") or []
                     if i.get("fn") == "justhodl-alpha-compass"),
                    None,
                ),
                "telegram_sent": (e.get("route") or {}).get("notify"),
            } for e in conv[:5]],
        }
    except Exception as e:
        out["audit_err"] = str(e)[:200]
    
    # 7. Snapshot all events today (final picture)
    if "audit" in out:
        from collections import defaultdict
        by_event = defaultdict(int)
        by_engine = defaultdict(int)
        for e in entries:
            by_event[e.get("event", "?")] += 1
            src = (e.get("detail") or {}).get("_source_engine", "?")
            by_engine[src] += 1
        out["audit"]["by_event_type"] = dict(by_event)
        out["audit"]["by_source_engine"] = dict(by_engine)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
