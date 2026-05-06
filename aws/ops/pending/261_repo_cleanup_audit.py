#!/usr/bin/env python3
"""Step 261 — Audit deployed Lambdas vs repo dirs for cleanup planning.

Sandbox blocks AWS, so this runs in run-ops to enumerate the canonical
list of deployed Lambdas. The output goes to aws/ops/reports so a
follow-up local commit can do the archive moves.

For each deployed Lambda we capture:
  - function_name
  - last_modified (ISO from AWS metadata)
  - has_eventbridge_target (bool — driven by an EB rule)
  - has_function_url (bool — invoked from frontend)
  - last_invocation_metric_24h (CloudWatch)
  - is_in_repo (bool — does aws/lambdas/<name>/ exist locally)

Repo dirs that DO NOT correspond to any deployed Lambda are
archive candidates.
"""
import json
import os
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
REPORT_PATH = "aws/ops/reports/261_repo_cleanup_audit.json"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def list_deployed_lambdas():
    """Paginate through ListFunctions to get all Lambda names + metadata."""
    out = []
    next_marker = None
    while True:
        kw = {}
        if next_marker:
            kw["Marker"] = next_marker
        resp = lam.list_functions(**kw)
        for fn in resp.get("Functions", []):
            out.append({
                "function_name": fn["FunctionName"],
                "last_modified": fn.get("LastModified"),
                "runtime": fn.get("Runtime"),
                "memory_mb": fn.get("MemorySize"),
                "timeout_s": fn.get("Timeout"),
                "code_size": fn.get("CodeSize"),
            })
        next_marker = resp.get("NextMarker")
        if not next_marker:
            break
    return out


def list_eb_rule_targets():
    """Map of Lambda function name → list of EB rule names that target it."""
    out = {}
    next_token = None
    rule_list = []
    while True:
        kw = {}
        if next_token:
            kw["NextToken"] = next_token
        resp = events.list_rules(**kw)
        rule_list.extend(resp.get("Rules", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break

    for rule in rule_list:
        try:
            targets = events.list_targets_by_rule(Rule=rule["Name"]).get("Targets", [])
            for t in targets:
                arn = t.get("Arn", "")
                # Only Lambda targets
                if ":function:" in arn:
                    fn_name = arn.split(":function:")[-1].split(":")[0]
                    out.setdefault(fn_name, []).append(rule["Name"])
        except Exception:
            continue
    return out


def get_invocation_count_24h(fn_name):
    """How many times was this Lambda invoked in the last 24h?"""
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    try:
        resp = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn_name}],
            StartTime=start,
            EndTime=end,
            Period=86400,
            Statistics=["Sum"],
        )
        if resp.get("Datapoints"):
            return int(resp["Datapoints"][0].get("Sum", 0))
    except Exception:
        pass
    return 0


def has_function_url(fn_name):
    try:
        lam.get_function_url_config(FunctionName=fn_name)
        return True
    except ClientError:
        return False


def main():
    started = time.time()
    print("[261] listing deployed Lambdas…")
    deployed = list_deployed_lambdas()
    print(f"[261] found {len(deployed)} deployed Lambdas")

    print("[261] listing EventBridge rule targets…")
    eb_targets = list_eb_rule_targets()
    print(f"[261] found {len(eb_targets)} Lambdas with EB rules")

    # Discover repo dirs
    repo_dirs = sorted([d for d in os.listdir("aws/lambdas")
                        if os.path.isdir(f"aws/lambdas/{d}")])
    repo_set = set(repo_dirs)

    deployed_names = {f["function_name"] for f in deployed}

    # For each deployed Lambda, enrich with EB + function URL + invocations
    enriched = []
    for f in deployed:
        name = f["function_name"]
        eb_rules = eb_targets.get(name, [])
        f["eb_rules"] = eb_rules
        f["has_eb_rule"] = bool(eb_rules)
        f["has_function_url"] = has_function_url(name)
        f["invocations_24h"] = get_invocation_count_24h(name)
        f["is_in_repo"] = name in repo_set
        enriched.append(f)

    # Repo dirs that don't correspond to any deployed Lambda (true archive candidates)
    in_repo_not_deployed = sorted(repo_set - deployed_names)

    # Deployed Lambdas not in repo (rare but possible; we don't archive those)
    deployed_not_in_repo = sorted(deployed_names - repo_set)

    # Deployed Lambdas with NO EB rule, NO function URL, AND 0 invocations 24h
    # These are deployed-but-orphan and probably also archivable (after deletion)
    orphan_deployed = [
        f for f in enriched
        if not f["has_eb_rule"]
        and not f["has_function_url"]
        and f["invocations_24h"] == 0
    ]

    out = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duration_s": round(time.time() - started, 1),
        "n_deployed": len(deployed),
        "n_repo_dirs": len(repo_dirs),
        "n_in_repo_not_deployed": len(in_repo_not_deployed),
        "n_deployed_not_in_repo": len(deployed_not_in_repo),
        "n_orphan_deployed_no_invocations": len(orphan_deployed),
        # Archive candidates: in repo but not deployed (safe to move source aside)
        "archive_candidates_in_repo_not_deployed": in_repo_not_deployed,
        # Deployed Lambdas not in repo (probably console-created, document don't archive)
        "deployed_not_in_repo": deployed_not_in_repo,
        # Orphan deployed Lambdas (no EB, no URL, 0 invocations) — can be deleted
        # later but we surface them here for review.
        "orphan_deployed_no_invocations": [
            {"name": f["function_name"], "last_modified": f["last_modified"],
             "code_size": f["code_size"]}
            for f in orphan_deployed
        ],
        # Sanity checks
        "deployed_lambdas": [
            {
                "function_name": f["function_name"],
                "last_modified": f["last_modified"],
                "has_eb_rule": f["has_eb_rule"],
                "has_function_url": f["has_function_url"],
                "invocations_24h": f["invocations_24h"],
                "is_in_repo": f["is_in_repo"],
                "code_size": f["code_size"],
            }
            for f in enriched
        ],
    }

    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as fp:
        json.dump(out, fp, indent=2, default=str)

    # Print key numbers
    print(f"\n═══ RESULT ═══")
    print(f"  Deployed Lambdas:                   {out['n_deployed']}")
    print(f"  Repo Lambda dirs:                   {out['n_repo_dirs']}")
    print(f"  In repo but NOT deployed:           {out['n_in_repo_not_deployed']} ← archive candidates")
    print(f"  Deployed but not in repo:           {out['n_deployed_not_in_repo']}")
    print(f"  Orphan deployed (0 invocations 24h): {out['n_orphan_deployed_no_invocations']}")
    print(f"  Duration: {out['duration_s']}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
