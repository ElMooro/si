#!/usr/bin/env python3
"""
245_diagnose_repair_health_red.py
=================================

Diagnose + auto-repair the 7 Lambdas flagged RED on the health monitor on
2026-04-27. Runs via run-ops.yml (has AWS creds; this script does not).

Targets
-------
A. 0 invocations / EB rule probably disabled or deleted:
   - justhodl-data-collector       (expected rate(1 hour))
   - justhodl-email-reports-v2     (expected cron(0 12 * * ? *))
   - justhodl-khalid-metrics       (expected cron(0 11 * * ? *))
   - scrapeMacroData               (expected cron(0 12 * * ? *) + 21/21 errored)
   - fmp-stock-picks-agent         (expected hourly + 90/90 errored)

B. 100% error rate / handler mismatch already fixed in repo, deploy may
   not have synced AWS-side handler config:
   - news-sentiment-agent          (777/777 errored)

C. Low invocation count, schedule looks correct in repo:
   - justhodl-intelligence         (6/24h vs ≥10 expected)
   - justhodl-repo-monitor         (9/24h vs ≥10 expected)

What this script does
---------------------
1. For each Lambda: GetFunction config + list EB rules pointing at it.
2. Auto-repair (idempotent):
   - If a rule exists and is DISABLED → enable_rule()
   - If a Lambda has handler='lambda_function.lambda_handler' but the
     repo source has a different module → update_function_configuration
     to match the repo (we cannot read the AWS-side zip easily, so we
     match config to repo).
3. Print 24h CloudWatch Invocations metric for each, post-repair.

What this script does NOT do
----------------------------
- Create EB rules from scratch (risky without confirming intent)
- Delete or rename Lambdas
- Change permissions
- Touch anything outside the 7 named Lambdas
"""
from __future__ import annotations
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


# Map Lambda → expected handler from repo config.json (where applicable)
TARGETS = [
    "justhodl-data-collector",
    "justhodl-email-reports-v2",
    "justhodl-khalid-metrics",
    "scrapeMacroData",
    "fmp-stock-picks-agent",
    "news-sentiment-agent",
    "justhodl-intelligence",
    "justhodl-repo-monitor",
]


def get_repo_handler(fn: str) -> str | None:
    """What handler does our config.json say this Lambda should have?"""
    cfg = Path(f"aws/lambdas/{fn}/config.json")
    if not cfg.exists():
        return None
    try:
        d = json.loads(cfg.read_text())
        # Some configs use lowercase 'handler', others 'Handler'
        return d.get("handler") or d.get("Handler")
    except Exception:
        return None


def get_invocations_24h(fn: str) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start,
            EndTime=end,
            Period=86400,
            Statistics=["Sum"],
        )
        pts = r.get("Datapoints", [])
        return int(sum(p["Sum"] for p in pts))
    except ClientError:
        return -1


def get_errors_24h(fn: str) -> int:
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=24)
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda",
            MetricName="Errors",
            Dimensions=[{"Name": "FunctionName", "Value": fn}],
            StartTime=start,
            EndTime=end,
            Period=86400,
            Statistics=["Sum"],
        )
        pts = r.get("Datapoints", [])
        return int(sum(p["Sum"] for p in pts))
    except ClientError:
        return -1


def list_rules_for(fn: str) -> list[dict]:
    """Find every EB rule whose target is this Lambda."""
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        arn = cfg["FunctionArn"]
    except ClientError as e:
        return [{"_error": f"GetFunctionConfiguration: {e.response['Error']['Code']}"}]
    out = []
    paginator = events.get_paginator("list_rule_names_by_target")
    try:
        for page in paginator.paginate(TargetArn=arn):
            for name in page.get("RuleNames", []):
                rule = events.describe_rule(Name=name)
                out.append({
                    "name": rule["Name"],
                    "schedule": rule.get("ScheduleExpression"),
                    "state": rule.get("State"),
                    "arn": rule["Arn"],
                })
    except ClientError as e:
        out.append({"_error": f"list_rule_names_by_target: {e.response['Error']['Code']}"})
    return out


def get_aws_handler(fn: str) -> str | None:
    try:
        return lam.get_function_configuration(FunctionName=fn).get("Handler")
    except ClientError:
        return None


with report("diagnose_repair_health_red") as r:
    r.heading("Health-monitor RED triage — diagnose + auto-repair")
    r.log("Run timestamp: " + datetime.now(timezone.utc).isoformat(timespec="seconds"))
    r.log("Region: " + REGION)

    repaired_rules: list[str] = []
    repaired_handlers: list[str] = []
    findings: dict[str, dict] = {}

    for fn in TARGETS:
        r.section(f"── {fn} ──")
        item: dict = {"function": fn}

        # Existence
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            r.fail(f"  Lambda missing or inaccessible: {code}")
            item["status"] = "missing"
            item["error"] = code
            findings[fn] = item
            continue

        aws_handler = cfg.get("Handler")
        repo_handler = get_repo_handler(fn)
        runtime = cfg.get("Runtime")
        last_mod = cfg.get("LastModified", "?")[:19]
        r.log(f"  Runtime:    {runtime}")
        r.log(f"  Handler:    AWS={aws_handler!r}, repo={repo_handler!r}")
        r.log(f"  LastMod:    {last_mod}")

        item.update({
            "runtime": runtime,
            "aws_handler": aws_handler,
            "repo_handler": repo_handler,
            "last_modified": last_mod,
        })

        # CloudWatch
        inv = get_invocations_24h(fn)
        err = get_errors_24h(fn)
        r.log(f"  CloudWatch (24h): Invocations={inv}, Errors={err}, ErrRate={(err/inv*100 if inv>0 else 0):.1f}%")
        item["inv_24h"] = inv
        item["err_24h"] = err

        # EB rules
        rules = list_rules_for(fn)
        if not rules:
            r.warn(f"  EB rules: NONE pointing at this Lambda")
            item["rules"] = []
        else:
            for rule in rules:
                if "_error" in rule:
                    r.fail(f"  EB rule lookup error: {rule['_error']}")
                    continue
                state_color = "✓" if rule["state"] == "ENABLED" else "✗"
                r.log(f"  EB rule: {state_color} {rule['name']} state={rule['state']} schedule={rule['schedule']}")
            item["rules"] = [{k: v for k, v in r0.items() if not k.startswith("_")} for r0 in rules]

            # Auto-repair: enable any DISABLED rules
            for rule in rules:
                if rule.get("state") == "DISABLED":
                    try:
                        events.enable_rule(Name=rule["name"])
                        r.ok(f"  REPAIR: enabled rule '{rule['name']}'")
                        repaired_rules.append(f"{fn} ← {rule['name']}")
                    except ClientError as e:
                        r.fail(f"  enable_rule({rule['name']}) failed: {e.response['Error']['Code']}")

        # Auto-repair: handler mismatch (repo says X, AWS says Y)
        if repo_handler and aws_handler and repo_handler != aws_handler:
            # Verify the module the repo handler references actually exists
            module_name = repo_handler.split(".")[0]
            src_file = Path(f"aws/lambdas/{fn}/source/{module_name}.py")
            if src_file.exists():
                try:
                    lam.update_function_configuration(
                        FunctionName=fn,
                        Handler=repo_handler,
                    )
                    r.ok(f"  REPAIR: handler {aws_handler!r} → {repo_handler!r}")
                    repaired_handlers.append(f"{fn}: {aws_handler} → {repo_handler}")
                except ClientError as e:
                    r.fail(f"  update_function_configuration failed: {e.response['Error']['Code']}")
            else:
                r.warn(f"  Handler mismatch but repo source {src_file.name} not present — skipping")

        findings[fn] = item

    # ── Summary ──
    r.section("Summary")
    r.log(f"\n  Lambdas inspected: {len(TARGETS)}")
    r.log(f"  Rules re-enabled:  {len(repaired_rules)}")
    for rr in repaired_rules:
        r.log(f"    + {rr}")
    r.log(f"  Handlers fixed:    {len(repaired_handlers)}")
    for hh in repaired_handlers:
        r.log(f"    + {hh}")

    no_rules = [f for f, d in findings.items()
                if d.get("rules") == [] and d.get("status") != "missing"]
    if no_rules:
        r.warn(f"\n  Lambdas with NO EB rules (need investigation):")
        for f in no_rules:
            d = findings[f]
            r.log(f"    - {f}: {d.get('inv_24h',0)} invocations, {d.get('err_24h',0)} errors")

    # Persist machine-readable for follow-up
    out_path = Path("aws/ops/reports/latest/health_red_triage.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps({
        "run_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "repaired_rules": repaired_rules,
        "repaired_handlers": repaired_handlers,
        "findings": findings,
    }, indent=2, default=str))
    r.log(f"\n  Machine-readable findings: {out_path}")
