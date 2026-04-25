#!/usr/bin/env python3
"""
Step 95 — Investigate the 6 Lambdas at 100% error rate (read-only).

For each:
  1. Read most-recent CloudWatch log stream (last 50 events)
  2. Extract the error message / traceback / failure mode
  3. Check whether source code exists in repo for context
  4. Check what S3 file (if any) it should be writing
  5. Make a per-Lambda recommendation: FIX, DISABLE, or DELETE

The 6 (excluding scrapeMacroData which we already disabled):
  - news-sentiment-agent (439 inv/7d, 100% err)
  - global-liquidity-agent-v2 (439 inv/7d, 100% err)
  - fmp-stock-picks-agent (90 inv/7d, 100% err)
  - daily-liquidity-report (21 inv/7d, 100% err)
  - ecb-data-daily-updater (21 inv/7d, 100% err)
  - treasury-auto-updater (6 inv/7d, 100% err)

Plus the 7th already-disabled but still costing data:
  - justhodl-data-collector (234 inv/7d, 99 err — 42% rate, also concerning)

Output: aws/ops/audit/broken_lambdas_2026-04-25.md with
        per-Lambda triage and recommendation.
"""
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


BROKEN_LAMBDAS = [
    "news-sentiment-agent",
    "global-liquidity-agent-v2",
    "fmp-stock-picks-agent",
    "daily-liquidity-report",
    "ecb-data-daily-updater",
    "treasury-auto-updater",
    "justhodl-data-collector",  # 42% err rate
]


def get_recent_log_lines(name, limit=80):
    """Pull recent log events focusing on errors."""
    try:
        streams = logs.describe_log_streams(
            logGroupName=f"/aws/lambda/{name}",
            orderBy="LastEventTime", descending=True, limit=3,
        ).get("logStreams", [])
    except logs.exceptions.ResourceNotFoundException:
        return None, "log group does not exist"
    except Exception as e:
        return None, f"log fetch failed: {e}"

    if not streams:
        return None, "no log streams"

    all_events = []
    for s in streams[:2]:  # Top 2 most-recent streams
        try:
            ev = logs.get_log_events(
                logGroupName=f"/aws/lambda/{name}",
                logStreamName=s["logStreamName"],
                limit=limit, startFromHead=False,
            )
            for e in ev.get("events", []):
                all_events.append({
                    "ts": e["timestamp"],
                    "msg": e["message"].rstrip(),
                    "stream": s["logStreamName"],
                })
        except Exception as e:
            pass

    # Sort by timestamp desc
    all_events.sort(key=lambda x: -x["ts"])
    return all_events, None


def find_source(name):
    """Look up the Lambda source if in repo."""
    base = REPO_ROOT / "aws/lambdas" / name / "source"
    if not base.exists():
        return None
    for fname in ["lambda_function.py", "index.py", "handler.py", "app.py"]:
        p = base / fname
        if p.exists():
            return p, p.read_text(encoding="utf-8", errors="ignore")
    for p in base.rglob("*.py"):
        return p, p.read_text(encoding="utf-8", errors="ignore")
    return None


def get_eb_rules_for_lambda(name):
    """Find EB rules targeting this Lambda."""
    try:
        target_arn = f"arn:aws:lambda:us-east-1:857687956942:function:{name}"
        resp = eb.list_rule_names_by_target(TargetArn=target_arn)
        rule_names = resp.get("RuleNames", [])
        rules = []
        for rn in rule_names:
            try:
                d = eb.describe_rule(Name=rn)
                rules.append({
                    "name": rn,
                    "schedule": d.get("ScheduleExpression"),
                    "state": d.get("State"),
                })
            except Exception:
                pass
        return rules
    except Exception:
        return []


def extract_error_signature(events):
    """From a list of log events, find the most-likely error message."""
    if not events:
        return None

    # Look for the most common patterns
    error_indicators = ["Traceback", "ERROR", "Error", "Exception", "failed", "FAILED"]

    # Find the first match
    for e in events[:30]:  # Top 30 most-recent
        msg = e["msg"]
        for ind in error_indicators:
            if ind in msg:
                # Capture this and a few surrounding lines for context
                idx = events.index(e)
                # Take a slice of 5 lines around it
                window = events[max(0, idx - 1):idx + 5]
                return "\n".join(w["msg"] for w in window)[:1500]
    # Nothing matched explicitly — return first non-INIT line
    for e in events[:20]:
        m = e["msg"]
        if not m.startswith(("INIT_START", "REPORT", "END Request", "START Request")):
            return m[:500]
    return None


with report("investigate_broken_lambdas") as r:
    r.heading("Investigate 7 Lambdas with high error rates (read-only)")

    findings = []

    for name in BROKEN_LAMBDAS:
        r.section(f"--- {name} ---")
        finding = {"name": name, "logs_summary": "", "source_exists": False,
                   "eb_rules": [], "recommendation": ""}

        # 1. Get current configuration
        try:
            cfg = lam.get_function_configuration(FunctionName=name)
            finding["last_modified"] = cfg.get("LastModified")
            finding["mem_mb"] = cfg.get("MemorySize")
            finding["timeout"] = cfg.get("Timeout")
            finding["runtime"] = cfg.get("Runtime")
            r.log(f"  Config: runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')}MB timeout={cfg.get('Timeout')}s")
            r.log(f"  Last modified: {cfg.get('LastModified')}")
        except Exception as e:
            r.warn(f"  get_function_configuration failed: {e}")
            continue

        # 2. EB rules
        rules = get_eb_rules_for_lambda(name)
        finding["eb_rules"] = rules
        if rules:
            r.log(f"  EB rules: {len(rules)}")
            for rule in rules:
                r.log(f"    {rule['name']:40} state={rule['state']:10} schedule={rule['schedule']}")
        else:
            r.log("  No EB rules — Lambda must be invoked manually or by another service")

        # 3. Source in repo?
        src_result = find_source(name)
        if src_result:
            path, src = src_result
            finding["source_exists"] = True
            finding["source_path"] = str(path.relative_to(REPO_ROOT))
            finding["source_loc"] = src.count("\n")
            r.log(f"  Source: {path.relative_to(REPO_ROOT)} ({src.count(chr(10))} LOC)")
        else:
            r.log(f"  Source NOT in repo")

        # 4. Recent logs
        events, err = get_recent_log_lines(name, limit=100)
        if err:
            r.log(f"  Logs: {err}")
            finding["logs_summary"] = err
        elif not events:
            r.log(f"  No log events found")
            finding["logs_summary"] = "no events"
        else:
            r.log(f"  Found {len(events)} log events from latest streams")
            err_sig = extract_error_signature(events)
            if err_sig:
                r.log(f"  Error signature:")
                for line in err_sig.split("\n")[:8]:
                    r.log(f"    {line[:200]}")
                finding["logs_summary"] = err_sig
            else:
                # Just print last 5 events
                r.log(f"  Recent log lines (no obvious error):")
                for e in events[:5]:
                    r.log(f"    {e['msg'][:200]}")
                finding["logs_summary"] = "\n".join(e["msg"] for e in events[:5])[:500]

        findings.append(finding)

    # ─── Build recommendations doc ─────────────────────────────────────
    r.section("Build per-Lambda recommendation doc")

    md = []
    md.append(f"# Broken Lambdas — Investigation & Recommendations\n")
    md.append(f"**Generated:** {datetime.now(timezone.utc).isoformat()}")
    md.append(f"**Scope:** 7 Lambdas with high error rates surfaced by health monitor 2026-04-25\n")
    md.append("---\n")

    md.append("## Summary table\n")
    md.append("| Lambda | EB rules | Source in repo | Recommendation |")
    md.append("|---|---|---|---|")
    for f in findings:
        rules_str = ", ".join(r["name"] for r in f["eb_rules"]) if f["eb_rules"] else "none"
        src_str = "yes" if f["source_exists"] else "no"
        # Recommendation derived per-Lambda below; placeholder for now
        md.append(f"| `{f['name']}` | {rules_str[:50]} | {src_str} | _(see below)_ |")
    md.append("")

    md.append("## Per-Lambda findings\n")
    for f in findings:
        md.append(f"### `{f['name']}`\n")
        md.append(f"- **Runtime:** `{f.get('runtime', '?')}`")
        md.append(f"- **Memory:** {f.get('mem_mb', '?')}MB")
        md.append(f"- **Timeout:** {f.get('timeout', '?')}s")
        md.append(f"- **Last modified:** {f.get('last_modified', '?')}")
        if f["eb_rules"]:
            md.append(f"- **EB rules:**")
            for rule in f["eb_rules"]:
                md.append(f"  - `{rule['name']}` — `{rule['schedule']}` ({rule['state']})")
        else:
            md.append(f"- **EB rules:** none (orphan or invoked elsewhere)")
        if f["source_exists"]:
            md.append(f"- **Source:** [`{f['source_path']}`]({f['source_path']}) ({f.get('source_loc', '?')} LOC)")
        else:
            md.append(f"- **Source:** not in repo")
        md.append("")
        md.append("**Error signature:**")
        md.append("```")
        for line in (f["logs_summary"] or "")[:1500].split("\n")[:15]:
            md.append(line[:200])
        md.append("```")
        md.append("")

    out_path = REPO_ROOT / "aws/ops/audit/broken_lambdas_2026-04-25.md"
    out_path.write_text("\n".join(md))
    r.ok(f"  Wrote: {out_path.relative_to(REPO_ROOT)}")

    s3.put_object(
        Bucket="justhodl-dashboard-live",
        Key="_audit/broken_lambdas_2026-04-25.md",
        Body="\n".join(md).encode(),
        ContentType="text/markdown",
    )
    r.ok(f"  S3 backup: _audit/broken_lambdas_2026-04-25.md")

    r.kv(
        lambdas_investigated=len(findings),
        with_source_in_repo=sum(1 for f in findings if f["source_exists"]),
        with_eb_rules=sum(1 for f in findings if f["eb_rules"]),
    )
    r.log("Done")
