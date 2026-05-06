#!/usr/bin/env python3
"""
Step 204 — Cleanup: disable schedules for broken/zombie Lambdas
+ archive 3 dead pages.

CONSERVATIVE APPROACH: disable EventBridge rules (stops invocations,
saves cost). Lambdas themselves remain in case we want to fix them
later. To actually delete, use console + manual confirmation.

Cleanup targets:
  Lambda schedules to DISABLE:
    - news-sentiment-agent     (11/11 errors over 2d)
    - fmp-stock-picks-agent    (20/21 errors)
    - justhodl-daily-macro-report (0 invocations 2d, zombie)

  HTML pages to MOVE to /archive/:
    - pro.html                              (data 59 days stale)
    - exponential-search-dashboard.html     (uses dead OpenBB APIGW)
    - macroeconomic-platform.html           (uses dead OpenBB APIGW)

  HTML stubs to REMOVE (under 500B each):
    - Reports.html  (252B)
    - ml.html       (288B)
    - stocks.html   (249B)

  /repo.html — KEEP (now real, replaced 451B stub this session)
"""
import json, os, shutil
from pathlib import Path
from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

LAMBDAS_TO_DISABLE = [
    "news-sentiment-agent",
    "fmp-stock-picks-agent",
    "justhodl-daily-macro-report",
]

PAGES_TO_ARCHIVE = [
    "pro.html",
    "exponential-search-dashboard.html",
    "macroeconomic-platform.html",
]

STUBS_TO_REMOVE = [
    "Reports.html",
    "ml.html",
    "stocks.html",
]


with report("cleanup_broken_zombies") as r:
    r.heading("Cleanup: disable broken Lambdas + archive dead pages")

    # ─── A. Find + disable EventBridge rules for broken Lambdas ────────
    r.section("A. EventBridge rule lookup")
    rules_to_disable = []
    next_token = None
    while True:
        kwargs = {"Limit": 100}
        if next_token: kwargs["NextToken"] = next_token
        rules_resp = events.list_rules(**kwargs)
        for rule in rules_resp.get("Rules", []):
            if rule.get("State") != "ENABLED": continue
            try:
                tgts = events.list_targets_by_rule(Rule=rule["Name"])
                for t in tgts.get("Targets", []):
                    arn = t.get("Arn", "")
                    if ":lambda:" in arn:
                        ln = arn.split(":")[-1]
                        if ln in LAMBDAS_TO_DISABLE:
                            rules_to_disable.append({
                                "rule": rule["Name"],
                                "lambda": ln,
                                "schedule": rule.get("ScheduleExpression", "?"),
                            })
            except Exception:
                continue
        next_token = rules_resp.get("NextToken")
        if not next_token: break

    r.log(f"  Found {len(rules_to_disable)} rules to disable:")
    for info in rules_to_disable:
        r.log(f"    {info['rule']:55} → {info['lambda']:30} ({info['schedule']})")

    # ─── B. Disable each rule ───────────────────────────────────────────
    r.section("B. Disabling rules")
    for info in rules_to_disable:
        try:
            events.disable_rule(Name=info["rule"])
            r.ok(f"  disabled: {info['rule']}")
        except Exception as e:
            r.warn(f"  fail {info['rule']}: {e}")

    # ─── C. Archive dead pages ──────────────────────────────────────────
    r.section("C. Archive dead pages")
    repo_root = Path(os.environ.get("GITHUB_WORKSPACE", "/home/claude/si"))
    archive_dir = repo_root / "archive"
    archive_dir.mkdir(exist_ok=True)
    for page in PAGES_TO_ARCHIVE:
        src = repo_root / page
        if not src.exists():
            r.warn(f"  {page} not found, skipping")
            continue
        dst = archive_dir / page
        try:
            shutil.move(str(src), str(dst))
            r.ok(f"  archived: {page} → archive/{page}")
        except Exception as e:
            r.warn(f"  fail moving {page}: {e}")

    # ─── D. Remove stubs ────────────────────────────────────────────────
    r.section("D. Remove stubs")
    for stub in STUBS_TO_REMOVE:
        p = repo_root / stub
        if not p.exists():
            r.warn(f"  {stub} not found, skipping")
            continue
        size = p.stat().st_size
        if size > 500:
            r.warn(f"  {stub} is {size}B (>500B), refusing to delete (not a stub)")
            continue
        try:
            p.unlink()
            r.ok(f"  removed: {stub} ({size}B)")
        except Exception as e:
            r.warn(f"  fail removing {stub}: {e}")

    # ─── E. Write archive README ────────────────────────────────────────
    r.section("E. Archive README")
    readme_path = archive_dir / "README.md"
    readme_text = """# Archive

Pages here were once-active features that became unmaintainable
or whose data sources went away. They are kept for historical
reference. They are NOT deployed (GitHub Pages still serves them
because everything in the repo is served, but they're not linked
from anywhere).

## Why each was archived

| File | Reason |
|---|---|
| `pro.html` | Sole data source `pro-data.json` frozen since Feb 26 (~60 days stale at archive time) |
| `exponential-search-dashboard.html` | Uses dead OpenBB API Gateway `i70jxru6md.execute-api.us-east-1.amazonaws.com` (decommissioned) |
| `macroeconomic-platform.html` | Same dead OpenBB API Gateway dependency |

## Date archived
2026-04-26 — during Path E+F coverage gap closure.

## Restoration
If you ever want to restore a page, it's a `git mv archive/<file> ./<file>` away.
"""
    readme_path.write_text(readme_text)
    r.ok(f"  wrote {readme_path}")

    r.log("Done")
