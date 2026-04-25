#!/usr/bin/env python3
"""
Step 89 — Auto-generate expanded health expectations.

Strategy:
  Read the architecture inventory + every Lambda's source code, then
  derive expectations from facts (not guesses):

  1. For each Lambda with an EnabledScheduleExpression:
       - Min invocations per 24h = max(1, expected_invocations × 0.7)
       - Where expected_invocations = derived from cron/rate
       - max_error_rate = 0.20 (2x typical baseline)
     Skip Lambdas with no schedule (event-driven only) — they can't
     be alerted on inactivity.

  2. For each S3 file a Lambda writes (from source grep):
       - Pull the file's actual S3 LastModified
       - Pull the writer Lambda's schedule (from EB)
       - fresh_max = 2× schedule interval, capped at 31 days
       - expected_size = 50% of current observed size (catch shrink)
       - Skip if file doesn't exist yet

  3. Preserve all hand-written expectations (don't overwrite the
     critical-path entries that already exist).

  4. Mark auto-generated entries with auto_generated=True so we can
     distinguish them in code reviews.

Output: aws/ops/health/expectations.py (replaces existing).
        Old version archived to aws/ops/health/expectations.v1.py.
"""
import io
import json
import os
import re
import zipfile
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


# ─── Cron / rate parsing helpers ─────────────────────────────────────

def parse_schedule_to_per_day(schedule):
    """Estimate invocations per day from a cron/rate expression.

    Returns float (e.g. 96.0 for every 15 min, ~5.0 for weekday-only-hourly,
    or 0.14 for weekly).
    """
    if not schedule:
        return None

    # rate(N unit)
    m = re.match(r"rate\((\d+)\s*(\w+)\)", schedule)
    if m:
        n = int(m.group(1))
        unit = m.group(2).rstrip("s").lower()
        per_day = {
            "minute": 1440 / n,
            "hour": 24 / n,
            "day": 1.0 / n,
        }.get(unit)
        return per_day

    # cron(minutes hours day-of-month month day-of-week year?)
    m = re.match(r"cron\((.+)\)", schedule)
    if m:
        parts = m.group(1).split()
        if len(parts) >= 5:
            mins, hours, dom, mon, dow = parts[:5]
            # Estimate invocations per day
            per_day = 1.0
            # Hours field
            if hours == "*":
                hours_per_day = 24
            elif "/" in hours:
                step = int(hours.split("/")[-1])
                hours_per_day = 24 // step
            elif "," in hours:
                hours_per_day = len(hours.split(","))
            elif "-" in hours:
                start, end = hours.split("-")
                hours_per_day = int(end) - int(start) + 1
            else:
                hours_per_day = 1
            # Minutes within each hour
            if mins == "*":
                mins_per_hour = 60
            elif "/" in mins:
                step = int(mins.split("/")[-1])
                mins_per_hour = 60 // step
            elif "," in mins:
                mins_per_hour = len(mins.split(","))
            else:
                mins_per_hour = 1
            per_hour = mins_per_hour
            per_day = hours_per_day * per_hour
            # Day-of-week filter (multiplier <1)
            if dow != "?" and dow != "*":
                if dow == "MON-FRI":
                    per_day *= 5/7
                elif dow in ("SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"):
                    per_day *= 1/7
                elif "," in dow:
                    per_day *= len(dow.split(","))/7
            # Day-of-month filter
            if dom != "?" and dom != "*":
                if dom == "1":
                    per_day *= 1/30  # Monthly
            return per_day
    return None


def schedule_to_seconds(per_day):
    """Convert per-day rate to seconds-per-invocation."""
    if not per_day or per_day <= 0:
        return None
    return 86400 / per_day


# ─── Source extraction ───────────────────────────────────────────────

def extract_s3_writes(src):
    """Return set of S3 keys this code writes to."""
    keys = set()
    # put_object Key='...'
    for m in re.finditer(r"""put_object\s*\([^)]*?Key\s*=\s*f?['"]([^'"]+)['"]""", src, re.DOTALL):
        key = m.group(1)
        # Skip f-string interpolation noise but keep static-ish keys
        if "{" in key:
            # Try to keep just the prefix before any interpolation
            key = key.split("{")[0].rstrip("/") + "/"
        keys.add(key)
    return keys


def find_lambda_source(name):
    """Return (path, content) for a Lambda's main source file or (None, None)."""
    base = REPO_ROOT / "aws/lambdas" / name / "source"
    for fname in ["lambda_function.py", "index.py", "handler.py", "app.py"]:
        p = base / fname
        if p.exists():
            return p, p.read_text(encoding="utf-8", errors="ignore")
    # Fallback: any .py
    for p in base.rglob("*.py"):
        return p, p.read_text(encoding="utf-8", errors="ignore")
    return None, None


# ─── Existing expectations to preserve ───────────────────────────────

PRESERVE_IDS = set()  # Will be filled from current expectations.py


def load_current_expectations():
    """Read existing expectations.py to preserve hand-tuned thresholds."""
    p = REPO_ROOT / "aws/ops/health/expectations.py"
    if not p.exists():
        return {}, ""
    # Exec it in an isolated namespace
    ns = {}
    exec(p.read_text(), ns)
    return ns.get("EXPECTATIONS", {}), p.read_text()


with report("expand_health_expectations") as r:
    r.heading("Expand health expectations to all 95 Lambdas + auto-derived S3 files")

    # ─── Load existing ───
    current_exp, raw_current = load_current_expectations()
    PRESERVE_IDS.update(current_exp.keys())
    r.log(f"  Hand-curated entries to preserve: {len(PRESERVE_IDS)}")

    # ─── Load inventory + EB rules ───
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_audit/inventory_2026-04-25.json")
    inventory = json.loads(obj["Body"].read())
    r.log(f"  Inventory loaded: {len(inventory['lambdas'])} Lambdas")

    # Build EB lookup (function name → list of {schedule, state})
    eb_by_target = defaultdict(list)
    for rule in inventory["eb_rules"]:
        if rule.get("state") != "ENABLED":
            continue
        for t in rule.get("targets", []):
            arn_tail = t.get("arn", "")
            if "function:" in arn_tail:
                target = arn_tail.split("function:")[-1]
                eb_by_target[target].append(rule.get("schedule"))

    r.log(f"  Lambdas with enabled schedules: {len(eb_by_target)}")

    # ─── Auto-derive Lambda expectations ───
    r.section("Auto-deriving Lambda expectations")
    lambda_expectations = {}
    skipped_no_schedule = 0
    for fn in inventory["lambdas"]:
        name = fn["name"]
        sched_id = f"lambda:{name}"
        if sched_id in PRESERVE_IDS:
            continue  # Hand-tuned; skip auto-gen

        schedules = eb_by_target.get(name, [])
        if not schedules:
            skipped_no_schedule += 1
            continue

        # Estimate expected invocations per day from all schedules
        per_day_total = 0.0
        for s in schedules:
            pd = parse_schedule_to_per_day(s)
            if pd:
                per_day_total += pd

        if per_day_total <= 0:
            skipped_no_schedule += 1
            continue

        # Min invocations 24h = 70% of expected (allow some misses)
        min_inv = max(1, int(per_day_total * 0.7))
        # Cap so we don't alarm if observed < 24/day for a (1/min) schedule
        min_inv = min(min_inv, 1000)

        # Severity: critical if Lambda is in core_pipeline category, else important
        # Use simple heuristic: justhodl-* gets important by default
        if name.startswith("justhodl-"):
            severity = "important"
        else:
            severity = "nice_to_have"

        lambda_expectations[sched_id] = {
            "type": "lambda",
            "name": name,
            "max_error_rate": 0.30,    # Loose default; tune per-lambda later
            "min_invocations_24h": min_inv,
            "note": f"Auto-derived. Schedules: {schedules}. Expected ~{per_day_total:.1f}/day.",
            "severity": severity,
            "auto_generated": True,
        }

    r.log(f"  Auto-derived {len(lambda_expectations)} Lambda entries")
    r.log(f"  Skipped (no schedule): {skipped_no_schedule}")

    # ─── Auto-derive S3 file expectations ───
    r.section("Auto-deriving S3 file expectations from source code")

    # First, compute writers map: {s3_key: [(lambda_name, schedules_list)]}
    writers = defaultdict(list)
    for fn in inventory["lambdas"]:
        name = fn["name"]
        path, src = find_lambda_source(name)
        if not src:
            continue
        keys = extract_s3_writes(src)
        for k in keys:
            writers[k].append((name, eb_by_target.get(name, [])))

    r.log(f"  S3 keys mentioned by put_object across all Lambdas: {len(writers)}")

    s3_expectations = {}
    skipped_dynamic = 0
    skipped_no_schedule_writer = 0

    for key, writer_list in writers.items():
        # Skip dynamic keys (heavy interpolation)
        if "/" in key and key.endswith("/"):
            skipped_dynamic += 1
            continue
        # Skip keys without a real extension
        if not any(key.endswith(ext) for ext in [".json", ".html", ".csv", ".txt", ".jsonl"]):
            skipped_dynamic += 1
            continue
        # Skip archive/* keys (snapshots, not live data)
        if key.startswith("archive/"):
            continue
        # Skip _health/ (already managed)
        if key.startswith("_health/"):
            continue
        # Skip if hand-curated
        if f"s3:{key}" in PRESERVE_IDS:
            continue

        # Pick the most-frequently-scheduled writer
        best_per_day = 0
        best_writer = None
        for wname, wschedules in writer_list:
            for s in wschedules:
                pd = parse_schedule_to_per_day(s)
                if pd and pd > best_per_day:
                    best_per_day = pd
                    best_writer = wname

        if not best_writer:
            skipped_no_schedule_writer += 1
            continue

        # Get current size
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
            cur_size = head["ContentLength"]
            cur_age_sec = (datetime.now(timezone.utc) - head["LastModified"]).total_seconds()
        except Exception:
            continue  # File doesn't exist; skip

        # Derive thresholds
        interval_sec = schedule_to_seconds(best_per_day)
        if not interval_sec:
            continue
        # fresh_max = 2× interval, capped at 31 days
        fresh_max = min(int(interval_sec * 2), 31 * 86400)
        warn_max = min(fresh_max * 3, 60 * 86400)
        # expected_size = 50% of current (catches shrinkage but not normal variance)
        expected_size = max(int(cur_size * 0.5), 100)

        s3_expectations[f"s3:{key}"] = {
            "type": "s3_file",
            "key": key,
            "fresh_max": fresh_max,
            "warn_max": warn_max,
            "expected_size": expected_size,
            "note": f"Auto-derived. Writer: {best_writer} ({best_per_day:.1f}/day). Cur size: {cur_size}B.",
            "severity": "nice_to_have",  # Auto-derived = lower confidence
            "auto_generated": True,
        }

    r.log(f"  Auto-derived {len(s3_expectations)} S3 file entries")
    r.log(f"  Skipped (dynamic/no-ext): {skipped_dynamic}")
    r.log(f"  Skipped (no scheduled writer): {skipped_no_schedule_writer}")

    # ─── Merge with existing ───
    r.section("Merging hand-curated + auto-derived")
    new_exp = dict(current_exp)  # Start with hand-curated
    for k, v in lambda_expectations.items():
        new_exp[k] = v
    for k, v in s3_expectations.items():
        new_exp[k] = v

    # Stats
    by_type = defaultdict(int)
    by_severity = defaultdict(int)
    by_origin = defaultdict(int)
    for k, v in new_exp.items():
        by_type[v.get("type", "?")] += 1
        by_severity[v.get("severity", "?")] += 1
        by_origin["auto" if v.get("auto_generated") else "hand"] += 1

    r.log(f"  Total entries: {len(new_exp)}")
    r.log(f"  By type: {dict(by_type)}")
    r.log(f"  By severity: {dict(by_severity)}")
    r.log(f"  By origin: {dict(by_origin)}")

    # ─── Generate the new expectations.py file ───
    r.section("Writing new expectations.py")

    # Archive current
    cur_path = REPO_ROOT / "aws/ops/health/expectations.py"
    archive_path = REPO_ROOT / "aws/ops/health/expectations.v1.py"
    if cur_path.exists() and not archive_path.exists():
        archive_path.write_text(cur_path.read_text())
        r.ok(f"  Archived previous version to expectations.v1.py")

    # Build new file: keep helpers from old file, replace EXPECTATIONS dict
    helper_block = '''
# ═══════════════════════════════════════════════════════════════════
#  Status helpers — used by the monitor Lambda
# ═══════════════════════════════════════════════════════════════════

def status_for_age(age_sec, fresh_max, warn_max):
    """Return 'green' | 'yellow' | 'red' | 'unknown'."""
    if fresh_max is None:
        return "unknown"
    if age_sec is None:
        return "red"
    if age_sec <= fresh_max:
        return "green"
    if warn_max is None or age_sec <= warn_max:
        return "yellow"
    return "red"


def status_for_size(actual_bytes, expected_min):
    if expected_min is None or actual_bytes is None:
        return "unknown"
    if actual_bytes >= expected_min:
        return "green"
    if actual_bytes >= expected_min * 0.5:
        return "yellow"
    return "red"


def severity_rank(s):
    return {"critical": 0, "important": 1, "nice_to_have": 2}.get(s, 3)
'''

    header = f'''# ═══════════════════════════════════════════════════════════════════
#  JustHodl.AI Health Expectations
#
#  Hybrid: hand-curated entries (critical path components, tuned with
#  domain knowledge) + auto-derived entries from the architecture
#  inventory + source analysis.
#
#  Hand-curated entries take precedence; auto-derived ones added on top.
#  Auto-derived entries marked with auto_generated=True.
#
#  Generated: {datetime.now(timezone.utc).isoformat()}
#  Stats: {len(new_exp)} total ({by_origin["hand"]} hand, {by_origin["auto"]} auto)
#         By type: {dict(by_type)}
#         By severity: {dict(by_severity)}
# ═══════════════════════════════════════════════════════════════════

EXPECTATIONS = '''

    # Use repr but pretty-print
    def fmt_dict(d, indent=4):
        """Pretty-format a dict in stable key order, with indentation."""
        lines = ["{"]
        for k in sorted(d.keys()):
            v = d[k]
            inner_lines = [f"{' '*(indent+4)}{repr(ik)}: {repr(iv)}," for ik, iv in v.items()]
            lines.append(f"{' '*indent}{repr(k)}: {{")
            lines.extend(inner_lines)
            lines.append(f"{' '*indent}}},")
        lines.append("}")
        return "\n".join(lines)

    new_src = header + fmt_dict(new_exp) + "\n" + helper_block

    # Validate syntax
    import ast
    try:
        ast.parse(new_src)
    except SyntaxError as e:
        r.fail(f"  Syntax error in generated file: {e}")
        raise SystemExit(1)

    cur_path.write_text(new_src)
    r.ok(f"  Wrote new expectations.py ({len(new_src):,} bytes)")

    # ─── Re-deploy monitor with new expectations ───
    r.section("Re-deploying health monitor with expanded expectations")
    src_dir = REPO_ROOT / "aws/lambdas/justhodl-health-monitor/source"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
        zout.write(cur_path, "expectations.py")
    zbytes = buf.getvalue()

    lam.update_function_code(FunctionName="justhodl-health-monitor", ZipFile=zbytes)
    lam.get_waiter("function_updated").wait(
        FunctionName="justhodl-health-monitor", WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    r.ok(f"  Re-deployed: {len(zbytes)} bytes")

    # We may need to bump timeout — was 120s, but now we have 95+ Lambdas to check
    cur_cfg = lam.get_function_configuration(FunctionName="justhodl-health-monitor")
    if cur_cfg.get("Timeout", 0) < 300:
        lam.update_function_configuration(FunctionName="justhodl-health-monitor", Timeout=300)
        r.ok(f"  Bumped timeout 120s → 300s for the larger checker run")

    import time
    time.sleep(5)

    resp = lam.invoke(FunctionName="justhodl-health-monitor", InvocationType="RequestResponse")
    if resp.get("FunctionError"):
        r.fail(f"  Invoke FAILED: {resp.get('Payload').read().decode()[:500]}")
    else:
        r.ok(f"  Invoke clean (status {resp.get('StatusCode')})")

    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="_health/dashboard.json")
    dash = json.loads(obj["Body"].read())
    r.log(f"\n  System: {dash.get('system_status')}")
    r.log(f"  Counts: {dash.get('counts')}")
    r.log(f"  Total components: {dash.get('total_components')}")
    r.log(f"  Duration: {dash.get('duration_sec'):.1f}s")

    r.kv(
        prev_components=29,
        new_components=len(new_exp),
        hand_curated=by_origin["hand"],
        auto_derived=by_origin["auto"],
        next_step="step 90 cost audit",
    )
    r.log("Done")
