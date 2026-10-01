#!/usr/bin/env python3
"""
Ops 1054 — production validation sweep for the ten Bloomberg-parity upgrades.

For each upgrade pushed to ElMooro/si on 2026-09-30 / 2026-10-01, this script
checks AWS-side reality in us-east-1:

  1. Lambda exists (get_function): LastModified, code size, timeout, memory.
  2. EventBridge schedule state: classic rule and/or Scheduler schedule found,
     enabled, expression matches the repo config.json expectation.
  3. CloudWatch metrics over a per-lambda window: invocation count, error count,
     last invocation time. Logs are pulled only for lambdas with errors.
  4. S3 artifact freshness via head_object: FRESH / STALE / MISSING against
     per-artifact freshness budgets.

It also validates the nine ops-1051 R-engine Scheduler schedules
(existence, enabled state, expression match, duplicate/unwanted detection).

Writes the structured report to s3://justhodl-dashboard-live/ops/reports/1054.json
and prints it. Never raises on a check failure — every failure is recorded.
No secrets are read, logged, or written.

Verdicts per upgrade:
  PASS     — lambda present, schedule healthy, invocations flowing, artifacts fresh.
  DEGRADED — present but with errors, stale artifacts, schedule mismatch,
             zero recent invocations, or code older than the upgrade commit.
  FAIL     — lambda missing, primary artifact missing, or scheduled lambda that
             has never been invoked and has no schedule at all.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
S3_BUCKET = "justhodl-dashboard-live"
REPORT_KEY = "ops/reports/1054.json"

# All upgrades 1-9 were pushed to main on 2026-09-30; 10/10 on 2026-10-01.
COMMIT_AFTER_0930 = "2026-09-30T00:00:00+00:00"
COMMIT_AFTER_1001 = "2026-10-01T00:00:00+00:00"


def _art(key, fresh_hours, primary=True):
    """Build an artifact expectation entry."""
    return {"key": key, "fresh_hours": fresh_hours, "primary": primary}


def _sched(kind, name=None, expression=None):
    """Build a schedule expectation.

    kind: "scheduler" (EventBridge Scheduler, name required),
          "rule" (classic EventBridge rule, name required),
          "auto" (discover: try scheduler name, rule name, then prefix scan).
    """
    return {"kind": kind, "name": name, "expression": expression}


# ---------------------------------------------------------------------------
# Checklist: one entry per Lambda touched by upgrades 1-10.
# invocation_window_hours: how far back an invocation must exist (per cadence).
# code_after: the upgrade's push date; older LastModified => possibly undeployed.
# ---------------------------------------------------------------------------
CHECKS = [
    {
        "upgrade": "1/10 FRED macro regime",
        "lambda": "justhodl-macro-regime",
        "expected_timeout": 300,
        "expected_memory": 1024,
        "schedule": _sched("rule", name="justhodl-macro-regime-daily",
                           expression="cron(15 22 * * ? *)"),
        "artifacts": [_art("macro/regime.json", 26)],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "2/10 SEC 8-K enrichment",
        "lambda": "justhodl-sec-8k-enrich",
        "expected_timeout": 300,
        "expected_memory": 512,
        "schedule": _sched("auto", expression="cron(10,40 * * * ? *)"),
        "artifacts": [
            _art("data/8k-filings-enriched.json", 2),
            _art("data/8k-by-ticker.json", 2),
        ],
        "invocation_window_hours": 3,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "3/10 FINRA short interest (producer)",
        "lambda": "justhodl-short-interest",
        "expected_timeout": 360,
        "expected_memory": 512,
        "schedule": _sched("rule", name="justhodl-short-interest-sched"),
        "artifacts": [_art("data/short-interest-tickers.json", 96)],
        "invocation_window_hours": 96,
        "code_after": COMMIT_AFTER_0930,
        "notes": "Legacy rules justhodl-short-interest-6h + justhodl-short-interest-sched retained per config.",
    },
    {
        "upgrade": "3/10 squeeze pre-trigger (consumer)",
        "lambda": "justhodl-squeeze-pretrigger",
        "expected_timeout": 540,
        "expected_memory": 768,
        "schedule": _sched("scheduler", name="justhodl-squeeze-pretrigger-daily",
                           expression="cron(30 23 * * ? *)"),
        "artifacts": [],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "3/10 microcap float squeeze (consumer)",
        "lambda": "justhodl-microcap-float-squeeze",
        "expected_timeout": None,
        "expected_memory": None,
        "schedule": _sched("auto"),
        "artifacts": [],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_0930,
        "notes": "No config.json in repo; schedule discovered by prefix scan.",
    },
    {
        "upgrade": "3/10 stock screener (consumer, on-demand)",
        "lambda": "justhodl-stock-screener",
        "expected_timeout": 900,
        "expected_memory": 1280,
        "schedule": None,
        "artifacts": [],
        "invocation_window_hours": 0,
        "code_after": COMMIT_AFTER_0930,
        "notes": "On-demand via function URL; no schedule expected. equity_enrich is a shared module (code-only).",
    },
    {
        "upgrade": "4/10 SEC XBRL fundamentals",
        "lambda": "justhodl-xbrl-fundamentals",
        "expected_timeout": 900,
        "expected_memory": 1024,
        "schedule": _sched("auto", expression="cron(0 6 ? * SUN *)"),
        "artifacts": [
            _art("data/xbrl-fundamentals-index.json", 24 * 8),
            _art("data/xbrl-fundamentals/_state.json", 24 * 8),
        ],
        "invocation_window_hours": 24 * 8,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "5/10 CBOE options chain",
        "lambda": "justhodl-cboe-options-chain",
        "expected_timeout": 300,
        "expected_memory": 1024,
        "schedule": _sched("auto", expression="cron(5,35 * * * ? *)"),
        "artifacts": [
            _art("data/cboe-options-chain.json", 2),
            _art("data/cboe-options-chain-history.json", 2, primary=False),
        ],
        "invocation_window_hours": 3,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "6/10 FINRA TRACE bonds",
        "lambda": "justhodl-bond-trace",
        "expected_timeout": 120,
        "expected_memory": 256,
        "schedule": _sched("rule", name="bond-trace-daily",
                           expression="cron(0 21 ? * MON-FRI *)"),
        "artifacts": [
            _art("data/bond-trace.json", 78),
            _art("data/trace-bond-prints.json", 78),
            _art("data/trace-bond-prints-history.json", 78, primary=False),
        ],
        "invocation_window_hours": 78,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "7/10 corporate actions",
        "lambda": "justhodl-corporate-actions",
        "expected_timeout": 300,
        "expected_memory": 256,
        "schedule": _sched("auto", expression="cron(30 6 ? * * *)"),
        "artifacts": [_art("data/corporate-actions-index.json", 30)],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "8/10 price redundancy (quote badges)",
        "lambda": "justhodl-price-redundancy",
        "expected_timeout": 300,
        "expected_memory": 768,
        "schedule": _sched("rule", name="justhodl-price-redundancy-15min"),
        "artifacts": [
            _art("data/price-redundancy.json", 1),
            _art("data/quote-health.json", 1),
        ],
        "invocation_window_hours": 2,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "8/10 market tape",
        "lambda": "justhodl-market-tape",
        "expected_timeout": 120,
        "expected_memory": 256,
        "schedule": _sched("auto"),
        "artifacts": [_art("data/market-tape.json", 24)],
        "invocation_window_hours": 24,
        "code_after": COMMIT_AFTER_0930,
        "notes": "No schedule in config.json; discovered by prefix scan.",
    },
    {
        "upgrade": "8/10 polygon daily snapshot",
        "lambda": "justhodl-polygon-daily-snapshot",
        "expected_timeout": 180,
        "expected_memory": 1024,
        "schedule": _sched("rule", name="justhodl-polygon-daily-2130"),
        "artifacts": [_art("data/warm/us-equities-daily/latest-summary.json", 30)],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_0930,
    },
    {
        "upgrade": "9/10 symbology master (OpenFIGI)",
        "lambda": "justhodl-symbology-master",
        "expected_timeout": 120,
        "expected_memory": 1024,
        "schedule": _sched("rule", name="justhodl-symbology-master-daily"),
        "artifacts": [
            _art("data/symbology/master.json", 30),
            _art("data/symbology/bond-cusips.json", 30, primary=False),
        ],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_0930,
        "notes": "bond-cusips.json is new in 9/10; may legitimately not exist until the TRACE Phase 2 queue fills.",
    },
    {
        "upgrade": "10/10 issuer ETF holdings",
        "lambda": "justhodl-etf-issuer-holdings",
        "expected_timeout": 600,
        "expected_memory": 512,
        "schedule": _sched("auto", expression="cron(0 4 * * ? *)"),
        "artifacts": [_art("data/etf-issuer-holdings.json", 30)],
        "invocation_window_hours": 30,
        "code_after": COMMIT_AFTER_1001,
        "notes": "Pushed 2026-10-01; first scheduled run 04:00 UTC daily. Zero invocations before first run is expected.",
    },
]

# ---------------------------------------------------------------------------
# Ops 1051: nine R-series engines and their expected Scheduler schedules.
# Schedule names in 1051 are f"justhodl-{engine}-schedule" (double justhodl).
# ---------------------------------------------------------------------------
R1051_ENGINES = {
    "justhodl-factor-decomposition": "cron(0 6 * * ? *)",
    "justhodl-fx-decomposition": "cron(15 6 * * ? *)",
    "justhodl-fedwatch-rate-probability": "cron(30 6 * * ? *)",
    "justhodl-cftc-deep-view": "cron(45 6 * * ? *)",
    "justhodl-sec-filing-diff": "cron(0 7 ? * MON *)",
    "justhodl-transcript-query": "cron(15 7 ? * MON *)",
    "justhodl-peer-comparison": "cron(30 7 ? * MON *)",
    "justhodl-screen-builder": "cron(45 7 ? * MON *)",
    "justhodl-supply-chain-linkage": "cron(0 8 ? * MON *)",
}

# ---------------------------------------------------------------------------
# Check helpers. Every helper returns a dict and never raises.
# ---------------------------------------------------------------------------

def _err(detail, exc):
    """Format an exception into a short, secret-free detail string."""
    return "%s: %s" % (detail, str(exc)[:200])


def check_lambda(lam, name):
    """Check a Lambda function exists; return config facts or not-found."""
    try:
        resp = lam.get_function(FunctionName=name)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("ResourceNotFoundException",):
            return {"exists": False}
        return {"exists": False, "error": _err("get_function", e)}
    except Exception as e:  # noqa: BLE001 - record, never raise
        return {"exists": False, "error": _err("get_function", e)}
    cfg = resp.get("Configuration", {})
    return {
        "exists": True,
        "last_modified": cfg.get("LastModified"),
        "code_size": cfg.get("CodeSize"),
        "timeout": cfg.get("Timeout"),
        "memory": cfg.get("MemorySize"),
        "runtime": cfg.get("Runtime"),
        "state": cfg.get("State"),
    }


def _scheduler_get(scheduler, name):
    """Fetch one EventBridge Scheduler schedule; None if missing."""
    try:
        return scheduler.get_schedule(Name=name)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return None
        raise
    except Exception:
        raise


def _rule_get(events, name):
    """Fetch one classic EventBridge rule; None if missing."""
    try:
        return events.describe_rule(Name=name)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
            return None
        raise
    except Exception:
        raise


def _rule_targets_lambda(events, rule_name, lambda_name):
    """Check whether a classic rule targets the given lambda. Never raises."""
    try:
        resp = events.list_targets_by_rule(Rule=rule_name)
        for t in resp.get("Targets", []):
            arn = t.get("Arn", "")
            if lambda_name in arn:
                return True
        return False
    except Exception:  # noqa: BLE001 - discovery is best-effort
        return False


def check_schedule(events, scheduler, spec, lambda_name):
    """Check schedule state for a lambda.

    Returns dict with found/mechanism/name/enabled/expression/match/target_ok.
    Discovery ("auto") scans classic rules and Scheduler schedules by prefix.
    """
    if spec is None:
        return {"expected": False, "note": "no schedule expected (on-demand)"}
    kind = spec.get("kind")
    name = spec.get("name")
    expected_expr = spec.get("expression")
    result = {"expected": True, "kind": kind, "name": name,
              "expected_expression": expected_expr}

    def _score(found, mechanism, sched_name, enabled, expression, target_ok):
        """Fill the result dict from one concrete schedule hit."""
        result.update({
            "found": True,
            "mechanism": mechanism,
            "resolved_name": sched_name,
            "enabled": enabled,
            "expression": expression,
            "target_ok": target_ok,
            "expression_match": (expected_expr is None or expression == expected_expr),
        })

    try:
        if kind == "scheduler" and name:
            s = _scheduler_get(scheduler, name)
            if s is None:
                result.update({"found": False})
            else:
                target = (s.get("Target") or {}).get("Arn", "")
                _score(True, "scheduler", name,
                       s.get("State") == "ENABLED",
                       s.get("ScheduleExpression"),
                       lambda_name in target)
            return result
        if kind == "rule" and name:
            r = _rule_get(events, name)
            if r is None:
                result.update({"found": False})
            else:
                _score(True, "rule", name,
                       r.get("State") == "ENABLED",
                       r.get("ScheduleExpression"),
                       _rule_targets_lambda(events, name, lambda_name))
            return result
        # auto discovery
        if name:
            s = _scheduler_get(scheduler, name)
            if s is not None:
                target = (s.get("Target") or {}).get("Arn", "")
                _score(True, "scheduler", name,
                       s.get("State") == "ENABLED",
                       s.get("ScheduleExpression"),
                       lambda_name in target)
                return result
            r = _rule_get(events, name)
            if r is not None:
                _score(True, "rule", name,
                       r.get("State") == "ENABLED",
                       r.get("ScheduleExpression"),
                       _rule_targets_lambda(events, name, lambda_name))
                return result
        # prefix scan across both mechanisms
        hits = []
        try:
            paginator = events.get_paginator("list_rules")
            for page in paginator.paginate(NamePrefix=lambda_name):
                for rule in page.get("Rules", []):
                    rn = rule.get("Name", "")
                    ok = _rule_targets_lambda(events, rn, lambda_name)
                    hits.append(("rule", rn, rule.get("State") == "ENABLED",
                                 rule.get("ScheduleExpression"), ok))
        except Exception:  # noqa: BLE001 - best-effort discovery
            pass
        try:
            paginator = scheduler.get_paginator("list_schedules")
            for page in paginator.paginate(NamePrefix=lambda_name):
                for sched in page.get("Schedules", []):
                    sn = sched.get("Name", "")
                    full = _scheduler_get(scheduler, sn) or {}
                    target = (full.get("Target") or {}).get("Arn", "")
                    hits.append(("scheduler", sn, full.get("State") == "ENABLED",
                                 full.get("ScheduleExpression"),
                                 lambda_name in target))
        except Exception:  # noqa: BLE001 - best-effort discovery
            pass
        targeted = [h for h in hits if h[4]]
        pool = targeted if targeted else hits
        if not pool:
            result.update({"found": False, "discovered": []})
        else:
            mech, sched_name, enabled, expression, target_ok = pool[0]
            _score(True, mech, sched_name, enabled, expression, target_ok)
            result["discovered"] = [
                {"mechanism": h[0], "name": h[1], "enabled": h[2],
                 "expression": h[3], "target_ok": h[4]} for h in pool
            ]
        return result
    except Exception as e:  # noqa: BLE001 - record, never raise
        result.update({"found": False, "error": _err("schedule check", e)})
        return result


def check_metrics(cw, lambda_name, window_hours):
    """CloudWatch Invocations/Errors over the window; last invocation time."""
    if not window_hours:
        return {"checked": False, "note": "no invocation window (on-demand)"}
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=window_hours)
    out = {"checked": True, "window_hours": window_hours,
           "invocations": None, "errors": None, "last_invocation": None}
    try:
        for metric, key in (("Invocations", "invocations"), ("Errors", "errors")):
            resp = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName=metric,
                Dimensions=[{"Name": "FunctionName", "Value": lambda_name}],
                StartTime=start,
                EndTime=end,
                Period=3600,
                Statistics=["Sum"],
            )
            dps = resp.get("Datapoints", [])
            out[key] = int(sum(d.get("Sum", 0) for d in dps))
            if metric == "Invocations":
                with_hits = [d for d in dps if d.get("Sum", 0) > 0]
                if with_hits:
                    latest = max(with_hits, key=lambda d: d["Timestamp"])
                    out["last_invocation"] = latest["Timestamp"].isoformat()
    except Exception as e:  # noqa: BLE001 - record, never raise
        out["error"] = _err("cloudwatch", e)
    return out


def check_error_logs(logs, lambda_name, metrics):
    """Pull recent ERROR log lines, only when errors were observed."""
    if not metrics.get("checked") or not metrics.get("errors"):
        return {"checked": False, "note": "no errors in window; logs skipped"}
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - 24 * 3600 * 1000
    try:
        resp = logs.filter_log_events(
            logGroupName="/aws/lambda/%s" % lambda_name,
            startTime=start_ms,
            endTime=end_ms,
            filterPattern="ERROR",
            limit=10,
        )
        lines = []
        for ev in resp.get("events", [])[:3]:
            msg = ev.get("message", "")
            lines.append(msg[:200].replace("\n", " "))
        return {"checked": True, "error_events": len(resp.get("events", [])),
                "sample": lines}
    except Exception as e:  # noqa: BLE001 - record, never raise
        return {"checked": True, "error": _err("logs", e)}


def check_artifact(s3, key, fresh_hours):
    """head_object on an S3 artifact; FRESH / STALE / MISSING."""
    try:
        resp = s3.head_object(Bucket=S3_BUCKET, Key=key)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey", "NotFound"):
            return {"key": key, "exists": False, "status": "MISSING"}
        return {"key": key, "exists": False, "status": "ERROR",
                "error": _err("head_object", e)}
    except Exception as e:  # noqa: BLE001 - record, never raise
        return {"key": key, "exists": False, "status": "ERROR",
                "error": _err("head_object", e)}
    lm = resp.get("LastModified")
    age_hours = None
    status = "UNKNOWN"
    if lm is not None:
        age_hours = (datetime.now(timezone.utc) - lm).total_seconds() / 3600.0
        status = "FRESH" if age_hours <= fresh_hours else "STALE"
    return {
        "key": key,
        "exists": True,
        "status": status,
        "last_modified": lm.isoformat() if lm else None,
        "age_hours": round(age_hours, 2) if age_hours is not None else None,
        "size": resp.get("ContentLength"),
    }

# ---------------------------------------------------------------------------
# Verdicts and the ops-1051 section
# ---------------------------------------------------------------------------

def _parse_dt(value):
    """Parse an ISO datetime string to aware datetime; None on failure."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:  # noqa: BLE001 - unparseable => None
        return None


def decide_verdict(entry, fn, sched, metrics, artifacts, code_after):
    """Decide PASS / DEGRADED / FAIL for one checklist entry."""
    raw_notes = entry.get("notes")
    notes = [raw_notes] if isinstance(raw_notes, str) else list(raw_notes or [])
    if not fn.get("exists"):
        return "FAIL", ["lambda not found in us-east-1"]

    # Config drift: timeout / memory vs repo config.json.
    exp_t, exp_m = entry.get("expected_timeout"), entry.get("expected_memory")
    if exp_t is not None and fn.get("timeout") != exp_t:
        notes.append("timeout %s != config.json %s" % (fn.get("timeout"), exp_t))
    if exp_m is not None and fn.get("memory") != exp_m:
        notes.append("memory %s != config.json %s" % (fn.get("memory"), exp_m))

    # Code freshness: LastModified older than the upgrade's push date.
    after = _parse_dt(code_after)
    lm = _parse_dt(fn.get("last_modified"))
    if after and lm and lm < after:
        notes.append("code LastModified %s predates upgrade commit %s "
                     "(deploy may not have picked up the upgrade)"
                     % (fn.get("last_modified"), code_after))

    sched_problem = False
    if sched.get("expected"):
        if not sched.get("found"):
            notes.append("expected schedule not found "
                         "(checked scheduler + classic rules)")
            sched_problem = True
        else:
            if not sched.get("enabled"):
                notes.append("schedule %s is DISABLED"
                             % sched.get("resolved_name"))
                sched_problem = True
            if not sched.get("target_ok"):
                notes.append("schedule %s does not target this lambda"
                             % sched.get("resolved_name"))
                sched_problem = True
            if sched.get("expected_expression") and not sched.get("expression_match"):
                notes.append("schedule expression %r != config.json %r"
                             % (sched.get("expression"),
                                sched.get("expected_expression")))
                sched_problem = True

    inv = metrics.get("invocations")
    window = entry.get("invocation_window_hours") or 0
    if metrics.get("checked") and inv == 0 and window:
        notes.append("zero invocations in last %sh" % window)

    errors = metrics.get("errors") or 0
    if errors:
        notes.append("%d error(s) in window" % errors)

    primary_missing = False
    for art_spec, art in zip(entry.get("artifacts", []), artifacts):
        if art.get("status") == "MISSING" and art_spec.get("primary", True):
            notes.append("primary artifact missing: %s" % art.get("key"))
            primary_missing = True
        elif art.get("status") == "STALE":
            notes.append("artifact stale (%.1fh old, budget %sh): %s"
                         % (art.get("age_hours") or -1,
                            art_spec.get("fresh_hours"), art.get("key")))
        elif art.get("status") == "MISSING":
            notes.append("secondary artifact missing (tolerated): %s"
                         % art.get("key"))

    # FAIL conditions: dead lambda or broken data pipeline.
    if primary_missing:
        return "FAIL", notes
    if sched_problem and (inv == 0) and window:
        return "FAIL", notes + ["no schedule and no invocations: engine is dead"]

    # DEGRADED: anything suboptimal but alive.
    degraded_markers = (
        sched_problem, errors > 0,
        any(a.get("status") == "STALE" for a in artifacts),
        (metrics.get("checked") and inv == 0 and window),
        any("predates upgrade commit" in n for n in notes),
        any("!= config.json" in n for n in notes),
    )
    if any(degraded_markers):
        return "DEGRADED", notes
    return "PASS", notes


def check_r1051(lam, scheduler):
    """Validate the nine ops-1051 R-engine Scheduler schedules."""
    engines = []
    for engine, expected_expr in R1051_ENGINES.items():
        sched_name = "justhodl-%s-schedule" % engine
        item = {"engine": engine, "schedule_name": sched_name,
                "expected_expression": expected_expr}
        fn = check_lambda(lam, engine)
        item["lambda_exists"] = bool(fn.get("exists"))
        try:
            s = _scheduler_get(scheduler, sched_name)
        except Exception as e:  # noqa: BLE001 - record, never raise
            item["error"] = _err("get_schedule", e)
            s = "ERROR"
        if s is None:
            item["schedule_found"] = False
            item["verdict"] = "FAIL"
        elif s == "ERROR":
            item["schedule_found"] = False
            item["verdict"] = "FAIL"
        else:
            target = (s.get("Target") or {}).get("Arn", "")
            expr = s.get("ScheduleExpression")
            enabled = s.get("State") == "ENABLED"
            item.update({
                "schedule_found": True,
                "enabled": enabled,
                "expression": expr,
                "expression_match": expr == expected_expr,
                "target_ok": engine in target,
            })
            ok = (item["lambda_exists"] and enabled
                  and item["expression_match"] and item["target_ok"])
            item["verdict"] = "PASS" if ok else "DEGRADED"
        engines.append(item)

    # Duplicate / unwanted detection: anything under the 1051 name prefix.
    extras, dup_targets = [], {}
    try:
        paginator = scheduler.get_paginator("list_schedules")
        seen_names = []
        for page in paginator.paginate(NamePrefix="justhodl-justhodl-"):
            for sched in page.get("Schedules", []):
                seen_names.append(sched.get("Name"))
        expected_names = {"justhodl-%s-schedule" % e for e in R1051_ENGINES}
        for n in seen_names:
            if n not in expected_names:
                extras.append(n)
        # Same-target duplicates among the nine.
        for engine in R1051_ENGINES:
            s = _scheduler_get(scheduler, "justhodl-%s-schedule" % engine)
            if s:
                tgt = (s.get("Target") or {}).get("Arn", "")
                dup_targets.setdefault(tgt, []).append(engine)
        duplicates = {t: v for t, v in dup_targets.items() if len(v) > 1}
    except Exception as e:  # noqa: BLE001 - record, never raise
        extras = [{"error": _err("list_schedules", e)}]
        duplicates = {}

    verdicts = [e["verdict"] for e in engines]
    summary = {
        "total": len(engines),
        "pass": verdicts.count("PASS"),
        "degraded": verdicts.count("DEGRADED"),
        "fail": verdicts.count("FAIL"),
        "unexpected_schedules_under_prefix": extras,
        "duplicate_targets": duplicates,
    }
    return {"engines": engines, "summary": summary}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Run the full validation sweep, write the S3 report, print it."""
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    scheduler = boto3.client("scheduler", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    report = {
        "ops": 1054,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "region": REGION,
        "account": ACCOUNT,
        "upgrades": [],
    }

    for entry in CHECKS:
        name = entry["lambda"]
        unit = {"upgrade": entry["upgrade"], "lambda": name}
        try:
            fn = check_lambda(lam, name)
            unit["function"] = fn
            sched = check_schedule(events, scheduler, entry.get("schedule"), name)
            unit["schedule"] = sched
            metrics = check_metrics(cw, name, entry.get("invocation_window_hours") or 0)
            unit["metrics"] = metrics
            unit["error_logs"] = check_error_logs(logs, name, metrics)
            arts = []
            for art_spec in entry.get("artifacts", []):
                arts.append(check_artifact(s3, art_spec["key"],
                                          art_spec["fresh_hours"]))
            unit["artifacts"] = arts
            verdict, notes = decide_verdict(entry, fn, sched, metrics, arts,
                                            entry.get("code_after"))
            unit["verdict"] = verdict
            unit["notes"] = notes
        except Exception as e:  # noqa: BLE001 - one bad unit never kills the sweep
            unit["verdict"] = "FAIL"
            unit["notes"] = ["sweep harness error: %s" % _err("unit", e)]
        report["upgrades"].append(unit)
        print("  %-45s %s" % (name, unit["verdict"]))

    print("Checking ops-1051 R-engine schedules...")
    try:
        report["r1051"] = check_r1051(lam, scheduler)
    except Exception as e:  # noqa: BLE001 - record, never raise
        report["r1051"] = {"error": _err("r1051", e), "engines": [],
                           "summary": {}}

    verdicts = [u["verdict"] for u in report["upgrades"]]
    r_verdicts = [e["verdict"] for e in report["r1051"].get("engines", [])]
    counts = {
        "upgrade_PASS": verdicts.count("PASS"),
        "upgrade_DEGRADED": verdicts.count("DEGRADED"),
        "upgrade_FAIL": verdicts.count("FAIL"),
        "r1051_PASS": r_verdicts.count("PASS"),
        "r1051_DEGRADED": r_verdicts.count("DEGRADED"),
        "r1051_FAIL": r_verdicts.count("FAIL"),
    }
    report["summary"] = counts
    if counts["upgrade_FAIL"] or counts["r1051_FAIL"]:
        report["overall"] = "FAIL"
    elif counts["upgrade_DEGRADED"] or counts["r1051_DEGRADED"]:
        report["overall"] = "DEGRADED"
    else:
        report["overall"] = "PASS"

    body = json.dumps(report, indent=2, default=str)
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=REPORT_KEY,
                      Body=body.encode("utf-8"),
                      ContentType="application/json")
        print("Report written to s3://%s/%s" % (S3_BUCKET, REPORT_KEY))
    except Exception as e:  # noqa: BLE001 - print anyway
        print("WARNING: S3 report write failed: %s" % _err("put_object", e))

    print(body)
    print("Overall: %s  %s" % (report["overall"], json.dumps(counts)))


if __name__ == "__main__":
    main()
