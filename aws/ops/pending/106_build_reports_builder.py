#!/usr/bin/env python3
"""
Step 106 — Build justhodl-reports-builder Lambda.

This Lambda reads SSM calibration data + DynamoDB signals/outcomes
and produces s3://justhodl-dashboard-live/reports/scorecard.json
which reports.html consumes.

Phase 1: probe schema (5 sample items from each DDB table)
Phase 2: write Lambda code based on actual schema
Phase 3: deploy + EB rule (rate(1 hour)) + initial invoke
Phase 4: verify output JSON in S3

Output keys produced:
  - signal_scorecard: list of {signal_type, total, correct, hit_rate,
    avg_magnitude_error, by_horizon{...}, trend_30d, trend_60d, trend_90d, weight}
  - khalid_timeline: list of {date, score, regime}
  - meta: {generated_at, signals_count, outcomes_count, has_calibration}
"""
import io
import json
import os
import time
import zipfile
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

ddb = boto3.resource("dynamodb", region_name=REGION)
ddb_client = boto3.client("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def d2f(o):
    """Decimal → float for JSON serialization."""
    if isinstance(o, Decimal): return float(o)
    if isinstance(o, dict):    return {k: d2f(v) for k, v in o.items()}
    if isinstance(o, list):    return [d2f(v) for v in o]
    return o


with report("build_reports_builder") as r:
    r.heading("Build justhodl-reports-builder Lambda + scorecard.json")

    # ─── Phase 1: probe DDB schemas ─────────────────────────────────────
    r.section("1. Probe DynamoDB schemas")
    for tname in ["justhodl-signals", "justhodl-outcomes"]:
        try:
            t = ddb.Table(tname)
            scan = t.scan(Limit=3)
            items = scan.get("Items", [])
            r.log(f"\n  {tname}: total~{t.item_count} items")
            r.log(f"  Sample item keys: {sorted(items[0].keys()) if items else 'empty'}")
            if items:
                r.log(f"  Sample item (first 2 fields):")
                sample = d2f(items[0])
                for k, v in list(sample.items())[:8]:
                    sval = json.dumps(v, default=str)[:100]
                    r.log(f"    {k}: {sval}")
        except Exception as e:
            r.warn(f"  Probe {tname} failed: {e}")

    # ─── Phase 2: read existing calibration data from SSM ──────────────
    r.section("2. Read existing SSM calibration data")
    calib = {}
    for key in ["/justhodl/calibration/weights", "/justhodl/calibration/accuracy",
                "/justhodl/calibration/report"]:
        try:
            resp = ssm.get_parameter(Name=key, WithDecryption=False)
            val = resp["Parameter"]["Value"]
            try:
                calib[key.split("/")[-1]] = json.loads(val)
                r.log(f"  {key}: parsed JSON, {len(val)}B")
            except json.JSONDecodeError:
                calib[key.split("/")[-1]] = val
                r.log(f"  {key}: raw string, {len(val)}B")
        except Exception as e:
            r.log(f"  {key}: not found / {e}")
            calib[key.split("/")[-1]] = None

    if calib.get("weights"):
        sample_w = list(calib["weights"].items())[:3] if isinstance(calib["weights"], dict) else None
        r.log(f"  Sample weights: {sample_w}")
    if calib.get("accuracy"):
        sample_a = list(calib["accuracy"].items())[:3] if isinstance(calib["accuracy"], dict) else None
        r.log(f"  Sample accuracy: {sample_a}")

    # ─── Phase 3: write Lambda code ─────────────────────────────────────
    r.section("3. Write Lambda code")
    lambda_code = r'''"""
justhodl-reports-builder

Reads SSM calibration + DynamoDB signals/outcomes, computes per-signal
scorecard + Khalid Index timeline, writes scorecard.json to S3 for
reports.html to consume.

Schedule: rate(1 hour) — calibration weights only update weekly but
hourly keeps Khalid timeline fresh.
"""
import boto3
import json
import os
from collections import defaultdict, OrderedDict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

S3_BUCKET = "justhodl-dashboard-live"
SCORECARD_KEY = "reports/scorecard.json"

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def d2f(o):
    if isinstance(o, Decimal): return float(o)
    if isinstance(o, dict):    return {k: d2f(v) for k, v in o.items()}
    if isinstance(o, list):    return [d2f(v) for v in o]
    return o


def get_ssm_json(name):
    try:
        v = ssm.get_parameter(Name=name)["Parameter"]["Value"]
        return json.loads(v)
    except Exception:
        return None


def scan_table(table_name, since_iso=None, max_items=10000):
    """Full scan of a table, optionally filtered by logged_at >= since_iso."""
    t = dynamodb.Table(table_name)
    items = []
    kwargs = {}
    while True:
        resp = t.scan(**kwargs)
        for item in resp.get("Items", []):
            items.append(d2f(item))
            if len(items) >= max_items:
                return items
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
    return items


def signal_type_of(s):
    """Pull the signal_type/source from a signal item."""
    return s.get("signal_type") or s.get("source") or s.get("signal") or "unknown"


def parse_iso(s):
    if not s: return None
    try:
        # Strip trailing Z and parse
        s = str(s).replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except Exception:
        return None


def compute_scorecard(signals, outcomes):
    """Group outcomes by signal_type and compute metrics."""
    # Build signal_id -> signal map
    sig_by_id = {s.get("signal_id"): s for s in signals if s.get("signal_id")}

    # Group outcomes by signal_type
    by_type = defaultdict(list)
    for o in outcomes:
        sid = o.get("signal_id")
        sig = sig_by_id.get(sid)
        if not sig:
            continue
        st = signal_type_of(sig)
        by_type[st].append({**o, "signal": sig})

    now = datetime.now(timezone.utc)
    scorecard = []
    for st, items in by_type.items():
        total = len(items)
        correct = sum(1 for i in items if i.get("correct") is True)
        hit_rate = (correct / total) if total else 0.0

        # Magnitude error
        mag_errors = []
        for i in items:
            pred = i.get("signal", {}).get("predicted_magnitude_pct")
            actual = i.get("actual_change_pct") or i.get("actual_pct")
            if pred is not None and actual is not None:
                try:
                    mag_errors.append(abs(float(pred) - float(actual)))
                except Exception:
                    pass
        avg_mag_err = (sum(mag_errors) / len(mag_errors)) if mag_errors else None

        # By horizon
        by_horizon = defaultdict(lambda: {"total": 0, "correct": 0})
        for i in items:
            h = i.get("horizon_days") or i.get("signal", {}).get("horizon_days_primary")
            try: h = int(h) if h is not None else None
            except Exception: h = None
            if h is None:
                continue
            by_horizon[h]["total"] += 1
            if i.get("correct") is True:
                by_horizon[h]["correct"] += 1
        for h in by_horizon:
            t_ = by_horizon[h]["total"]
            by_horizon[h]["hit_rate"] = (by_horizon[h]["correct"] / t_) if t_ else 0.0

        # Trend over time windows (using scored_at)
        def window_hit_rate(days):
            cutoff = now - timedelta(days=days)
            in_window = [i for i in items
                         if (parse_iso(i.get("scored_at") or i.get("logged_at"))
                             or datetime(1970, 1, 1, tzinfo=timezone.utc)) >= cutoff]
            if not in_window: return None
            c = sum(1 for i in in_window if i.get("correct") is True)
            return c / len(in_window) if in_window else None

        scorecard.append({
            "signal_type": st,
            "total": total,
            "correct": correct,
            "hit_rate": round(hit_rate, 4),
            "avg_magnitude_error_pct": round(avg_mag_err, 3) if avg_mag_err is not None else None,
            "by_horizon": dict(by_horizon),
            "trend_30d": window_hit_rate(30),
            "trend_60d": window_hit_rate(60),
            "trend_90d": window_hit_rate(90),
        })

    # Sort by sample size desc
    scorecard.sort(key=lambda x: -x["total"])
    return scorecard


def compute_khalid_timeline(signals):
    """Extract daily Khalid Index values from logged signals."""
    # Filter signals with khalid_score_at_log
    points = []
    for s in signals:
        score = s.get("khalid_score_at_log") or s.get("khalid_score")
        regime = s.get("regime_at_log") or s.get("regime")
        ts = s.get("logged_at")
        if score is None or not ts:
            continue
        try:
            dt = parse_iso(ts)
            if not dt:
                continue
            points.append({
                "date": dt.date().isoformat(),
                "ts": dt.isoformat(),
                "score": float(score),
                "regime": regime,
            })
        except Exception:
            pass

    # Group by date, take first reading of each day
    by_date = OrderedDict()
    for p in sorted(points, key=lambda x: x["ts"]):
        if p["date"] not in by_date:
            by_date[p["date"]] = p

    timeline = list(by_date.values())
    # Trim to last 90 days
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).date().isoformat()
    return [p for p in timeline if p["date"] >= cutoff]


def lambda_handler(event, context):
    """Build scorecard.json from SSM + DynamoDB and write to S3."""
    # 1. SSM calibration data
    weights = get_ssm_json("/justhodl/calibration/weights") or {}
    accuracy = get_ssm_json("/justhodl/calibration/accuracy") or {}
    calib_report = get_ssm_json("/justhodl/calibration/report") or {}

    # 2. DDB scans
    signals = scan_table("justhodl-signals", max_items=15000)
    outcomes = scan_table("justhodl-outcomes", max_items=15000)
    print(f"signals={len(signals)} outcomes={len(outcomes)}")

    # 3. Compute scorecard
    scorecard = compute_scorecard(signals, outcomes)
    # Merge calibrator weight + accuracy if available
    for row in scorecard:
        st = row["signal_type"]
        if isinstance(weights, dict) and st in weights:
            row["calibrator_weight"] = weights[st]
        if isinstance(accuracy, dict) and st in accuracy:
            row["calibrator_accuracy"] = accuracy[st]

    # 4. Khalid timeline
    timeline = compute_khalid_timeline(signals)

    # 5. Build output
    out = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "signals_total": len(signals),
            "outcomes_total": len(outcomes),
            "scored_outcomes": sum(1 for o in outcomes if o.get("correct") is not None),
            "has_calibration": bool(weights and accuracy),
            "calibration_summary": {
                "weights_count": len(weights) if isinstance(weights, dict) else 0,
                "accuracy_count": len(accuracy) if isinstance(accuracy, dict) else 0,
                "report_keys": list(calib_report.keys()) if isinstance(calib_report, dict) else [],
            },
        },
        "signal_scorecard": scorecard,
        "khalid_timeline": timeline,
        "calibration_weights": weights if isinstance(weights, dict) else {},
        "calibration_accuracy": accuracy if isinstance(accuracy, dict) else {},
    }

    # 6. Write to S3
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=SCORECARD_KEY,
        Body=json.dumps(out, default=str).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=300",
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "scorecard_rows": len(scorecard),
            "timeline_points": len(timeline),
            "signals_seen": len(signals),
            "outcomes_seen": len(outcomes),
        }),
    }
'''

    src_dir = REPO_ROOT / "aws/lambdas/justhodl-reports-builder/source"
    src_dir.mkdir(parents=True, exist_ok=True)
    (src_dir / "lambda_function.py").write_text(lambda_code)
    r.ok(f"  Wrote: aws/lambdas/justhodl-reports-builder/source/lambda_function.py "
         f"({lambda_code.count(chr(10))} LOC)")

    # Validate syntax
    import ast
    try:
        ast.parse(lambda_code)
        r.ok("  Syntax OK")
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    # config.json (Lambda metadata for CI/CD pattern)
    config = {
        "FunctionName": "justhodl-reports-builder",
        "Runtime": "python3.12",
        "Handler": "lambda_function.lambda_handler",
        "MemorySize": 512,
        "Timeout": 120,
        "Description": "Builds reports/scorecard.json from SSM calibration + DDB signals/outcomes",
        "Role": "arn:aws:iam::857687956942:role/lambda-execution-role",
    }
    (src_dir.parent / "config.json").write_text(json.dumps(config, indent=2))

    # ─── Phase 4: deploy ────────────────────────────────────────────────
    r.section("4. Deploy Lambda")
    name = "justhodl-reports-builder"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        info = zipfile.ZipInfo("lambda_function.py")
        info.external_attr = 0o644 << 16
        zout.writestr(info, lambda_code)
    zbytes = buf.getvalue()

    # Check if exists
    try:
        lam.get_function_configuration(FunctionName=name)
        # Update existing
        lam.update_function_code(FunctionName=name, ZipFile=zbytes)
        lam.get_waiter("function_updated").wait(FunctionName=name,
                                                WaiterConfig={"Delay": 3, "MaxAttempts": 30})
        r.ok(f"  Updated existing Lambda code ({len(zbytes)}B)")
    except lam.exceptions.ResourceNotFoundException:
        # Create new
        lam.create_function(
            FunctionName=name,
            Runtime=config["Runtime"],
            Role=config["Role"],
            Handler=config["Handler"],
            Code={"ZipFile": zbytes},
            Description=config["Description"],
            MemorySize=config["MemorySize"],
            Timeout=config["Timeout"],
        )
        lam.get_waiter("function_active").wait(FunctionName=name,
                                               WaiterConfig={"Delay": 3, "MaxAttempts": 30})
        r.ok(f"  Created new Lambda ({len(zbytes)}B)")

    # Update config (memory/timeout) in case it pre-existed with different settings
    try:
        lam.update_function_configuration(
            FunctionName=name,
            MemorySize=config["MemorySize"],
            Timeout=config["Timeout"],
            Description=config["Description"],
        )
        lam.get_waiter("function_updated").wait(FunctionName=name,
                                                WaiterConfig={"Delay": 3, "MaxAttempts": 30})
    except Exception as e:
        r.warn(f"  config update: {e}")

    # ─── Phase 5: EB rule ───────────────────────────────────────────────
    r.section("5. EventBridge schedule")
    rule_name = "justhodl-reports-builder-hourly"
    try:
        eb.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(1 hour)",
            State="ENABLED",
            Description="Hourly: rebuild reports/scorecard.json",
        )
        r.ok(f"  put_rule {rule_name}")
    except Exception as e:
        r.fail(f"  put_rule: {e}")

    target_arn = f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:{name}"
    try:
        eb.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1", "Arn": target_arn}],
        )
        r.ok(f"  put_targets done")
    except Exception as e:
        r.fail(f"  put_targets: {e}")

    # Permission for EB to invoke
    try:
        lam.add_permission(
            FunctionName=name,
            StatementId=f"eb-{rule_name}",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:{ACCOUNT}:rule/{rule_name}",
        )
        r.ok("  add_permission for EB invoke")
    except lam.exceptions.ResourceConflictException:
        r.log("  permission already exists")
    except Exception as e:
        r.warn(f"  add_permission: {e}")

    # ─── Phase 6: initial invoke ────────────────────────────────────────
    r.section("6. Initial invoke")
    time.sleep(3)
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
        if resp.get("FunctionError"):
            payload = resp.get("Payload").read().decode()
            r.fail(f"  FunctionError: {payload[:500]}")
        else:
            payload = json.loads(resp.get("Payload").read().decode())
            body = json.loads(payload.get("body", "{}"))
            r.ok(f"  Invoked: {body}")
    except Exception as e:
        r.fail(f"  invoke: {e}")

    # ─── Phase 7: verify S3 file ────────────────────────────────────────
    r.section("7. Verify scorecard.json in S3")
    try:
        head = s3.head_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
        r.ok(f"  scorecard.json: {head['ContentLength']:,}B  modified {head['LastModified']}")
        # Read first 500 bytes to confirm valid JSON
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="reports/scorecard.json")
        body = obj["Body"].read().decode("utf-8")
        data = json.loads(body)
        meta = data.get("meta", {})
        r.log(f"  meta: signals={meta.get('signals_total')} outcomes={meta.get('outcomes_total')} "
              f"scored={meta.get('scored_outcomes')}")
        r.log(f"  scorecard rows: {len(data.get('signal_scorecard', []))}")
        r.log(f"  timeline points: {len(data.get('khalid_timeline', []))}")
        if data.get("signal_scorecard"):
            top = data["signal_scorecard"][0]
            r.log(f"  Top signal by sample size: {top.get('signal_type')} "
                  f"({top.get('total')} predictions, {top.get('hit_rate', 0)*100:.0f}% hit rate)")
    except Exception as e:
        r.fail(f"  S3 verification: {e}")

    r.kv(
        lambda_deployed=name,
        eb_rule=rule_name,
        scorecard_key="reports/scorecard.json",
    )
    r.log("Done")
