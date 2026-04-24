#!/usr/bin/env python3
"""
WEEK 2A — Predictions schema migration in signal-logger.

Per locked decisions doc (aws/ops/design/2026-04-25-decisions-locked.md):

NEW FIELDS on every signal item (additive — old items unaffected):
  schema_version            "2"               (bump from implicit v1)
  predicted_magnitude_pct   float | None      Q1.1 — optional, callers pass when natural
  predicted_target_price    Decimal | None    Q1.2 — computed: baseline × (1 + magnitude/100)
  horizon_days_primary      int               longest window in days
  regime_at_log             str | None        snapshot of Khalid regime at log-time
  khalid_score_at_log       int | None        snapshot of Khalid Index score
  rationale                 str | None        human-readable why
  supporting_signals        list | None       related signal_ids that agree

CHANGES TO log_sig() signature:
  - Added kwargs: magnitude=None, target_price=None, rationale=None, supporting=None
  - All optional, all default to None — existing call sites work unchanged
  - Computes predicted_target_price from baseline × magnitude if both present

NEW lambda_handler bootstrap:
  - Reads data/report.json ONCE up front to capture khalid_index + regime
  - Stores in module-level _REGIME_SNAPSHOT dict
  - log_sig() reads from snapshot for every new item

NO BACKWARDS-INCOMPATIBLE CHANGES:
  - All new fields are NULLABLE
  - Old DynamoDB items remain valid (DynamoDB is schemaless)
  - Future consumers check schema_version field — None = v1, "2" = new shape
"""
import io
import os
import zipfile
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
REPO_ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))

lam = boto3.client("lambda", region_name=REGION)


def build_zip(src_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zout:
        for f in src_dir.rglob("*"):
            if f.is_file():
                zout.write(f, str(f.relative_to(src_dir)))
    return buf.getvalue()


def deploy(fn_name, src_dir):
    z = build_zip(src_dir)
    lam.update_function_code(FunctionName=fn_name, ZipFile=z)
    lam.get_waiter("function_updated").wait(
        FunctionName=fn_name, WaiterConfig={"Delay": 3, "MaxAttempts": 30}
    )
    return len(z)


with report("week_2a_predictions_schema") as r:
    r.heading("Week 2A — Predictions schema migration in signal-logger")

    sl_path = REPO_ROOT / "aws/lambdas/justhodl-signal-logger/source/lambda_function.py"
    src = sl_path.read_text(encoding="utf-8")

    # ─── PATCH 1 — Add module-level regime snapshot ────────────────
    # Insert after the get_baseline_price function block, before log_sig
    old_logsig_signature = "def log_sig(stype,val,pred,conf,against,windows,price=None,meta=None,bench=None):"

    new_helpers_and_signature = '''# Captures Khalid regime once per Lambda invocation; populated by lambda_handler
_REGIME_SNAPSHOT={"regime":None,"khalid_score":None}

def _capture_regime_snapshot():
    """Read data/report.json once, capture regime + khalid_score for this run.
    Called from lambda_handler at start of invocation."""
    try:
        d=fs3("data/report.json")
        ki=d.get("khalid_index")
        if isinstance(ki,dict):
            _REGIME_SNAPSHOT["khalid_score"]=int(float(ki.get("score",0))) if ki.get("score") is not None else None
            _REGIME_SNAPSHOT["regime"]=ki.get("regime") or d.get("regime")
        elif ki is not None:
            _REGIME_SNAPSHOT["khalid_score"]=int(float(ki))
            _REGIME_SNAPSHOT["regime"]=d.get("regime")
        else:
            _REGIME_SNAPSHOT["regime"]=d.get("regime")
        print(f"[REGIME] snapshot: regime={_REGIME_SNAPSHOT['regime']}, score={_REGIME_SNAPSHOT['khalid_score']}")
    except Exception as e:
        print(f"[REGIME] snapshot failed (non-fatal): {e}")

def log_sig(stype,val,pred,conf,against,windows,price=None,meta=None,bench=None,
            magnitude=None,target_price=None,rationale=None,supporting=None):'''

    if old_logsig_signature not in src:
        r.fail("  log_sig signature not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_logsig_signature, new_helpers_and_signature, 1)
    r.ok("  Inserted regime-snapshot helper + extended log_sig signature")

    # ─── PATCH 2 — Replace the item={...} construction ─────────────
    old_item_block = '''    item={"signal_id":sid,"signal_type":stype,"signal_value":str(val),
          "predicted_direction":pred,"confidence":f2d(float(conf)),
          "measure_against":against,"baseline_price":f2d(float(price)) if price else None,
          "baseline_benchmark_price":f2d(float(bench_price)) if bench_price else None,
          "benchmark":bench,"check_windows":[str(d) for d in windows],
          "check_timestamps":ts,"outcomes":{},"accuracy_scores":{},
          "logged_at":now.isoformat(),"logged_epoch":int(now.timestamp()),
          "status":"pending","metadata":f2d(meta or {}),
          "ttl":int((now+timedelta(days=365)).timestamp())}'''

    new_item_block = '''    # Compute predicted_target_price from magnitude × baseline (Q1.2 — both)
    computed_target=None
    if target_price is not None:
        computed_target=float(target_price)
    elif magnitude is not None and price:
        # +X% magnitude → target_price = baseline × (1 + X/100)
        # NEUTRAL/0 magnitude leaves target = baseline
        computed_target=float(price)*(1.0+float(magnitude)/100.0)

    horizon_primary=max(windows) if windows else None

    item={"signal_id":sid,"signal_type":stype,"signal_value":str(val),
          "predicted_direction":pred,"confidence":f2d(float(conf)),
          "measure_against":against,"baseline_price":f2d(float(price)) if price else None,
          "baseline_benchmark_price":f2d(float(bench_price)) if bench_price else None,
          "benchmark":bench,"check_windows":[str(d) for d in windows],
          "check_timestamps":ts,"outcomes":{},"accuracy_scores":{},
          "logged_at":now.isoformat(),"logged_epoch":int(now.timestamp()),
          "status":"pending","metadata":f2d(meta or {}),
          "ttl":int((now+timedelta(days=365)).timestamp()),
          # ─── Schema v2 fields (Week 2A) ────────────────────
          "schema_version":"2",
          "predicted_magnitude_pct":f2d(float(magnitude)) if magnitude is not None else None,
          "predicted_target_price":f2d(float(computed_target)) if computed_target is not None else None,
          "horizon_days_primary":int(horizon_primary) if horizon_primary else None,
          "regime_at_log":_REGIME_SNAPSHOT.get("regime"),
          "khalid_score_at_log":_REGIME_SNAPSHOT.get("khalid_score"),
          "rationale":str(rationale) if rationale else None,
          "supporting_signals":list(supporting) if supporting else None,
          }'''

    if old_item_block not in src:
        r.fail("  item dict block not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_item_block, new_item_block, 1)
    r.ok("  Replaced item={} block to include schema v2 fields")

    # ─── PATCH 3 — Call _capture_regime_snapshot() at start of handler ──
    old_handler_start = '''def lambda_handler(event,context):
    logged=[]
    # data.json
    d=fs3("data/report.json")'''

    new_handler_start = '''def lambda_handler(event,context):
    # Capture Khalid regime once for this invocation; every log_sig() call
    # reads from _REGIME_SNAPSHOT to populate regime_at_log + khalid_score_at_log
    _capture_regime_snapshot()
    logged=[]
    # data.json
    d=fs3("data/report.json")'''

    if old_handler_start not in src:
        r.fail("  lambda_handler start not found verbatim — cannot patch")
        raise SystemExit(1)

    src = src.replace(old_handler_start, new_handler_start, 1)
    r.ok("  Added regime snapshot capture at handler start")

    # ─── Validate + write + deploy ─────────────────────────────────
    import ast
    try:
        ast.parse(src)
    except SyntaxError as e:
        r.fail(f"  Syntax error: {e}")
        raise SystemExit(1)

    sl_path.write_text(src, encoding="utf-8")
    r.ok(f"  Source valid ({len(src)} bytes), saved")

    size = deploy("justhodl-signal-logger", sl_path.parent)
    r.ok(f"  Deployed signal-logger ({size:,} bytes)")

    # ─── Trigger fresh run to verify schema v2 lands on new items ──
    r.section("Trigger fresh signal-logger run with schema v2")
    try:
        resp = lam.invoke(
            FunctionName="justhodl-signal-logger",
            InvocationType="Event",
        )
        r.ok(f"  Async-triggered (status {resp['StatusCode']})")
        r.log("  Verification will follow in next ops script.")
    except Exception as e:
        r.fail(f"  Trigger failed: {e}")

    r.kv(
        schema_version="bumped from implicit v1 to '2'",
        new_fields=8,
        backward_compatible=True,
        magnitude_default="None (callers pass when natural)",
    )
    r.log("Done")
