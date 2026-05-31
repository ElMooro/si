#!/usr/bin/env python3
"""Step 1026 — Final consolidation: deploy calibrator + the 111 config fixes
land via the deploy-lambdas workflow, then produce a comprehensive
end-state report showing what's wired and what isn't.

The 111 config fixes from this commit will trigger deploy-lambdas.yml for
each affected Lambda — but that takes time and is run per-Lambda by GH
Actions. The memory takes effect on next deploy of each one.

WHAT THIS REPORT TELLS YOU:
  - Which engines are wired to the event bus (producers)
  - Which engines listen to the bus (consumers via coordinator routes)
  - Audit log activity in last 24h
  - Comparative health: before vs after this session's changes
"""
import io, json, os, time, zipfile, pathlib
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1026_final_state.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)


def redeploy_code(fn_name, retry=4):
    src_dir = pathlib.Path(f"aws/lambdas/{fn_name}/source")
    if not src_dir.exists():
        return {"err": "source dir not found"}
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in src_dir.glob("*.py"):
            zf.writestr(f.name, f.read_bytes())
    zb = buf.getvalue()
    for attempt in range(retry):
        try:
            lam.update_function_code(FunctionName=fn_name, ZipFile=zb, Publish=False)
            lam.get_waiter("function_updated").wait(FunctionName=fn_name)
            return {"action": "updated", "zip_size": len(zb)}
        except Exception as e:
            if "ResourceConflict" in str(e) and attempt < retry - 1:
                time.sleep(5 * (attempt + 1))
                continue
            return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


def read_audit_log_today(limit=40):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"system-events/audit/{today}.jsonl"
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read().decode("utf-8")
        lines = [l for l in body.split("\n") if l.strip()]
        return {
            "key": key, "n_events": len(lines),
            "entries": [json.loads(l) for l in lines[-limit:]],
        }
    except s3.exceptions.NoSuchKey:
        return {"missing": True}
    except Exception as e:
        return {"err": str(e)[:200]}


def check_bus_state():
    """Verify the event bus + rule are still configured."""
    out = {}
    try:
        out["bus"] = events.describe_event_bus(Name="justhodl-system-events")
        out["bus"] = {
            "name":  out["bus"]["Name"],
            "arn":   out["bus"]["Arn"],
        }
    except Exception as e:
        out["bus_err"] = str(e)[:200]
    try:
        r = events.describe_rule(
            Name="justhodl-events-to-coordinator",
            EventBusName="justhodl-system-events",
        )
        out["rule"] = {
            "name": r["Name"], "state": r.get("State"),
            "pattern": r.get("EventPattern"),
        }
        tgts = events.list_targets_by_rule(
            Rule="justhodl-events-to-coordinator",
            EventBusName="justhodl-system-events",
        )
        out["targets"] = [t.get("Arn", "").split(":")[-1] for t in tgts.get("Targets", [])]
    except Exception as e:
        out["rule_err"] = str(e)[:200]
    return out


def main():
    started = datetime.now(timezone.utc)
    out = {"started": started.isoformat()}
    
    # ─── Phase 1: deploy the calibrator change ──────────────────────────
    print("[1026] phase 1: deploy calibrator (now emits calibrator.weights_updated)…")
    out["calibrator_deploy"] = redeploy_code("justhodl-calibrator")
    time.sleep(3)
    
    # ─── Phase 2: verify event bus state ────────────────────────────────
    print("[1026] phase 2: verify event bus state…")
    out["bus_state"] = check_bus_state()
    
    # ─── Phase 3: read audit log ────────────────────────────────────────
    print("[1026] phase 3: read audit log…")
    out["audit_log"] = read_audit_log_today(limit=50)
    
    # ─── Phase 4: list all 5 engines that publish events ────────────────
    # (these have system_events.py in their source dir)
    print("[1026] phase 4: enumerate event producers/consumers…")
    producers = []
    consumers = []
    for d in sorted(pathlib.Path("aws/lambdas").iterdir()):
        if not d.is_dir():
            continue
        src = d / "source"
        if not src.exists():
            continue
        # Has system_events.py imported?
        lambda_py = src / "lambda_function.py"
        if not lambda_py.exists():
            continue
        try:
            content = lambda_py.read_text()
            if "from system_events import" in content or "import system_events" in content:
                # Find which events it publishes
                evt_constants = [
                    "EVT_OUTCOME_RESOLVED", "EVT_REGIME_CHANGED",
                    "EVT_NEAR_MISS_EXTREME", "EVT_CALIBRATOR_PROPOSAL_HIGH_CONF",
                    "EVT_CALIBRATOR_WEIGHTS_UPDATED", "EVT_SIGNAL_DEPRECATED",
                    "EVT_SIGNAL_PROMOTED", "EVT_ENGINE_ERROR", "EVT_MISS_DETECTED",
                ]
                publishes = []
                for c in evt_constants:
                    if c in content:
                        publishes.append(c.replace("EVT_", "").lower().replace("_", "."))
                # Also search for string literal events
                for literal_event in ["outcome.resolved", "regime.changed", "near_miss.extreme",
                                        "calibrator.proposal_high_confidence",
                                        "calibrator.weights_updated", "signal.deprecated",
                                        "signal.promoted", "engine.error", "miss.detected"]:
                    if f'"{literal_event}"' in content and literal_event not in publishes:
                        publishes.append(literal_event)
                if publishes:
                    producers.append({
                        "engine": d.name,
                        "publishes": publishes,
                    })
        except Exception:
            continue
    out["producers"] = producers
    out["n_producers"] = len(producers)
    
    # ─── Phase 5: get coordinator's ROUTES table (consumers) ─────────────
    coord_lambda_py = pathlib.Path("aws/lambdas/justhodl-event-coordinator/source/lambda_function.py")
    if coord_lambda_py.exists():
        content = coord_lambda_py.read_text()
        # Parse the ROUTES dict heuristically — extract event names
        import re
        # Find "<event_name>": {...}
        matches = re.findall(r'"(\w+\.\w+)":\s*\{[^}]*"invoke":\s*\[([^\]]*)\]',
                               content)
        for ev, lambdas_str in matches:
            lambdas = re.findall(r'"([^"]+)"', lambdas_str)
            consumers.append({"event": ev, "invokes": lambdas})
    out["consumer_routes"] = consumers
    out["n_consumer_routes"] = len(consumers)
    
    # ─── Phase 6: comparative health (pre vs post) ──────────────────────
    # Pull invocation metrics for the previously-broken engines
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=7)
    def metrics_for(fn):
        try:
            inv = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=start, EndTime=end, Period=86400,
                Statistics=["Sum"],
            )
            err = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Errors",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=start, EndTime=end, Period=86400,
                Statistics=["Sum"],
            )
            n_inv = int(sum(p["Sum"] for p in inv.get("Datapoints") or []))
            n_err = int(sum(p["Sum"] for p in err.get("Datapoints") or []))
            return {
                "invocations_7d":  n_inv,
                "errors_7d":       n_err,
                "error_rate_pct":  round(n_err / max(1, n_inv) * 100, 2),
            }
        except Exception as e:
            return {"err": str(e)[:120]}
    
    out["broken_engine_metrics"] = {
        "justhodl-crisis-plumbing":          metrics_for("justhodl-crisis-plumbing"),
        "justhodl-liquidity-credit-engine":  metrics_for("justhodl-liquidity-credit-engine"),
        "justhodl-crypto-opportunities":     metrics_for("justhodl-crypto-opportunities"),
        "justhodl-outcome-checker":          metrics_for("justhodl-outcome-checker"),
    }
    
    # ─── Phase 7: scan for config.json memory_size remnants ──────────────
    print("[1026] phase 7: verify memory_size bug is gone…")
    wrong = []
    for cfg_path in sorted(pathlib.Path("aws/lambdas").glob("*/config.json")):
        try:
            cfg = json.loads(cfg_path.read_text())
            if "memory_size" in cfg:
                wrong.append(cfg_path.parent.name)
        except Exception:
            continue
    out["memory_size_remnants"] = wrong
    out["n_memory_size_remnants"] = len(wrong)
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
