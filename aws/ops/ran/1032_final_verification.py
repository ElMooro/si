#!/usr/bin/env python3
"""Step 1032 — Final verification: did all the new producers' events
actually land in the audit log? Now that 1031 has been running for a
while, the EventBridge → coordinator → audit log pipeline should have
caught up.

Specifically:
  - master-ranker had 3 tier-5+ tickers → expect 3 convergence.tier_up events
  - master-ranker had 38 tier-3+ → expect 38 convergence.tier_up events (tier-3)
    (suppressed on Telegram but still audited)
  - signal-board posture was NEUTRAL — only emits if previous != current
  - miss-detector wasn't invoked yet

Also runs a comprehensive end-state report on:
  - Event bus structure
  - All schedules
  - Producer wirings (cross-check the code)
  - Consumer routes
  - Last hour of events
"""
import json, os, pathlib
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import boto3

REPORT = "aws/ops/reports/1032_final_verification.json"
REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # ─── Audit log analysis ──────────────────────────────────────────────
    print("[1032] reading today's audit log…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"system-events/audit/{today}.jsonl")
        lines = [l for l in obj["Body"].read().decode().split("\n") if l.strip()]
        entries = [json.loads(l) for l in lines]
        out["audit"] = {
            "n_events": len(entries),
            "by_event_type": dict(),
            "by_source_engine": dict(),
            "convergence_events": [],
            "posture_events": [],
            "all_events": [],
        }
        # Counts
        by_ev = defaultdict(int)
        by_src = defaultdict(int)
        for e in entries:
            ev = e.get("event", "?")
            by_ev[ev] += 1
            det = e.get("detail", {})
            src = det.get("_source_engine", "?")
            by_src[src] += 1
            
            if ev == "convergence.tier_up":
                out["audit"]["convergence_events"].append({
                    "ts": e.get("ts"),
                    "ticker": det.get("ticker"),
                    "new_tier": det.get("new_tier"),
                    "n_systems": det.get("n_systems"),
                    "systems": det.get("systems", [])[:5],
                })
            elif ev == "posture.changed":
                out["audit"]["posture_events"].append({
                    "ts": e.get("ts"),
                    "previous": det.get("previous"),
                    "current": det.get("current"),
                })
            
            out["audit"]["all_events"].append({
                "ts": e.get("ts", "")[:19],
                "event": ev,
                "source": src,
                "summary": {k: v for k, v in det.items() if k != "traceback"}
                            if ev == "engine.error" else None,
            })
        
        out["audit"]["by_event_type"] = dict(by_ev)
        out["audit"]["by_source_engine"] = dict(by_src)
    except s3.exceptions.NoSuchKey:
        out["audit"] = {"missing": True, "n_events": 0}
    
    # ─── Event-flow-health.json ──────────────────────────────────────────
    print("[1032] reading event-flow-health…")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key="data/event-flow-health.json")
        h = json.loads(obj["Body"].read().decode())
        out["flow_health"] = {
            "pulse":               h.get("pulse"),
            "n_anomalies":         len(h.get("anomalies", [])),
            "anomalies":           h.get("anomalies", []),
            "coordinator_health":  h.get("coordinator_health"),
            "generated_at":        h.get("generated_at"),
            "n_event_types_seen":  h.get("totals_today", {}).get("n_event_types_seen"),
            "by_event_today":      {k: v.get("today") for k, v in
                                     h.get("by_event", {}).items()},
        }
    except Exception as e:
        out["flow_health_err"] = str(e)[:200]
    
    # ─── Schedules: confirm event-flow-monitor + signal-board scheduled ──
    print("[1032] verifying schedules…")
    schedules = {}
    for rule_name in ("event-flow-monitor-hourly", "signal-board-1h",
                        "miss-detector-daily", "justhodl-master-ranker-daily"):
        try:
            r = events.describe_rule(Name=rule_name)
            schedules[rule_name] = {
                "schedule": r.get("ScheduleExpression"),
                "state":    r.get("State"),
            }
        except Exception:
            schedules[rule_name] = {"err": "not_found"}
    out["schedules"] = schedules
    
    # ─── Cross-check producer code: which Lambdas have publish() ─────────
    print("[1032] scanning all Lambda sources for event publishes…")
    producers = {}
    for d in sorted(pathlib.Path("aws/lambdas").iterdir()):
        if not d.is_dir():
            continue
        lf = d / "source" / "lambda_function.py"
        if not lf.exists():
            continue
        try:
            content = lf.read_text()
        except Exception:
            continue
        if "from system_events import" not in content:
            continue
        # Extract event names published
        publishes = set()
        import re
        # Quoted event names like "regime.changed"
        for m in re.finditer(r'publish\([^)]*"([a-z_]+\.[a-z_]+)"', content):
            publishes.add(m.group(1))
        # Pattern: EVT_FOO constants
        for m in re.finditer(r'publish[a-z_]*\([^)]*EVT_([A-Z_]+)', content):
            const = m.group(1)
            publishes.add(const.lower().replace("_", "."))
        # Pattern: tuples in publish_many like ("event.name", { ... })
        for m in re.finditer(r'\(\s*"([a-z_]+\.[a-z_]+)"\s*,\s*\{', content):
            publishes.add(m.group(1))
        if publishes:
            producers[d.name] = sorted(publishes)
    out["producers"] = producers
    out["n_producers"] = len(producers)
    
    # ─── Cross-check coordinator routes ──────────────────────────────────
    coord_lf = pathlib.Path("aws/lambdas/justhodl-event-coordinator/source/lambda_function.py")
    routes = []
    if coord_lf.exists():
        content = coord_lf.read_text()
        import re
        for m in re.finditer(r'"(\w+\.\w+)":\s*\{[^}]*"invoke":\s*\[([^\]]*)\]',
                              content, re.DOTALL):
            ev = m.group(1)
            inv_list = re.findall(r'"([^"]+)"', m.group(2))
            routes.append({"event": ev, "invokes": inv_list})
    out["routes"] = routes
    out["n_routes"] = len(routes)
    
    # ─── Last hour of events specifically ────────────────────────────────
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    recent_count = 0
    if out["audit"].get("n_events", 0) > 0:
        for e in out["audit"].get("all_events", []):
            try:
                ts = datetime.fromisoformat((e["ts"] + "+00:00") if "+" not in e["ts"] else e["ts"])
                if ts >= one_hour_ago:
                    recent_count += 1
            except Exception:
                pass
    out["events_last_hour"] = recent_count
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))


if __name__ == "__main__":
    main()
