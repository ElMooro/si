"""ops/746 — diagnose justhodl-nobrainer-rationale Claude-call failures.

The caller logs `[rationale] {ticker}/{theme} ERR HTTP {code}: {body}` on
failure and `[ssm-anthropic] ...` if the SSM key fetch fails. Pull those.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(retries={"max_attempts": 3})
logs = boto3.client("logs", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

report = {"ops": 746, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "nobrainer-rationale Claude-call diagnosis"}

fn = "justhodl-nobrainer-rationale"

# is ANTHROPIC_KEY set in the live env?
try:
    cfgd = lam.get_function_configuration(FunctionName=fn)
    env = (cfgd.get("Environment") or {}).get("Variables") or {}
    report["env_keys_present"] = sorted(env.keys())
    report["anthropic_key_in_env"] = "ANTHROPIC_KEY" in env and bool(env.get("ANTHROPIC_KEY"))
    report["timeout"] = cfgd.get("Timeout")
    report["memory"] = cfgd.get("MemorySize")
except Exception as e:
    report["config_err"] = str(e)[:200]

# pull recent logs
lg = f"/aws/lambda/{fn}"
start_ms = int((time.time() - 2 * 86400) * 1000)
HINTS = ("ERR", "[ssm-anthropic]", "rationale]", "HTTP ", "401", "403",
         "404", "429", "Traceback", "[ERROR]", "anthropic", "Claude")
try:
    events, token, pages = [], None, 0
    while pages < 6:
        kw = dict(logGroupName=lg, startTime=start_ms, limit=400, interleaved=True)
        if token:
            kw["nextToken"] = token
        resp = logs.filter_log_events(**kw)
        events.extend(resp.get("events", []))
        token = resp.get("nextToken")
        pages += 1
        if not token:
            break
    hits = [e for e in events
            if any(h in (e.get("message") or "") for h in HINTS)]
    hits.sort(key=lambda e: e.get("timestamp", 0))
    report["total_events"] = len(events)
    report["matched"] = len(hits)
    report["recent_lines"] = [
        {"t": datetime.fromtimestamp(e["timestamp"] / 1000,
                                     timezone.utc).isoformat()[11:19],
         "msg": (e.get("message") or "").strip()[:500]}
        for e in hits[-30:]]
except Exception as e:
    report["logs_err"] = str(e)[:200]

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/746_nobrainer_claude_diag.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/746_nobrainer_claude_diag.json")
