"""ops 2803 — add runaway-guard daily caps for the residual uncapped risk engines
found by the burn audit (loop/Sonnet/on-demand). Generous caps: stop a runaway
spike without breaking normal cadence. Merges with existing once-a-day caps."""
import os, json
from datetime import datetime, timezone
import boto3
ssm = boto3.client("ssm", region_name="us-east-1")
R = {"ops": 2803, "ts": datetime.now(timezone.utc).isoformat()}
try:
    cur = json.loads(ssm.get_parameter(Name="/justhodl/llm/engine-daily-cap")["Parameter"]["Value"])
except Exception:
    cur = {}
guards = {
    "justhodl-research-papers": 30,      # Sonnet + per-item loop
    "justhodl-debate-engine": 30,        # per-item loop + retry
    "justhodl-ka-metrics": 15,           # Sonnet 7k-tok, on-demand, retry
    "justhodl-khalid-metrics": 15,       # Sonnet 7k-tok, on-demand, retry
    "justhodl-ai-website-synthesis": 30, # 24/day Haiku
    "justhodl-auction-crisis-ai": 30,    # 24/day Haiku
}
merged = dict(cur)
for k, v in guards.items():
    merged.setdefault(k, v)  # don't override existing (e.g. the 1/day set)
ssm.put_parameter(Name="/justhodl/llm/engine-daily-cap", Value=json.dumps(merged), Type="String", Overwrite=True)
R["engine_caps"] = json.loads(ssm.get_parameter(Name="/justhodl/llm/engine-daily-cap")["Parameter"]["Value"])
print("engine-daily-cap now:", json.dumps(R["engine_caps"], indent=1))
R["status"] = "RUNAWAY GUARDS ADDED"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2803_runaway_guards.json", "w"), indent=1, default=str)
print("OPS 2803:", R["status"])
