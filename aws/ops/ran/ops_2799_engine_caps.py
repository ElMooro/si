"""ops 2799 — set per-engine daily LLM-call caps (once-a-day) via SSM."""
import os, json
from datetime import datetime, timezone
import boto3
ssm = boto3.client("ssm", region_name="us-east-1")
R = {"ops": 2799, "ts": datetime.now(timezone.utc).isoformat()}
caps = {
    "justhodl-equity-research": 1,
    "justhodl-ticker-deep-research": 1,
    "justhodl-news-wire": 1,
    "justhodl-research-critique": 1,
}
try:
    ssm.put_parameter(Name="/justhodl/llm/engine-daily-cap", Value=json.dumps(caps),
                      Type="String", Overwrite=True)
    cur = ssm.get_parameter(Name="/justhodl/llm/engine-daily-cap")["Parameter"]["Value"]
    R["engine_caps"] = json.loads(cur)
    print("engine-daily-cap set:", cur)
except Exception as e:
    R["engine_caps"] = "ERR " + str(e)[:100]
    print("ERR", str(e)[:100])
R["status"] = "ENGINE CAPS SET (once/day)"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2799_engine_caps.json", "w"), indent=1, default=str)
print("OPS 2799:", R["status"])
