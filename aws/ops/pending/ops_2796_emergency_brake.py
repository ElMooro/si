"""ops 2796 — EMERGENCY BRAKE on LLM spend (no redeploy, 5-min propagation).
Sets economy mode (all reason/critical -> Haiku, ~5-7x cheaper than Sonnet) and
a hard daily budget cap. Instantly reversible via SSM.
"""
import os, json
from datetime import datetime, timezone
import boto3
ssm = boto3.client("ssm", region_name="us-east-1")
R = {"ops": 2796, "ts": datetime.now(timezone.utc).isoformat(), "set": {}}
for name, val in [("/justhodl/llm/mode", "economy"), ("/justhodl/llm/daily-budget-usd", "15")]:
    try:
        ssm.put_parameter(Name=name, Value=val, Type="String", Overwrite=True)
        cur = ssm.get_parameter(Name=name)["Parameter"]["Value"]
        R["set"][name] = cur
        print("%s = %s" % (name, cur))
    except Exception as e:
        R["set"][name] = "ERR " + str(e)[:80]
        print("ERR", name, str(e)[:80])
R["status"] = "BRAKE ENGAGED — economy mode + $15/day hard cap (tunable via SSM)"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2796_emergency_brake.json", "w"), indent=1, default=str)
print("OPS 2796:", R["status"])
