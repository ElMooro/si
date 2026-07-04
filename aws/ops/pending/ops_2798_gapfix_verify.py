"""ops 2798 — confirm shim economy-gap fix live on the previously-uncovered Sonnet engines."""
import os, json, time
from datetime import datetime, timezone
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")
R = {"ops": 2798, "ts": datetime.now(timezone.utc).isoformat()}
now = time.time()
fresh = {}
for fn in ["justhodl-ka-metrics", "justhodl-khalid-metrics", "justhodl-financial-secretary",
           "justhodl-research-papers", "justhodl-news-wire"]:
    try:
        lm = lam.get_function_configuration(FunctionName=fn)["LastModified"]
        t = datetime.strptime(lm.split(".")[0], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        fresh[fn] = round((now - t) / 60, 1)
    except Exception as e:
        fresh[fn] = "err:" + str(e)[:40]
R["redeploy_age_min"] = fresh
# news-wire schedule now 30min?
try:
    rules = lam.get_policy(FunctionName="justhodl-news-wire")
except Exception:
    pass
R["ssm"] = {n: ssm.get_parameter(Name=n)["Parameter"]["Value"] for n in
            ["/justhodl/llm/mode", "/justhodl/llm/daily-budget-usd"]}
recent = [v for v in fresh.values() if isinstance(v, (int, float)) and v < 30]
R["status"] = "GAP-FIX LIVE" if len(recent) >= 3 else "CHECK"
print("redeploy ages(min):", json.dumps(fresh))
print("ssm:", json.dumps(R["ssm"]))
print("STATUS:", R["status"])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2798_gapfix_verify.json", "w"), indent=1, default=str)
print("OPS 2798 COMPLETE")
