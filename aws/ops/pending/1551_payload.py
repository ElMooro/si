# ops 1551 — capture skill-aggregator full err + traceback from router response payload
import json, boto3
from botocore.config import Config
lam = boto3.client("lambda", region_name="us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 1}))
r = lam.invoke(FunctionName="justhodl-ai-brief-router", InvocationType="RequestResponse",
               Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode())
payload = r["Payload"].read().decode()
out = {"ops": 1551, "fn_err": r.get("FunctionError", "NONE")}
try:
    body = json.loads(payload)
    if isinstance(body.get("body"), str):
        body = json.loads(body["body"])
    res = body.get("results") or body
    skill = None
    if isinstance(res, list):
        skill = next((x for x in res if x.get("context_id") == "frontrun-skill-aggregator"), None)
    out["skill_result"] = skill or res
except Exception:
    out["raw_payload"] = payload[:3000]
open("aws/ops/reports/1551_payload.json", "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, default=str)[:1800])
