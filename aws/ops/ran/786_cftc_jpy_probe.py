"""ops/786 — probe the CFTC agent's JPY (6J) response structure.

yen-carry's positioning leg needs the shape of the CFTC /cot/6J payload to
read net non-commercial JPY positioning reliably. This invokes the agent
directly and dumps the structure.
"""
import json, os
from datetime import datetime, timezone
import boto3
from botocore.config import Config

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120, retries={"max_attempts": 2}))
s3 = boto3.client("s3", region_name="us-east-1")
CFTC = "cftc-futures-positioning-agent"
report = {"ops": 786, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Probe CFTC /cot/6J JPY structure"}


def invoke(path):
    try:
        r = lam.invoke(FunctionName=CFTC, InvocationType="RequestResponse",
                       Payload=json.dumps({"rawPath": path}).encode())
        p = json.loads(r["Payload"].read() or b"{}")
        body = p.get("body", p)
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                pass
        return {"status": r.get("StatusCode"),
                "fn_error": r.get("FunctionError"), "body": body}
    except Exception as e:
        return {"err": f"{type(e).__name__}: {str(e)[:200]}"}


r6j = invoke("/cot/6J")
report["cot_6J"] = r6j


def shape(o, depth=0):
    if depth > 3:
        return "..."
    if isinstance(o, dict):
        return {k: shape(v, depth + 1) for k, v in list(o.items())[:25]}
    if isinstance(o, list):
        return [shape(o[0], depth + 1)] if o else []
    return type(o).__name__


report["cot_6J_shape"] = shape(r6j.get("body"))

# also confirm the cache exists + its top-level shape
try:
    cache = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/cftc-all-cache.json")["Body"].read())
    report["cache_exists"] = True
    report["cache_top_keys"] = (list(cache.keys())[:30]
                                if isinstance(cache, dict)
                                else f"list[{len(cache)}]")
except Exception as e:
    report["cache_exists"] = False
    report["cache_err"] = str(e)[:150]

print(json.dumps(report, indent=2, default=str)[:5000])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/786_cftc_jpy_probe.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/786_cftc_jpy_probe.json")
