# ops 1655 — pulse v1.0.1: correct sentinel state key + TELEGRAM_TOKEN env; merge orphan events
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1655}
sent_env = (lam.get_function_configuration(FunctionName="justhodl-alert-sentinel").get("Environment") or {}).get("Variables") or {}
lam.update_function_configuration(FunctionName="justhodl-intraday-pulse",
    Environment={"Variables": {"POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
                                  "TELEGRAM_TOKEN": sent_env.get("TELEGRAM_TOKEN", ""),
                                  "TELEGRAM_CHAT": sent_env.get("TELEGRAM_CHAT", "")}})
out["tg_env"] = bool(sent_env.get("TELEGRAM_TOKEN"))
time.sleep(6)
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-intraday-pulse/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-intraday-pulse/source"))
for _ in range(8):
    try:
        lam.update_function_code(FunctionName="justhodl-intraday-pulse", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-intraday-pulse")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
# merge orphan buffer -> real sentinel buffer
try:
    orphan = json.loads(s3.get_object(Bucket=B, Key="data/_sentinel/state.json")["Body"].read())
    ob = orphan.get("buffer") or []
except Exception:
    ob = []
real = json.loads(s3.get_object(Bucket=B, Key="data/_alerts/last.json")["Body"].read())
rb = real.get("buffer") or []
existing = {x.get("line") for x in rb}
merged = 0
for x in ob:
    if x.get("line") not in existing:
        rb.append(x)
        merged += 1
real["buffer"] = rb[-400:]
s3.put_object(Bucket=B, Key="data/_alerts/last.json", Body=json.dumps(real, default=str).encode(),
              ContentType="application/json")
try:
    s3.delete_object(Bucket=B, Key="data/_sentinel/state.json")
except Exception:
    pass
out["merged_events"] = merged
out["real_buffer_n"] = len(real["buffer"])
r = lam.invoke(FunctionName="justhodl-intraday-pulse", InvocationType="RequestResponse",
                Payload=json.dumps({"force": True}).encode())
out["pulse2"] = json.loads(json.loads(r["Payload"].read()).get("body", "{}"))
real2 = json.loads(s3.get_object(Bucket=B, Key="data/_alerts/last.json")["Body"].read())
out["buffer_after_pulse"] = len(real2.get("buffer") or [])
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1655_pulse_wire.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps(out, default=str))
