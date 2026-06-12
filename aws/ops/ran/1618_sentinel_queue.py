# ops 1618 — sentinel v1.1.1 queue-until-delivered: deploy, invoke twice, prove retry semantics
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1618}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-alert-sentinel/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-alert-sentinel/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-alert-sentinel", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(40):
    c = lam.get_function_configuration(FunctionName="justhodl-alert-sentinel")
    if c.get("LastUpdateStatus") != "InProgress" and c.get("State") != "Pending":
        break
    time.sleep(3)
runs = []
for i in (1, 2):
    lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
    d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
    runs.append({"run": i, "version": d.get("version"), "message_sent": d.get("message_sent"),
                  "state_saved": d.get("state_saved"), "n_changes": d.get("n_changes"),
                  "changes": d.get("changes"), "diag": d.get("diagnostics")})
    time.sleep(4)
out["runs"] = runs
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1618_sentinel_queue.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps(runs, default=str))
