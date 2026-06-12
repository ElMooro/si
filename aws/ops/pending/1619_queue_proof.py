# ops 1619 — prove v1.1.1 retry semantics: re-arm canary diff, invoke twice, expect persistence
import json, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1619}
st = json.loads(s3.get_object(Bucket=B, Key="data/_alerts/last.json")["Body"].read())
for k in ("canary_reds", "canary_level_v3", "canary_v3", "_canary_names"):
    st.pop(k, None)
s3.put_object(Bucket=B, Key="data/_alerts/last.json",
              Body=json.dumps(st, default=str).encode(), ContentType="application/json")
runs = []
for i in (1, 2):
    lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse", Payload=b"{}")
    d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
    runs.append({"run": i, "sent": d.get("message_sent"), "state_saved": d.get("state_saved"),
                  "n_changes": d.get("n_changes"), "changes": d.get("changes"),
                  "diag": d.get("diagnostics")})
    time.sleep(4)
out["runs"] = runs
out["proof"] = ("PASS" if (runs[0]["n_changes"] == 1 and runs[0]["state_saved"] is False
                             and runs[1]["n_changes"] == 1) else "FAIL")
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1619_queue_proof.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"proof": out["proof"], "runs": runs}, default=str))
