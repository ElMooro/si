# ops 1617 — diagnose sentinel telegram False: replay the canary change w/ full log capture
import json, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1617}
# surgically remove canary keys from state so the same diff fires again
st = json.loads(s3.get_object(Bucket=B, Key="data/_alerts/last.json")["Body"].read())
for k in ("canary_reds", "canary_level_v3", "canary_v3", "_canary_names"):
    st.pop(k, None)
s3.put_object(Bucket=B, Key="data/_alerts/last.json", Body=json.dumps(st, default=str).encode(),
              ContentType="application/json")
out["state_keys_removed"] = True
# confirm env creds present
c = lam.get_function_configuration(FunctionName="justhodl-alert-sentinel")
env = (c.get("Environment") or {}).get("Variables") or {}
out["env_check"] = {"has_token": bool(env.get("TELEGRAM_TOKEN")),
                     "token_head": (env.get("TELEGRAM_TOKEN") or "")[:12],
                     "chat": env.get("TELEGRAM_CHAT")}
r = lam.invoke(FunctionName="justhodl-alert-sentinel", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
out["log_tail"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-1600:]
d = json.loads(s3.get_object(Bucket=B, Key="data/alert-sentinel.json")["Body"].read())
out["verify"] = {"message_sent": d.get("message_sent"), "n_changes": d.get("n_changes"),
                  "changes": d.get("changes"), "diagnostics": d.get("diagnostics")}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1617_telegram_diag.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "sent": d.get("message_sent"),
                   "diag": d.get("diagnostics")}, default=str))
