# ops 1647 — debate desk v2.0.1 (time-guarded) deploy + invoke + verify
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1648}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-research-papers/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-research-papers/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-research-papers", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName="justhodl-research-papers")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
r = lam.invoke(FunctionName="justhodl-research-papers", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["err"] = r.get("FunctionError", "NONE")
out["log"] = base64.b64decode(r.get("LogResult", "")).decode(errors="replace")[-700:]
idx = json.loads(s3.get_object(Bucket=B, Key="data/research-papers.json")["Body"].read())
out["written"] = idx.get("written_this_run")
out["diag"] = idx.get("diagnostics")
out["index_head"] = [{k: p_.get(k) for k in ("t", "conviction", "stance")}
                       for p_ in (idx.get("papers") or [])[:5]]
ws = idx.get("written_this_run") or []
if ws:
    t = ws[0]
    pp = json.loads(s3.get_object(Bucket=B, Key=f"data/research/{t}.json")["Body"].read())
    pa = pp.get("paper") or {}
    db = pa.get("debate") or {}
    out["debate_sample"] = {
        "ticker": t, "conviction": pa.get("conviction_1_10"),
        "stance": pa.get("position_stance"),
        "bull_point_1": ((db.get("bull") or {}).get("strongest_points") or [""])[0][:220],
        "bear_attack_1": ((db.get("bear") or {}).get("strongest_attacks") or [""])[0][:220],
        "kill_conditions": (db.get("bear") or {}).get("kill_conditions"),
        "debate_verdict": (pa.get("debate_verdict") or "")[:300],
        "conceded": pa.get("points_conceded_to_bear"),
        "models": db.get("models"),
        "has_all": all(k in pa for k in ("debate_verdict", "points_conceded_to_bear",
                                            "position_stance", "conviction_1_10"))}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1648_debate_repair.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"err": out["err"], "written": out["written"]}, default=str))
