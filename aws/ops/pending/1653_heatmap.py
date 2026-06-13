# ops 1653 — deploy valuations v1.5.0 (mcap + heatmap tiles), invoke, verify tiles
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1653}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-stock-valuations/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-stock-valuations/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-stock-valuations", ZipFile=buf.getvalue()); break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName="justhodl-stock-valuations")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
r = lam.invoke(FunctionName="justhodl-stock-valuations", InvocationType="RequestResponse", Payload=b"{}")
out["err"] = r.get("FunctionError", "NONE")
d = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
hm = d.get("heatmap") or {}
hp, sp = hm.get("hp") or [], hm.get("sp") or []
def hist(arr):
    b = {"0-25": 0, "25-50": 0, "50-75": 0, "75-100": 0}
    for t in arr:
        a = t["a"]
        k = "0-25" if a < 25 else "25-50" if a < 50 else "50-75" if a < 75 else "75-100"
        b[k] += 1
    return b
out["verify"] = {"version": d.get("version"),
                  "diag": [x for x in (d.get("diagnostics") or []) if "heatmap" in x or "v1.5" in x],
                  "n_hp": len(hp), "n_sp": len(sp),
                  "sp_mcap_null": sum(1 for r2 in (d.get("sp_table") or []) if not r2.get("mcap")),
                  "hp_hist": hist(hp), "sp_hist": hist(sp),
                  "hp_hot": sorted(hp, key=lambda x: -x["a"])[:6],
                  "sp_hot": sorted(sp, key=lambda x: -x["a"])[:6],
                  "sp_cold": sorted(sp, key=lambda x: x["a"])[:4],
                  "n_groups_hp": len({t["g"] for t in hp})}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1653_heatmap.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"err": out["err"], "hp": len(hp), "sp": len(sp)}))
