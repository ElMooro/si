# ops 1624 — stock-valuations v1.0.1: redeploy, invoke (forced refetch), verify ROE/div_y/gf
import json, zipfile, io, os, time, base64
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1625}
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    for root, _, fs in os.walk("aws/lambdas/justhodl-stock-valuations/source"):
        for f in fs:
            fp = os.path.join(root, f)
            z.write(fp, os.path.relpath(fp, "aws/lambdas/justhodl-stock-valuations/source"))
for _ in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-stock-valuations", ZipFile=buf.getvalue())
        out["fn"] = "updated"; break
    except Exception as e:
        if "ResourceConflict" in str(e): time.sleep(8)
        else: raise
for _ in range(50):
    c = lam.get_function_configuration(FunctionName="justhodl-stock-valuations")
    if c.get("LastUpdateStatus") != "InProgress":
        break
    time.sleep(3)
r = lam.invoke(FunctionName="justhodl-stock-valuations", InvocationType="RequestResponse",
                LogType="Tail", Payload=b"{}")
out["fn_err"] = r.get("FunctionError", "NONE")
d = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
sp = d.get("sp_table") or []
aapl = next((x for x in sp if x.get("t") == "AAPL"), {})
with_gf = [x for x in sp if x.get("gf_gap") is not None]
out["verify"] = {"version": d.get("version"), "duration_s": d.get("duration_s"),
                  "key_diag": [x for x in (d.get("diagnostics") or [])
                                if "return/yield" in x or "gf-value" in x or "refetch" in x],
                  "sp_coverage": d.get("sp_coverage"),
                  "aapl": {k: aapl.get(k) for k in ("roe", "roa", "div_y", "fcf_y", "gm",
                                                      "pe", "label")},
                  "n_with_gf_gap": len(with_gf),
                  "biggest_gf_upside": sorted(with_gf, key=lambda x: -(x["gf_gap"] or 0))[:4],
                  "n_serious": d.get("n_serious"), "hp_top_t": [(x.get("t"), x.get("score"))
                                                                  for x in (d.get("hp") or [])[:3]]}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1625_valuations_v102.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "roe": out["verify"]["aapl"].get("roe"),
                   "gf": out["verify"]["n_with_gf_gap"]}, default=str))
