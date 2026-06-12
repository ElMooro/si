# ops 1629 — v1.2.0 own-history axis: redeploy, invoke (hist build ~150s), verify
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1629}
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
out["fn_err"] = r.get("FunctionError", "NONE")
d = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
sp = d.get("sp_table") or []
def fv(t, keys):
    x = next((x for x in sp if x.get("t") == t), {})
    return {k: x.get(k) for k in keys}
dd = [x for x in sp if x.get("deep_discount")]
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "diag": [x for x in (d.get("diagnostics") or []) if "hist" in x or "ratios-annual" in x],
  "hist_coverage": d.get("hist_coverage"), "n_deep_discount": d.get("n_deep_discount"),
  "aapl": fv("AAPL", ("value_pct", "hist_pct", "vclass")),
  "nvda": fv("NVDA", ("value_pct", "hist_pct")),
  "deep_discount_head": [{k: x.get(k) for k in ("t", "sector", "value_pct", "hist_pct",
                            "vclass", "pe", "ps", "fcf_y", "gf_gap")} for x in dd[:10]],
}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1629_own_history.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "hist": d.get("hist_coverage"),
                   "dd": d.get("n_deep_discount")}, default=str))
