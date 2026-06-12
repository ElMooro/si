# ops 1628 — v1.1.1: negative-ROE trap only when unprofitable; verify HPQ/HCA reclassified
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1635}
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
def fv(t):
    x = next((x for x in sp if x.get("t") == t), {})
    return {k: x.get(k) for k in ("sbc_rev", "vclass", "rule40")}
out["verify"] = {"version": d.get("version"),
                  "sbc_coverage": sum(1 for x in sp if x.get("sbc_rev") is not None),
                  "high_sbc": sorted([{"t": x["t"], "sbc": round(x["sbc_rev"] * 100, 1)}
                                       for x in sp if (x.get("sbc_rev") or 0) > 0.10],
                                      key=lambda z: -z["sbc"])[:6],
                  "hpq": fv("HPQ"), "hca": fv("HCA"), "pru": fv("PRU"), "gpn": fv("GPN"),
                  "vclass_hist": {}}
for x in sp:
    out["verify"]["vclass_hist"][x.get("vclass")] = out["verify"]["vclass_hist"].get(x.get("vclass"), 0) + 1
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1635_sbc_emit.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps(out["verify"], default=str))
