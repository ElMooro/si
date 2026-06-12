# ops 1627 — v1.1.0 audit fixes: redeploy, invoke (no refetch), verify labels/classes/fin-adjust
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1627}
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
hp = d.get("hp") or []
def find(t, arr, key="t"):
    return next((x for x in arr if x.get(key) == t), None)
neg_pe = next((x for x in sp if (x.get("pe") or 1) < 0), None)
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "diag": [x for x in (d.get("diagnostics") or []) if "catalyst" in x or "sp cache" in x],
  "sp_rows": len(sp),
  "labels": {L: sum(1 for x in sp if x.get("label") == L) for L in ("CHEAP", "FAIR", "RICH")},
  "vclass_hist": {}, "hp_class_hist": {},
  "undervalued_head": [{k: x.get(k) for k in ("t", "sector", "value_pct", "pe", "ps", "roe",
                          "fcf_y", "rev_g", "gf_gap")} for x in sp
                         if x.get("vclass") == "POTENTIALLY UNDERVALUED"][:6],
  "trap_head": [{k: x.get(k) for k in ("t", "sector", "value_pct", "rev_g", "fcf_y", "roe")}
                 for x in sp if x.get("vclass") == "VALUE TRAP RISK"][:6],
  "neg_pe_sample": ({k: neg_pe.get(k) for k in ("t", "pe", "value_pct", "label", "vclass")}
                     if neg_pe else None),
  "aapl": {k: (find("AAPL", sp) or {}).get(k) for k in ("label", "vclass", "value_pct")},
  "spnt": {k: ((find("SPNT", hp) or {}).get(k)) for k in ("score", "hp_class", "soft_flags")},
  "spnt_cats": (find("SPNT", hp) or {}).get("cats"),
  "frsh": {k: ((find("FRSH", hp) or {}).get(k)) for k in ("score", "hp_class")},
  "ramp_class": (find("RAMP", hp) or {}).get("hp_class"),
  "n_serious": d.get("n_serious"), "hp_top": [(x.get("t"), x.get("score"), x.get("hp_class"))
                                                for x in hp[:6]],
}
for x in sp:
    out["verify"]["vclass_hist"][x.get("vclass")] = out["verify"]["vclass_hist"].get(x.get("vclass"), 0) + 1
for x in hp:
    out["verify"]["hp_class_hist"][x.get("hp_class")] = out["verify"]["hp_class_hist"].get(x.get("hp_class"), 0) + 1
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1627_valuations_v110.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fn_err": out["fn_err"], "rows": len(sp),
                   "spnt": out["verify"]["spnt"]}, default=str))
