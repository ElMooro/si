# ops 1633 — exclude funds from underlooked universe; purge CEF papers; regenerate
import json, zipfile, io, os, time
import boto3
from botocore.config import Config
cfg = Config(read_timeout=880, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1633}
def zipdir(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, fs in os.walk(d):
            for f2 in fs:
                fp = os.path.join(root, f2)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()
def upd(fn, zb):
    for _ in range(6):
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zb); break
        except Exception as e:
            if "ResourceConflict" in str(e): time.sleep(8)
            else: raise
    for _ in range(50):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") != "InProgress":
            return
        time.sleep(3)
upd("justhodl-stock-valuations", zipdir("aws/lambdas/justhodl-stock-valuations/source"))
fills = []
for i in range(2):
    r = lam.invoke(FunctionName="justhodl-stock-valuations", InvocationType="RequestResponse", Payload=b"{}")
    d = json.loads(s3.get_object(Bucket=B, Key="data/stock-valuations.json")["Body"].read())
    fills.append({"run": i + 1, "err": r.get("FunctionError", "NONE"),
                   "hp_cov": d.get("hp_coverage"), "hp_uni": d.get("hp_universe")})
    if d.get("hp_coverage") == d.get("hp_universe"):
        break
out["fills"] = fills
out["val"] = {"version": d.get("version"), "n_industries": d.get("n_industries"),
               "underlooked_head": [{k: x.get(k) for k in ("t", "underlooked", "hp_score",
                                       "class", "industry", "turnover_bp", "ps", "rev_yoy_pct")}
                                      for x in (d.get("underlooked_top") or [])[:8]],
               "fund_check": [x.get("industry") for x in (d.get("underlooked_top") or [])]}
# purge CEF papers + state, regenerate
st = json.loads(s3.get_object(Bucket=B, Key="data/_research/state.json")["Body"].read())
for t in ("RQI", "GAB", "BDJ", "BBUC"):
    st.get("done", {}).pop(t, None)
    st["index"] = [e for e in st.get("index", []) if e.get("t") != t]
    try:
        s3.delete_object(Bucket=B, Key=f"data/research/{t}.json")
    except Exception:
        pass
s3.put_object(Bucket=B, Key="data/_research/state.json", Body=json.dumps(st).encode(),
              ContentType="application/json")
r = lam.invoke(FunctionName="justhodl-research-papers", InvocationType="RequestResponse", Payload=b"{}")
out["res_err"] = r.get("FunctionError", "NONE")
idx = json.loads(s3.get_object(Bucket=B, Key="data/research-papers.json")["Body"].read())
out["research"] = {"n_papers": idx.get("n_papers"), "written": idx.get("written_this_run"),
                    "index_head": [{k: p.get(k) for k in ("t", "title", "conviction")}
                                    for p in (idx.get("papers") or [])[:4]],
                    "diag": idx.get("diagnostics")}
if idx.get("papers"):
    pp = json.loads(s3.get_object(Bucket=B, Key=idx["papers"][0]["key"])["Body"].read())
    pa = pp.get("paper") or {}
    out["paper_sample"] = {"t": pp.get("ticker"), "model": pp.get("model_used"),
                            "title": pa.get("title"), "conviction": pa.get("conviction_1_10"),
                            "thesis": (pa.get("one_line_thesis") or "")[:200]}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/1633_fund_purge.json", "w").write(json.dumps(out, indent=1, default=str))
print(json.dumps({"fills": fills[-1], "written": out["research"]["written"]}, default=str))
