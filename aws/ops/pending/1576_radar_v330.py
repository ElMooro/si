# ops 1576 — deploy ecb-derived v3.3.0; verify countries/sparks/changes; audit ecb-hist store
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1576, "errors": []}

def rc(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException","TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries")

def zs(src):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r: zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    return b.getvalue()

def ready(fn):
    for _ in range(50):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

rc(lambda: lam.update_function_code(FunctionName="justhodl-ecb-derived",
    ZipFile=zs("aws/lambdas/justhodl-ecb-derived/source")))
ready("justhodl-ecb-derived")
r = rc(lambda: lam.invoke(FunctionName="justhodl-ecb-derived", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"] = r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
ind = d.get("indicators") or {}
ch = d.get("charts") or {}
spk = {k: (v.get("spark") or {}).get("pctile") for k, v in ind.items()
       if isinstance(v, dict) and v.get("spark")}
out["verify"] = {
  "version": d.get("version"), "duration_s": d.get("duration_s"),
  "brief_kb": round(len(json.dumps(d, default=str))/1024, 1),
  "country_unemployment": ind.get("country_unemployment"),
  "n_sparks": d.get("n_sparks"), "spark_pctiles": spk,
  "country_charts": {k: {"n": len(ch[k]["points"]), "latest": ch[k]["latest"],
                          "as_of": ch[k]["latest_date"]} for k in
                      ("unemp_de","unemp_fr","unemp_it","unemp_es","unemp_ch") if k in ch},
  "n_charts": len(ch),
  "changes_today": d.get("changes_today"),
  "dump_score": {k: (d.get("dump_score") or {}).get(k) for k in ("score_0_100","level","coverage_pillars")},
  "ai_error": (d.get("ai_brief") or {}).get("error")}
# ecb-hist store audit
mf = None
try:
    mf = json.loads(s3.get_object(Bucket=B, Key="data/ecb-hist/_manifest.json")["Body"].read())
except Exception as e:
    out["manifest_err"] = str(e)[:80]
objs, tok = [], None
while True:
    kw = {"Bucket": B, "Prefix": "data/ecb-hist/", "MaxKeys": 200}
    if tok: kw["ContinuationToken"] = tok
    rr = s3.list_objects_v2(**kw)
    objs += [o["Key"] for o in rr.get("Contents", [])]
    tok = rr.get("NextContinuationToken")
    if not tok: break
out["ecb_hist_audit"] = {
  "manifest_n_series": len((mf or {}).get("series") or []),
  "manifest_sample": ((mf or {}).get("series") or [])[:3],
  "s3_files_n": len(objs),
  "s3_files_sample": [o.split("/")[-1] for o in objs[:12]],
  "freqs_in_manifest": sorted({s_.get("freq") for s_ in ((mf or {}).get("series") or [])})}
open("aws/ops/reports/1576_radar_v330.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"err":out["fn_err"],"v":out["verify"]["version"],
  "countries":(out["verify"]["country_unemployment"] or {}).get("countries"),
  "n_sparks":out["verify"]["n_sparks"],"n_charts":out["verify"]["n_charts"],
  "hist":{k:out["ecb_hist_audit"][k] for k in ("manifest_n_series","s3_files_n","freqs_in_manifest")}},default=str)[:800])
