# ops 1577 — deploy builder (stats+staleness on all 31) + engine v3.3.1; invoke both; verify
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1577, "errors": []}

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
    for _ in range(60):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

# deployed-builder forensic: how many series + does it write percentile?
try:
    url = lam.get_function(FunctionName="justhodl-ecb-history")["Code"]["Location"]
    import urllib.request
    zf = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(url, timeout=60).read()))
    src = zf.read("lambda_function.py").decode()
    out["deployed_builder"] = {"n_series_tuples": src.count('("'),
                                "writes_percentile": '"percentile"' in src,
                                "has_stale_days": "stale_days" in src,
                                "bytes": len(src)}
except Exception as e:
    out["deployed_builder"] = f"ERR {str(e)[:80]}"

for fn, src in (("justhodl-ecb-history", "aws/lambdas/justhodl-ecb-history/source"),
                ("justhodl-ecb-derived", "aws/lambdas/justhodl-ecb-derived/source")):
    rc(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zs(src))); ready(fn)

r = rc(lambda: lam.invoke(FunctionName="justhodl-ecb-history", InvocationType="RequestResponse", Payload=b"{}"))
out["hist_err"] = r.get("FunctionError","NONE")
if out["hist_err"]!="NONE": out["hist_payload"]=r["Payload"].read().decode()[:400]
r = rc(lambda: lam.invoke(FunctionName="justhodl-ecb-derived", InvocationType="RequestResponse", Payload=b"{}"))
out["eng_err"] = r.get("FunctionError","NONE")
if out["eng_err"]!="NONE": out["eng_payload"]=r["Payload"].read().decode()[:400]
time.sleep(2)

mf = json.loads(s3.get_object(Bucket=B, Key="data/ecb-hist/_manifest.json")["Body"].read())
ser = mf.get("series") or []
missing_stats = [x["id"] for x in ser if x.get("latest") is None or x.get("percentile") is None]
out["manifest"] = {"n": len(ser), "missing_stats": missing_stats,
                    "sample": [{k: x.get(k) for k in ("id","freq","latest","percentile",
                                "z_score","stale_days","discontinued")} for x in ser[:4]],
                    "n_discontinued": sum(1 for x in ser if x.get("discontinued"))}
d = json.loads(s3.get_object(Bucket=B, Key="data/ecb-derived.json")["Body"].read())
ind = d.get("indicators") or {}
cu = ind.get("country_unemployment") or {}
out["radar"] = {"version": d.get("version"), "n_sparks": d.get("n_sparks"),
                 "spark_misses": d.get("spark_misses"),
                 "countries": sorted((cu.get("countries") or {}).keys()),
                 "ch": (cu.get("countries") or {}).get("CH"),
                 "eurostat_debug": d.get("_eurostat_debug"),
                 "n_charts": len(d.get("charts") or {}),
                 "ai_error": (d.get("ai_brief") or {}).get("error")}
open("aws/ops/reports/1577_hist_heal.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(out, default=str)[:1300])
