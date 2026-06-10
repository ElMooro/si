# ops 1569 — wire alpha stack into synthesis: deploy signal-board (+8 feeds),
# auto-detect bottleneck score field, upload AI-brief registry to S3, verify board
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1569, "errors": []}

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

# A) bottleneck score-field autodetect → patch registry before upload
reg = json.load(open("config/ai-brief-contexts.json"))
try:
    bb = json.loads(s3.get_object(Bucket=B, Key="data/bottleneck-boom.json")["Body"].read())
    r0 = ((bb.get("ranks") or bb.get("top") or [{}])[0]) or {}
    fld = next((k for k in ("boom_score","bottleneck_score","score","composite")
                if isinstance(r0.get(k),(int,float))), "score")
    out["bottleneck_rank0_keys"] = sorted(r0.keys())[:12]
    out["bottleneck_score_field"] = fld
    reg["contexts"]["bottleneck-names"]["primary_score_field"] = fld
    tf = "ranks" if bb.get("ranks") else ("top" if bb.get("top") else "ranks")
    reg["contexts"]["bottleneck-names"]["primary_tickers_field"] = tf
except Exception as e:
    out["bottleneck_detect_err"] = str(e)[:80]
s3.put_object(Bucket=B, Key="config/ai-brief-contexts.json",
              Body=json.dumps(reg, indent=1).encode(), ContentType="application/json",
              CacheControl="no-cache")
out["registry_contexts_uploaded"] = len(reg["contexts"])

# B) deploy + invoke signal-board
rc(lambda: lam.update_function_code(FunctionName="justhodl-signal-board",
    ZipFile=zs("aws/lambdas/justhodl-signal-board/source")))
ready("justhodl-signal-board")
r = rc(lambda: lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"] = r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d = json.loads(s3.get_object(Bucket=B, Key="data/signal-board.json")["Body"].read())
feeds = d.get("feeds") or d.get("signals") or d.get("board") or []
def find(name):
    for f in (feeds if isinstance(feeds, list) else []):
        if isinstance(f, dict) and name.lower() in str(f.get("name","")).lower():
            return {k: f.get(k) for k in ("name","category","signal","score","note","stale","fresh") if k in f}
    return None
NEW = ["Ignition","Bottleneck","Crisis Canaries","Liquidity Inflection","Confluence",
       "Crisis-KB","EU Dump","Inclusion"]
out["new_feeds"] = {n: find(n) for n in NEW}
out["board_top_keys"] = sorted(d.keys())[:14]
out["composite"] = {k: d.get(k) for k in ("composite","composite_signal","posture","categories","n_feeds","n_fresh","n_stale") if k in d}
# verify registry roundtrip
reg2 = json.loads(s3.get_object(Bucket=B, Key="config/ai-brief-contexts.json")["Body"].read())
out["registry_s3_has_new"] = all(k in reg2["contexts"] for k in
    ("ignition-names","bottleneck-names","canaries-decisive-call",
     "liquidity-inflection-decisive-call","eu-dump-decisive-call","confluence-decisive-call"))
open("aws/ops/reports/1569_synthesis.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn_err":out["fn_err"],"reg_n":out["registry_contexts_uploaded"],
  "reg_ok":out["registry_s3_has_new"],"bb_field":out.get("bottleneck_score_field"),
  "new":{k:(v or "MISSING") for k,v in out["new_feeds"].items()}},default=str)[:900])
