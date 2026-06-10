# ops 1563 — kb v1.0.3 exhaustive harvester + inclusion v1.0.3; verify framework names + skeleton
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1563, "errors": []}

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
    for _ in range(40):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

for fn,src in (("justhodl-kb-matcher","aws/lambdas/justhodl-kb-matcher/source"),
               ("justhodl-index-inclusion","aws/lambdas/justhodl-index-inclusion/source")):
    rc(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zs(src))); ready(fn)

v = {}
r = rc(lambda: lam.invoke(FunctionName="justhodl-kb-matcher", InvocationType="RequestResponse", Payload=b"{}"))
v["kb_err"]=r.get("FunctionError","NONE")
if v["kb_err"]!="NONE": v["kb_payload"]=r["Payload"].read().decode()[:400]
kb=json.loads(s3.get_object(Bucket=B,Key="data/kb-match.json")["Body"].read())
sm=kb.get("schema_map") or {}
v["kb"]={"stats":kb.get("kb_stats"),"n_named":sm.get("n_named_nodes"),"named":sm.get("named"),
  "skeleton":json.dumps(sm.get("kb_skeleton"),default=str)[:900],
  "top":[(f.get("framework"),f.get("match_pct"),f.get("n_evaluable"),f.get("n_rules")) for f in (kb.get("top_matches") or [])[:5]],
  "top1_matched":((kb.get("top_matches") or [{}])[0]).get("matched_rules"),
  "top1_next":((kb.get("top_matches") or [{}])[0]).get("next_triggers")}

r = rc(lambda: lam.invoke(FunctionName="justhodl-index-inclusion", InvocationType="RequestResponse", Payload=b"{}"))
v["ii_err"]=r.get("FunctionError","NONE")
ii=json.loads(s3.get_object(Bucket=B,Key="data/index-inclusion.json")["Body"].read())
v["ii"]={"eligible":ii.get("n_eligible"),
  "head":[(w.get("ticker"),w.get("name"),w.get("mcap_bn")) for w in (ii.get("watch_list") or []) if w.get("passes_profit_rule")][:10]}

out["verify"]=v
open("aws/ops/reports/1563_kb_final.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(v,default=str)[:1400])
