"""ops 2881 — final: full-key matcher live, recompile (expect gaps=0 LOOP_CLOSED), strategist introspection, page check."""
import os, io, json, time, zipfile, traceback, urllib.request, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-brain-compiler"
R={"ops":2881,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-400:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=330,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"

@guard("deploy_compiler")
def deploy_compiler():
    src=open("aws/lambdas/justhodl-brain-compiler/source/lambda_function.py",encoding="utf-8").read()
    buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close()
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    for _ in range(40):
        if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": return True
        time.sleep(3)
    return False

@guard("recompile")
def recompile():
    p=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    R["invoke"]=p["Payload"].read().decode()[:150]
    if p.get("FunctionError"): R["errors"]["compiler_fn"]=R["invoke"]; return None
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]=d.get("summary")
    R["build_queue"]=[{"concept":b["concept"],"n":b["n_claims"]} for b in (d.get("build_queue") or [])]
    # spot-check the swap-lines claim routing
    sw=[c for c in (d.get("claims") or []) if "swap lines" in (c.get("concepts") or [])]
    R["swap_claim"]={"status":sw[0]["status"],"eng":[e["engine"] for e in sw[0]["engines"][:2]]} if sw else "not-found"
    return True

@guard("strategist")
def strategist():
    # wait for THIS push's deploy (workflow) to land
    t_push=R["ts"][:19]
    for _ in range(45):
        c=lam.get_function_configuration(FunctionName="justhodl-strategist")
        if c.get("LastModified","")>t_push and c.get("LastUpdateStatus")=="Successful": break
        time.sleep(6)
    p=lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse"); p["Payload"].read()
    time.sleep(2)
    raw=s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read().decode()
    d=json.loads(raw)
    R["strategist"]={"generated_at":d.get("generated_at"),"top_keys":list(d.keys())[:14],
        "n_feeds":d.get("n_feeds") or d.get("feeds") or d.get("n_inputs"),
        "capstones_in_output":{k:(k in raw) for k in ("canary-warroom","liquidity-inflection","cycle-clock","nowcast-desk")},
        "consensus_like":{k:d.get(k) for k in ("consensus","call","verdict","headline","stance") if d.get(k) is not None}}
    return True

@guard("swap_unit")
def swap_unit():
    lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
    time.sleep(3)
    cg=json.loads(s3.get_object(Bucket=B,Key="data/canary-grid.json")["Body"].read())
    sw=next((s for s in cg.get("signals",[]) if s.get("key")=="swap_line_usage"),{})
    R["swap_line"]={"unit":sw.get("unit"),"val":sw.get("value"),"avail":sw.get("available")}
    R["grid"]={"avail":cg.get("n_available"),"total":cg.get("n_total"),"ew":cg.get("early_warning_level")}
    return True

@guard("page")
def page():
    req=urllib.request.Request("https://justhodl.ai/brain-compiler.html",headers={"User-Agent":"Mozilla/5.0"})
    h=urllib.request.urlopen(req,timeout=20).read().decode("utf-8","ignore")
    R["page_live"]=("Brain Compiler" in h)
    return True

deploy_compiler(); recompile(); strategist(); swap_unit(); page()
gaps=(R.get("summary",{}) or {}).get("gaps")
R["status"]="LOOP_CLOSED" if (gaps==0 and not R["errors"]) else ("LIVE" if not R["errors"] else "PARTIAL")
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2881_loop_closed.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2881 COMPLETE")
