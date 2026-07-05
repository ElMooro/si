"""ops 2878 — registry v2 (FRED anywhere + semantic keys), update compiler, recompile, verify strategist."""
import os, io, json, re, glob, time, zipfile, traceback, boto3
from collections import Counter
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-brain-compiler"
R={"ops":2878,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-500:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=170,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
KEY_STOP={"data","json","name","value","label","text","note","title","true","false","none","null","body","type","status","error","engine","version","level","score","read","source","detail","utf_8"}

@guard("registry_v2")
def registry_v2():
    reg={}; STOP={"THE","AND","FOR","WITH","FROM","THIS","THAT","JSON","HTTP","POST","TRUE","NONE","DATA","NULL","UTF","ISO","EOF","GET","PUT"}
    for f in glob.glob("aws/lambdas/*/source/lambda_function.py"):
        eng=f.split("/")[2]
        try: src=open(f,encoding="utf-8",errors="ignore").read()
        except Exception: continue
        m=re.match(r'\s*(?:"""|\'\'\')(.{20,900}?)(?:"""|\'\'\')',src,re.S)
        doc=re.sub(r"\s+"," ",m.group(1)).strip()[:320] if m else ""
        fred=sorted({t for t in re.findall(r'\b([A-Z][A-Z0-9]{3,17})\b',src) if any(ch.isdigit() for ch in t) and t not in STOP})[:60]
        keys=sorted({k for k in re.findall(r'["\']([a-z][a-z0-9_]{3,30})["\']',src) if "_" in k and k not in KEY_STOP})[:90]
        outs=sorted(set(re.findall(r'data/[a-z0-9._-]+\.json',src)))[:12]
        reg[eng]={"doc":doc,"fred":fred,"keys":keys,"outs":outs}
    s3.put_object(Bucket=B,Key="data/engine-registry.json",Body=json.dumps({"generated_at":R["ts"],"version":2,"n_engines":len(reg),"engines":reg},ensure_ascii=False).encode(),ContentType="application/json",CacheControl="max-age=3600")
    R["registry_engines"]=len(reg)
    return True

@guard("update_fn")
def update_fn():
    src=open("aws/lambdas/justhodl-brain-compiler/source/lambda_function.py",encoding="utf-8").read()
    buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close()
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    for _ in range(40):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus")=="Successful": return True
        time.sleep(3)
    return False

@guard("recompile")
def recompile():
    p=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    body=p["Payload"].read().decode(); R["invoke"]=body[:200]
    if p.get("FunctionError"): R["errors"]["compiler_fn"]=body[:500]
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]=d.get("summary")
    R["build_queue"]=[{"concept":b["concept"],"n":b["n_claims"]} for b in (d.get("build_queue") or [])]
    R["gap_claims"]=[{"c":b["concept"],"q":(b.get("sample_claims") or [""])[0][:130]} for b in (d.get("build_queue") or [])[:6]]
    return True

@guard("strategist")
def strategist():
    p=lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse")
    p["Payload"].read()
    time.sleep(2)
    raw=s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read().decode()
    d=json.loads(raw)
    R["strategist"]={"generated_at":d.get("generated_at"),
        "reads_new_capstones":{k:(k in raw) for k in ("canary-warroom","liquidity-inflection","cycle-clock","nowcast-desk")},
        "consensus":d.get("consensus") or d.get("verdict") or d.get("headline")}
    return True

registry_v2(); update_fn(); recompile(); strategist()
R["status"]="LIVE" if not R["errors"] and (R.get("summary",{}) or {}).get("n_claims",0)>0 else ("PARTIAL" if R.get("summary") else "FAILED")
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3300])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2878_recompile.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2878 COMPLETE")
