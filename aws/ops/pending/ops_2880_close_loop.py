"""ops 2880 — verify swap-line canary, recompile brain (expect gaps=0), final strategist capstone check."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"
R={"ops":2880,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
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
def wait_deployed(fn, after_iso):
    """Poll until the function's LastModified is after our push (deploy workflow done)."""
    for _ in range(50):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastModified","")>after_iso and c.get("LastUpdateStatus")=="Successful": return True
        time.sleep(6)
    return False

@guard("swap_canary")
def swap_canary():
    R["grid_deployed"]=wait_deployed("justhodl-canary-grid", R["ts"][:19])
    lam.invoke(FunctionName="justhodl-canary-grid",InvocationType="RequestResponse")["Payload"].read()
    time.sleep(3)
    cg=json.loads(s3.get_object(Bucket=B,Key="data/canary-grid.json")["Body"].read())
    sw=next((s for s in cg.get("signals",[]) if s.get("key")=="swap_line_usage"),None)
    R["swap_line"]={"avail":sw.get("available"),"val":sw.get("value"),"stress":sw.get("stress"),"age":sw.get("age_days")} if sw else "missing"
    R["grid"]={"avail":cg.get("n_available"),"total":cg.get("n_total"),"ew":cg.get("early_warning_level")}
    return True

@guard("registry_refresh")
def registry_refresh():
    """swap_line_usage must be in the registry before recompiling."""
    import re, glob
    from collections import Counter
    reg={}; STOP={"THE","AND","FOR","WITH","FROM","THIS","THAT","JSON","HTTP","POST","TRUE","NONE","DATA","NULL","UTF","ISO","EOF","GET","PUT"}
    KEY_STOP={"data","json","name","value","label","text","note","title","true","false","none","null","body","type","status","error","engine","version","level","score","read","source","detail"}
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
    return True

@guard("recompile")
def recompile():
    p=lam.invoke(FunctionName="justhodl-brain-compiler",InvocationType="RequestResponse")
    R["invoke"]=p["Payload"].read().decode()[:150]
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]=d.get("summary")
    R["build_queue"]=[{"concept":b["concept"],"n":b["n_claims"]} for b in (d.get("build_queue") or [])]
    return True

@guard("strategist")
def strategist():
    R["strat_deployed"]=wait_deployed("justhodl-strategist", "2026-07-05T15:50")
    p=lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse"); p["Payload"].read()
    time.sleep(2)
    raw=s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read().decode()
    R["strategist_capstones"]={k:(k in raw) for k in ("canary-warroom","liquidity-inflection","cycle-clock","nowcast-desk")}
    return True

swap_canary(); registry_refresh(); recompile(); strategist()
gaps=(R.get("summary",{}) or {}).get("gaps")
R["status"]="LOOP_CLOSED" if (gaps==0 and not R["errors"]) else ("LIVE" if not R["errors"] else "PARTIAL")
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2880_close_loop.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2880 COMPLETE")
