"""ops 4533 — runner-grep every engine's literal data/*.json put keys ->
overrides artifact; deploy catalog (union); re-inventory; six fill."""
import io,json,os,re,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-provider-catalog"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4533,"started":datetime.now(timezone.utc).isoformat()}
keyrx=re.compile(r'Key\s*=\s*f?["\']([^"\']+)')
writes={}
for d in sorted(os.listdir("aws/lambdas")):
    f=f"aws/lambdas/{d}/source/lambda_function.py"
    if not os.path.exists(f): continue
    try: src=open(f,encoding="utf-8",errors="replace").read()
    except Exception: continue
    ks=sorted({m.group(1) for m in keyrx.finditer(src)
               if m.group(1).startswith("data/") and m.group(1).endswith(".json")
               and "{" not in m.group(1) and "%" not in m.group(1)})
    if ks: writes[d]=ks[:40]
R["n_engines_with_writes"]=len(writes)
s3.put_object(Bucket=B,Key="data/audit/engine-writes-overrides.json",
    Body=json.dumps({"generated":R["started"],"n":len(writes),"writes":writes},default=str).encode(),
    ContentType="application/json")
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
i2=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["fn_err"]=i2.get("FunctionError"); _=i2["Payload"].read()
time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
R["totals"]=hub.get("totals")
R["zero"]=[p["slug"] for p in hub["providers"] if not p["n_keys"]]
R["newly"]={p["slug"]:p["n_keys"] for p in hub["providers"]
            if p["slug"] in ("worldbank","dbnomics","snb","bcb","cboe","boj") and p["n_keys"]}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":(f"MULTI-PROVIDER JOIN (pv[0] bug fixed) ({R['n_engines_with_writes']} engines' literal put-keys unioned into join) — "
  f"totals={json.dumps(R['totals'])} newly={json.dumps(R['newly'])} still_zero={json.dumps(R['zero'])[:130]}. "
  "Verify+seal; remaining zeros should be external blocks only."),
 "evidence":[{"kind":"log","ref":"data/audit/engine-writes-overrides.json","snippet":"n"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"engines_w={R['n_engines_with_writes']} totals={json.dumps(R['totals'])} newly={json.dumps(R['newly'])} zero={R['zero']}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4533_ov.json","w"),indent=1,default=str)
open("aws/ops/reports/4533_ov.md","w").write("# 4533 — "+R["verdict"]+"\n")
print(R["verdict"][:300])
