"""ops 4530 — Khalid: 'lots of datasets missing, Perplexity told you how to
generate it' = the E12 map generation. +13 SIGS deployed to rollup ->
regenerate engine-provider-map -> re-inventory catalog -> six zeros fill."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4530,"started":datetime.now(timezone.utc).isoformat()}
def deploy(fn):
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
        time.sleep(6)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(f"aws/lambdas/{fn}/source/lambda_function.py","lambda_function.py")
        for f in os.listdir("aws/shared"):
            if f.endswith(".py"): z.write("aws/shared/"+f,f)
    for _ in range(6):
        try: lam.update_function_code(FunctionName=fn,ZipFile=buf.getvalue()); break
        except lam.exceptions.ResourceConflictException: time.sleep(12)
    for _ in range(20):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
        time.sleep(5)
deploy("justhodl-provenance-rollup")
i1=lam.invoke(FunctionName="justhodl-provenance-rollup",InvocationType="RequestResponse",Payload=b"{}")
R["rollup"]={"fn_err":i1.get("FunctionError")}; _=i1["Payload"].read()
time.sleep(4)
try:
    m=json.loads(s3.get_object(Bucket=B,Key="data/audit/engine-provider-map.json")["Body"].read())
    mm=m.get("map",m)
    provs=set()
    for v in mm.values():
        if isinstance(v,list): provs.update(x for x in v if isinstance(x,str))
    R["map_providers"]=sorted(provs)
    R["map_n"]=len(mm)
except Exception as e: R["map_err"]=str(e)[:90]
deploy("justhodl-provider-catalog")
i2=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
R["catalog"]={"fn_err":i2.get("FunctionError")}; _=i2["Payload"].read()
time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
R["totals"]=hub.get("totals")
R["zero"]=[p["slug"] for p in hub["providers"] if not p["n_keys"]]
R["newly"]={p["slug"]:{"n":p["n_keys"],"hot":p.get("hot_feeds")} for p in hub["providers"]
            if p["slug"] in ("worldbank","dbnomics","snb","bcb","cboe","boj") and p["n_keys"]}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("MAP REGENERATED (+13 SIGS: worldbank/dbnomics/snb/bcb/cboe/statcan/banxico/boe/gdelt/"
  f"eiopa/nasa/dol/occ) + last-good join fallback. map_n={R.get('map_n')} providers_in_map="
  f"{json.dumps(R.get('map_providers'))[:200]} · hub totals={json.dumps(R.get('totals'))} newly_filled="
  f"{json.dumps(R.get('newly'))[:200]} still_zero={json.dumps(R.get('zero'))[:150]}. Verify hub + seal E12-ext."),
 "evidence":[{"kind":"log","ref":"data/provider-catalog.json","snippet":"totals"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"totals={json.dumps(R.get('totals'))} newly={json.dumps(R.get('newly'))} zero={R.get('zero')}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4530_map_regen.json","w"),indent=1,default=str)
open("aws/ops/reports/4530_map_regen.md","w").write("# 4530 — "+R["verdict"]+"\n- map_providers: "+json.dumps(R.get("map_providers"),default=str)+"\n")
print(R["verdict"][:300])
