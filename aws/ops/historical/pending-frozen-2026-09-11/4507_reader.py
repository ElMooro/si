"""ops 4507 — surface the clipped results (4501/4502 tails died mid-report)
+ probe the treasury/bls wing shape + self-heal canary if its first invoke
was lost. Read-mostly; one conditional invoke."""
import gzip,json,os,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
s3=boto3.client("s3",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
R={"ops":4507,"as_of":datetime.now(timezone.utc).isoformat()}
def gj(k):
    b=s3.get_object(Bucket=BUCKET,Key=k)["Body"].read()
    if k.endswith(".gz"): b=gzip.decompress(b)
    return json.loads(b)
try:
    g=gj("data/warm/global-expansion-summary.json")
    R["occ"]=g.get("occ"); R["sec_midas"]=g.get("sec_midas")
except Exception as e: R["exp_err"]=str(e)[:80]
try:
    ny=gj("data/warm/nyfed-markets/latest-summary.json")
    b=ny.get("bounded") or {}
    R["seclending"]=b.get("seclending_latest"); R["ambs"]=b.get("ambs_latest")
except Exception as e: R["ny_err"]=str(e)[:80]
try:
    c=gj("data/canary-macro.json")
    vals=sum(1 for v in c.values() if isinstance(v,dict) and v.get("value") is not None)
    R["canary"]={"present":True,"hot_series":vals,"flags":c.get("flags"),
                 "SAHM":(c.get("SAHMREALTIME") or {}).get("value"),
                 "T10Y3M":(c.get("T10Y3M") or {}).get("value"),
                 "ICSA":(c.get("ICSA") or {}).get("value")}
except Exception:
    inv=lam.invoke(FunctionName="justhodl-canary-macro",InvocationType="RequestResponse",Payload=b"{}")
    bb=json.loads(inv["Payload"].read().decode())
    rn=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
    R["canary"]={"present":False,"reinvoked":True,
                 "hot_series":(rn or {}).get("hot_series"),"flags":(rn or {}).get("flags")}
try:
    t=gj("data/warm/treasury/debt_to_penny.json.gz")
    R["treasury_warm_keys"]=sorted(list(t.keys()))[:8]
    obs=t.get("observations")
    R["treasury_obs_shape"]=("list["+str(len(obs))+"]" if isinstance(obs,list) else type(obs).__name__)
except Exception as e: R["treasury_probe"]=str(e)[:90]
try:
    bl=gj("data/warm/usgov/bls/CUUR0000SA0.json.gz")
    R["bls_warm_keys"]=sorted(list(bl.keys()))[:8]
except Exception as e: R["bls_probe"]=str(e)[:90]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("CLIPPED RESULTS SURFACED (ops 4507): seclending="
  +json.dumps(R.get("seclending"),default=str)[:120]+" ambs="+json.dumps(R.get("ambs"),default=str)[:120]
  +" · occ="+json.dumps(R.get("occ"),default=str)[:140]+" · midas="+json.dumps(R.get("sec_midas"),default=str)[:140]
  +" · canary="+json.dumps(R.get("canary"),default=str)[:200]
  +" · wing-probe treas_keys="+json.dumps(R.get("treasury_warm_keys"))[:90]
  +" bls_keys="+json.dumps(R.get("bls_warm_keys"))[:90]+". Verify+seal each; wing patch next if shapes mismatch."),
 "evidence":[{"kind":"log","ref":"data/canary-macro.json","snippet":"flags"}]})
bus({"action":"fanout_pending"})
R["verdict"]="READ — "+json.dumps({k:bool(R.get(k)) for k in ("seclending","ambs","occ","sec_midas")})+" canary="+json.dumps(R.get("canary"),default=str)[:120]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4507_reader.json","w"),indent=1,default=str)
open("aws/ops/reports/4507_reader.md","w").write("# ops 4507 — reader — "+R["verdict"]+"\n"
 +"- seclending: "+json.dumps(R.get("seclending"),default=str)[:200]+"\n"
 +"- ambs: "+json.dumps(R.get("ambs"),default=str)[:200]+"\n"
 +"- occ: "+json.dumps(R.get("occ"),default=str)[:240]+"\n"
 +"- midas: "+json.dumps(R.get("sec_midas"),default=str)[:240]+"\n"
 +"- canary: "+json.dumps(R.get("canary"),default=str)[:260]+"\n"
 +"- treasury_keys: "+json.dumps(R.get("treasury_warm_keys"),default=str)+" obs="+str(R.get("treasury_obs_shape"))+"\n"
 +"- bls_keys: "+json.dumps(R.get("bls_warm_keys"),default=str)+"\n")
print(R["verdict"])
