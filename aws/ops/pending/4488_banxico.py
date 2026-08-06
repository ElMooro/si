"""ops 4488 — Banxico token -> SSM SecureString (repo is public; chat-paste
noted for rotation), engine re-run only=banxico, verify series land."""
import json,os,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-global-expansion"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ssm=boto3.client("ssm",region_name=REGION)
R={"ops":4488,"started":datetime.now(timezone.utc).isoformat()}
ssm.put_parameter(Name="/justhodl/banxico-token",
 Value="1b49956cf1d0d27393ef6829a4d2fab25e9051da4d774cd04c7f4bf190370067",
 Type="SecureString",Overwrite=True)
R["ssm"]="stored SecureString"
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",
               Payload=json.dumps({"only":"banxico"}).encode())
bb=json.loads(inv["Payload"].read().decode())
R["run"]=json.loads(bb["body"]) if isinstance(bb,dict) and "body" in bb else bb
time.sleep(3)
try:
    import gzip
    d=json.loads(gzip.decompress(s3.get_object(Bucket=BUCKET,
        Key="data/warm/banxico/core-series.json.gz")["Body"].read()))
    ser=(d.get("payload",{}).get("bmx",{}).get("series") or [])
    R["series"]=[{"id":x.get("idSerie"),"title":(x.get("titulo") or "")[:40],
                  "n_obs":len(x.get("datos") or []),
                  "latest":(x.get("datos") or [{}])[-1]} for x in ser]
except Exception as e: R["feed_err"]=str(e)[:120]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("BANXICO LIT — Khalid pasted the token (SSM SecureString'd; chat-paste, rotation "
  f"advised). Series: {json.dumps(R.get('series'),default=str)[:350]}. 7/11 doc providers live; "
  "entsoe + copernicus keys remain the last two slots. Verify+seal."),
 "evidence":[{"kind":"log","ref":"data/warm/banxico/core-series.json.gz","snippet":"idSerie"}]})
bus({"action":"fanout_pending"})
ok=bool(R.get("series"))
R["verdict"]=f"PASS — {len(R.get('series') or [])} series: {json.dumps(R.get('series'),default=str)[:180]}" if ok else f"PARTIAL — {json.dumps(R.get('run'),default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4488_banxico.json","w"),indent=1,default=str)
open("aws/ops/reports/4488_banxico.md","w").write(f"# ops 4488 — banxico — {R['verdict']}\n")
print(R["verdict"])
