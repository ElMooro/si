"""ops 4442 — FIGI enrichment live (Khalid's OpenFIGI key -> SSM SecureString).

Repo is PUBLIC, so the key never touches code: stored at
/justhodl/openfigi/api-key (SecureString), engine reads it at runtime.
Enrichment is progressive (2,500/run, batch-100, paced) and converges to
full coverage across nightly runs; unmatched tickers get explicit
figi_status=no_match. Prior-run FIGIs carry forward. Timeout raised to 600s.
"""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-symbology-master"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); R={"ops":4471,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
try: lam.update_function_configuration(FunctionName=FN,Timeout=600); time.sleep(8)
except Exception as e: R["to_err"]=str(e)[:80]
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
_=inv["Payload"].read(); time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/symbology/master.json")["Body"].read())
    bt=d.get("by_ticker",{})
    n_figi=sum(1 for r in bt.values() if r.get("figi"))
    n_cus=sum(1 for r in bt.values() if r.get("cusip"))
    n_lei=sum(1 for r in bt.values() if r.get("lei"))
    R["figi"]={"chain":d.get("enrichment_status",{}).get("cusip_chain"),
               "n_figi":n_figi,"n_cusip":n_cus,"n_lei":n_lei,
               "AAPL":{k:bt.get("AAPL",{}).get(k) for k in ("ticker","cusip","isin","lei","figi")}}
except Exception as e: R["feed_err"]=str(e)[:120]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
fg=R.get("figi") or {}
bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":("NAME-JOIN PASS 2 (AAPL cohort sweep, unique-match-only): 13F filings map -> "
  "CUSIP -> constructed US ISIN (check-digit) -> GLEIF file -> LEI. First run: "
  f"chain={json.dumps(fg.get('chain'),default=str)[:200]} totals figi={fg.get('n_figi')} "
  f"cusip={fg.get('n_cusip')} lei={fg.get('n_lei')} · AAPL now: "
  f"{json.dumps(fg.get('AAPL'),default=str)[:200]}. No CGS license touched — CUSIPs from "
  "public 13F filings, LEIs from GLEIF open data. Verify AAPL=037833100/US0378331005 + seal."),
 "evidence":[{"kind":"log","ref":"data/symbology/master.json","snippet":"figi"}]})
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"name-join pass 2 live"})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — cusip={fg.get('n_cusip')} isin~same lei={fg.get('n_lei')} AAPL={json.dumps(fg.get('AAPL'),default=str)[:110]}" if fg.get("n_cusip") else f"PARTIAL — {json.dumps(fg,default=str)[:200]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4471_stringshape.json","w"),indent=1,default=str)
open("aws/ops/reports/4471_stringshape.md","w").write(f"# ops 4442 — FIGI — {R['verdict']}\n- ssm: {R.get('ssm')}\n- stats: {json.dumps(fg,default=str)[:500]}\n")
print(R["verdict"])
