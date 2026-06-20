"""ops 2051: re-audit coverage with broadened extractor + re-invoke engine."""
import boto3, json, time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import importlib.util
s3=boto3.client("s3","us-east-1"); lam=boto3.client("lambda","us-east-1"); B="justhodl-dashboard-live"
spec=importlib.util.spec_from_file_location("st","aws/lambdas/justhodl-strategist/source/lambda_function.py")
st=importlib.util.module_from_spec(spec); spec.loader.exec_module(st)
man=json.loads(s3.get_object(Bucket=B,Key="data/engine-manifest.json")["Body"].read())
feeds={(e.get("keys") or ["?"])[0]:e["engine"] for e in man.get("engines",[]) if e.get("keys")}
def probe(key):
    try:
        o=s3.get_object(Bucket=B,Key=key); d=json.loads(o["Body"].read())
        age=(datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600
        info=st.extract(d)
        return ("STALE" if age>=240 else ("COVERED" if info else "NOVIEW"))
    except Exception: return "MISSING"
with ThreadPoolExecutor(max_workers=24) as ex: buckets=list(ex.map(probe,list(feeds.keys())))
from collections import Counter; c=Counter(buckets)
print("NEW COVERAGE:",dict(c),"| COVERED(fresh w/ read):",c["COVERED"],"of",len(feeds))
# wait for deploy then invoke
for _ in range(20):
    cf=lam.get_function(FunctionName="justhodl-strategist")["Configuration"]
    if cf.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read())
fl=d["fleet"]; it=d.get("interpretation") or {}
print("\nENGINE NOW: fresh",fl["n_fresh"],"| consensus",fl["consensus"],"| +/-/neu",fl["n_positive"],fl["n_negative"],fl["n_neutral"])
if it.get("raw") or it.get("error"): print("interp:",it.get("parse_note") or it.get("error"))
else:
    print("DRIVER:",it.get("dominant_driver"))
    print("CALL:",str(it.get("decisive_call"))[:200])
    print("CONVICTION:",it.get("conviction"),"| claims:",len(it.get("key_claims") or []))
print("DONE 2051")
