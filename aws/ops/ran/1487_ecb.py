"""Verify the audit's ECB claims: does the fetch succeed, does the data have
history, is ecb-detail.json really 2KB. From AWS (real Lambda IP, not sandbox)."""
import json, os, time, zipfile, io
import boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-ecb"
out={}
# 1) what's in ecb-detail.json — size + history?
for k in ["data/ecb-detail.json","data/euro-fragmentation.json","data/eurodollar-stress.json","data/systemic-stress.json"]:
    try:
        o=s3.get_object(Bucket="justhodl-dashboard-live",Key=k); raw=o["Body"].read()
        d=json.loads(raw); age=round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)
        info={"bytes":len(raw),"age_h":age,"keys":list(d.keys())[:14] if isinstance(d,dict) else f"list[{len(d)}]"}
        # does any value have a history/series array?
        if isinstance(d,dict):
            hist=[kk for kk in d if isinstance(d.get(kk),list) and len(d[kk])>10]
            info["history_arrays"]=hist[:6]
            # deep check for nested history
            for kk,vv in list(d.items())[:30]:
                if isinstance(vv,dict):
                    for k2,v2 in vv.items():
                        if isinstance(v2,list) and len(v2)>20: info.setdefault("nested_history",[]).append(f"{kk}.{k2}[{len(v2)}]")
        out[k]=info
    except Exception as e: out[k]={"MISSING":str(e)[:40]}
# 2) does a LIVE ECB fetch succeed from AWS? test the actual ECB API with Mozilla UA + csvdata
code=r'''
import json,urllib.request,ssl
def lambda_handler(e,c):
    ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
    out={}
    tests={
      "ILM_jsondata":"https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C.A030000.U2.Z06?format=jsondata&lastNObservations=5",
      "ILM_csvdata_1997":"https://data-api.ecb.europa.eu/service/data/ILM/W.U2.C.A030000.U2.Z06?format=csvdata&startPeriod=1997-01-01",
      "CISS_csv":"https://data-api.ecb.europa.eu/service/data/CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX?format=csvdata&startPeriod=1997-01-01",
    }
    H={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36","Accept":"application/vnd.sdmx.data+json;version=1.0.0-wd, text/csv;q=0.9, */*;q=0.5","Accept-Language":"en-US,en;q=0.9"}
    for name,u in tests.items():
        try:
            r=urllib.request.urlopen(urllib.request.Request(u,headers=H),timeout=25,context=ctx)
            body=r.read().decode("utf-8","replace")
            # count data points
            n_csv=body.count("\n")-1 if "csvdata" in u else None
            first=body[:80]; last=body.strip().split("\n")[-1][:60] if "\n" in body else ""
            out[name]={"status":r.status,"bytes":len(body),"n_rows":n_csv,"first":first[:50],"last_row":last}
        except urllib.error.HTTPError as ex: out[name]={"status":ex.code,"body":ex.read().decode("utf-8","replace")[:100]}
        except Exception as ex: out[name]={"err":str(ex)[:80]}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["LIVE_ECB_FETCH"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out["fetch_err"]=str(e)[:120]
open("aws/ops/reports/1487_ecb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
