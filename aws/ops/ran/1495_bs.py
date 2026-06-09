import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-bs"
code=r'''
import json,urllib.request,ssl
def lambda_handler(e,c):
    ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
    H={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36","Accept":"text/csv;q=0.9, */*;q=0.5"}
    B="https://data-api.ecb.europa.eu/service/data/"
    # candidate ECB total-balance-sheet / total-assets series
    cands={
      "ILM_T000000_Z01":"ILM/W.U2.C.T000000.U2.Z01.A",
      "ILM_total_assets":"ILM/W.U2.C.T000000.Z5.Z01.A",
      "BSI_total_M":"BSI/M.U2.N.C.T00.A.1.Z5.0000.Z01.E",
      "ILM_what_we_use_already":"ILM/W.U2.C.A050000.U2.EUR",
    }
    out={}
    for n,k in cands.items():
        try:
            r=urllib.request.urlopen(urllib.request.Request(B+k+"?format=csvdata&lastNObservations=2",headers=H),timeout=20,context=ctx)
            ls=r.read().decode("utf-8","replace").strip().split("\n")
            out[n]={"status":r.status,"last":ls[-1][:80] if len(ls)>1 else "empty"}
        except urllib.error.HTTPError as ex: out[n]={"status":ex.code}
        except Exception as ex: out[n]={"err":str(ex)[:50]}
    # also: what does ecb-detail.json use for balance_sheet? (reuse its source)
    s3=boto3.client("s3",region_name="us-east-1")
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ecb-detail.json")["Body"].read())
        out["ecb_detail_balance_sheet"]=d.get("balance_sheet")
    except Exception as ex: out["bs_err"]=str(ex)[:50]
    import boto3
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=60,MemorySize=128)
    for _ in range(20):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:120]}
open("aws/ops/reports/1495_bs.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
