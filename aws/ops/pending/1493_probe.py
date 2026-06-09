import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-probe"
code=r'''
import json,urllib.request,ssl
def lambda_handler(e,c):
    ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
    H={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36","Accept":"text/csv;q=0.9, */*;q=0.5","Accept-Language":"en-US,en;q=0.9"}
    B="https://data-api.ecb.europa.eu/service/data/"
    # candidate series for the 3 gaps that need NEW ecb data
    tests={
      "TARGET2_claims_DE":"ILM/W.U2.C.A090400.U2.EUR",     # intra-eurosystem TARGET claims
      "MRO":"ILM/W.U2.C.A050100.U2.EUR",
      "LTRO":"ILM/W.U2.C.A050200.U2.EUR",
      "MLF":"ILM/W.U2.C.A050500.U2.EUR",
      "BLS_standards":"BLS/Q.U2.ALL.O.E.Z.B3.ST.S.WFNET",   # credit standards C&I
      "MIR_nfc_rate":"MIR/M.U2.B.A2A.A.R.A.2240.EUR.N",     # NFC lending rate
      "DFR":"FM/D.U2.EUR.4F.KR.DFR.LEV",                     # deposit facility rate
    }
    out={}
    for name,key in tests.items():
        try:
            u=B+key+"?format=csvdata&lastNObservations=3"
            r=urllib.request.urlopen(urllib.request.Request(u,headers=H),timeout=20,context=ctx)
            body=r.read().decode("utf-8","replace"); lines=body.strip().split("\n")
            out[name]={"status":r.status,"rows":len(lines)-1,"last":lines[-1][:70] if len(lines)>1 else None}
        except urllib.error.HTTPError as ex: out[name]={"status":ex.code,"body":ex.read().decode("utf-8","replace")[:80]}
        except Exception as ex: out[name]={"err":str(ex)[:60]}
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
open("aws/ops/reports/1493_p.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
