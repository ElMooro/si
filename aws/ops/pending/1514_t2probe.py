"""Find the correct ECB TARGET2 dataflow. A090400 404s. TARGET2 balances live in
the TGB (TARGET Balances) dataflow or under specific ILM/BSI keys. Probe candidates."""
import json, os, time, zipfile, io, boto3
from botocore.config import Config
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=90)); ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-t2"
code=r'''
import json,urllib.request,ssl
def lambda_handler(e,c):
    ctx=ssl.create_default_context(); ctx.check_hostname=False; ctx.verify_mode=ssl.CERT_NONE
    H={"User-Agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124 Safari/537.36","Accept":"text/csv;q=0.9, */*;q=0.5"}
    B="https://data-api.ecb.europa.eu/service/data/"
    # candidate TARGET2 / intra-eurosystem series across dataflows
    cands={
      # TGB = TARGET balances dataflow (DE Bundesbank claim is the classic fragmentation gauge)
      "TGB_DE_claim":"TGB/M.TG.N.DE.A.A.20.E.E",
      "TGB_generic":"TGB/M.TG.N.U2.A.A.20.E.E",
      # ILM intra-eurosystem alternatives
      "ILM_intra_A":"ILM/W.U2.C.A080000.U2.EUR",
      "ILM_intra_L":"ILM/W.U2.C.L080000.U2.EUR",
      # BSI external/intra positions
      "BSI_extra":"BSI/M.U2.N.C.LT.A.4.U2.2300.Z01.E",
    }
    out={}
    for n,k in cands.items():
        try:
            r=urllib.request.urlopen(urllib.request.Request(B+k+"?format=csvdata&lastNObservations=2",headers=H),timeout=20,context=ctx)
            ls=r.read().decode("utf-8","replace").strip().split("\n")
            out[n]={"status":r.status,"last":ls[-1][:90] if len(ls)>1 else "empty"}
        except urllib.error.HTTPError as ex: out[n]={"status":ex.code}
        except Exception as ex: out[n]={"err":str(ex)[:50]}
    # also list available dataflows matching TARGET
    try:
        u="https://data-api.ecb.europa.eu/service/dataflow/ECB?format=jsondata"
        d=urllib.request.urlopen(urllib.request.Request(u,headers=H),timeout=25,context=ctx).read().decode("utf-8","replace")
        # crude: find TARGET-ish dataflow ids
        import re
        ids=re.findall(r'"id":"(T[A-Z]{1,4})"',d)
        out["target_dataflows"]=list(set(ids))[:15]
    except Exception as ex: out["dataflow_err"]=str(ex)[:50]
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
open("aws/ops/reports/1514_t2.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
