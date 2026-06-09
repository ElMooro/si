import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=90,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; FN="tmp-cb"
code=r'''
import json,urllib.request,time
PROXY="https://justhodl-data-proxy.raafouis.workers.dev"; BU="brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
def call(method,p,body=None):
    try:
        req=urllib.request.Request(PROXY+p,data=(json.dumps(body).encode() if body else None),headers={"User-Agent":"jh","Origin":"https://justhodl.ai","Content-Type":"text/plain"},method=method)
        r=urllib.request.urlopen(req,timeout=20); return json.loads(r.read().decode())
    except Exception as e: return {"err":str(e)[:60]}
def lambda_handler(e,c):
    out={}
    # 1) page clean (no const-body throw markers; bounded chart)
    try:
        h=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/chart-pro.html?t=9",headers={"User-Agent":"Mozilla/5.0"}),timeout=15).read().decode()
        out["page"]={"let_body":"let body = document.getElementById('setups-body')" in h,"bounded":"flex:1 1 0;min-height:0;overflow:hidden" in h,"ctx_menu":"WatchlistContextMenu" in h,"brain_push":"pushNoteToBrain" in h}
    except Exception as ex: out["page_err"]=str(ex)[:60]
    # 2) simulate writing a chart note to the brain (what setNote->pushNoteToBrain does)
    note={"id":"chart-tsla","cat":"thesis","text":"[TSLA · chart note] testing chart→brain note flow","created":int(time.time()*1000),"source":"chart:TSLA"}
    out["write"]=call("PUT","/brain?uid="+BU,{"note":note})
    time.sleep(1)
    # 3) read brain back — is the chart note there?
    g=call("GET","/brain?uid="+BU+"&t=1")
    notes=g.get("notes",[]) if isinstance(g.get("notes"),list) else []
    out["brain_has_chart_note"]=any(n.get("id")=="chart-tsla" for n in notes)
    out["brain_total"]=len(notes)
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
open("aws/ops/reports/1475_cb.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
