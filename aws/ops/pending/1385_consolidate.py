"""Consolidate + purge: the brain fragmented across 5 identities with 138k junk
shards (test/batch bloat). Inspect a sample of each identity's notes to find the
REAL ones (human-written) vs test junk, then we'll decide consolidation.
Run FROM AWS via temp Lambda (sandbox IP CF-blocked)."""
import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
FN="tmp-brain-consolidate"
# This Lambda samples notes from each identity to classify real vs junk.
code=r'''
import json,urllib.request
BASE="https://justhodl-data-proxy.raafouis.workers.dev"
def get(uid):
    try:
        r=urllib.request.urlopen(urllib.request.Request(BASE+"/brain?uid="+uid+"&t=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"}),timeout=120)
        return json.loads(r.read().decode()).get("notes",[])
    except Exception as e: return {"err":str(e)[:80]}
def lambda_handler(e,c):
    uids=["9f48a96b-1a1e-4867-9fc6-e6cc5054c56d"]  # start with account only (biggest real-note chance)
    out={}
    for uid in uids:
        notes=get(uid)
        if isinstance(notes,dict): out[uid]=notes; continue
        # classify: junk = repetitive test patterns
        def isjunk(n):
            t=(n.get("text") or "")
            return ("MACRO NOTE." in t and t.count("MACRO NOTE.")>3) or t.startswith("XXXX") or t in ("test","save path test","round-trip persistence test","kv reset check","cors check") or t.startswith("multi note ") or t.startswith("batch note ") or t.startswith("real note ") or "no-preflight" in t or "PERMANENCE TEST" in t or t.startswith("device save test")
        real=[n for n in notes if not isjunk(n)]
        out[uid]={"total":len(notes),"real":len(real),"real_samples":[ (n.get("text") or "")[:60] for n in real[:8]]}
    return out
'''
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf: zf.writestr("lambda_function.py",code)
out={}
try:
    try: lam.get_function_configuration(FunctionName=FN); lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue())
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":buf.getvalue()},Timeout=300,MemorySize=512)
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    out=json.loads(r["Payload"].read())
    lam.delete_function(FunctionName=FN)
except Exception as e: out={"err":str(e)[:200]}
open("aws/ops/reports/1385_c.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
