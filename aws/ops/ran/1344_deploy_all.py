import json, os, time, zipfile, io
import boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
events=boto3.client("events",region_name="us-east-1",config=cfg)
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"; ACC="857687956942"
out={}
def zd(src):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" in r or f.endswith(".pyc"): continue
                fp=os.path.join(r,f); zf.write(fp,arcname=os.path.relpath(fp,src))
    return buf.getvalue()
akey=(lam.get_function_configuration(FunctionName="justhodl-morning-intelligence").get("Environment",{}).get("Variables",{}) or {}).get("ANTHROPIC_KEY","")
specs=[
 ("justhodl-my-brief","cron(30 13 * * ? *)","-daily",True),
 ("justhodl-regime-playbook","cron(30 14 * * ? *)","-daily",False),
 ("justhodl-narrative-vs-tape","rate(4 hours)","-4h",False),
 ("justhodl-position-sizer","rate(6 hours)","-6h",False),
 ("justhodl-engine-conflicts","rate(6 hours)","-6h",False),
]
for name,sched,suf,needkey in specs:
    src=f"aws/lambdas/{name}/source"; zb=zd(src)
    try:
        lam.get_function_configuration(FunctionName=name); lam.update_function_code(FunctionName=name,ZipFile=zb); st="updated"
    except lam.exceptions.ResourceNotFoundException:
        env={"Variables":{"ANTHROPIC_KEY":akey}} if needkey else {"Variables":{}}
        lam.create_function(FunctionName=name,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=90,MemorySize=256,Architectures=["x86_64"],Environment=env); st="created"
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=name)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    if needkey and akey:
        env=(lam.get_function_configuration(FunctionName=name).get("Environment",{}).get("Variables",{}) or {})
        if env.get("ANTHROPIC_KEY")!=akey: env["ANTHROPIC_KEY"]=akey; lam.update_function_configuration(FunctionName=name,Environment={"Variables":env}); time.sleep(6)
    rn=name+suf; events.put_rule(Name=rn,ScheduleExpression=sched,State="ENABLED")
    fn=lam.get_function(FunctionName=name); events.put_targets(Rule=rn,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=name,StatementId=f"EB-{rn}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rn}")
    except lam.exceptions.ResourceConflictException: pass
    out[name]=st
# invoke the non-AI ones to confirm
for n in ["justhodl-regime-playbook","justhodl-narrative-vs-tape","justhodl-position-sizer","justhodl-engine-conflicts"]:
    try: r=lam.invoke(FunctionName=n,InvocationType="RequestResponse",Payload=b"{}"); out[n+"_run"]=r.get("Payload").read().decode()[:90]
    except Exception as e: out[n+"_run"]=str(e)[:80]
open("aws/ops/reports/1344_da.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
