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
# get anthropic key from morning-intel
akey=(lam.get_function_configuration(FunctionName="justhodl-morning-intelligence").get("Environment",{}).get("Variables",{}) or {}).get("ANTHROPIC_KEY","")
specs=[("justhodl-journal-grader","aws/lambdas/justhodl-journal-grader/source","cron(0 14 * * ? *)",120,256,False),
       ("justhodl-devils-advocate","aws/lambdas/justhodl-devils-advocate/source","rate(6 hours)",90,256,True)]
for name,src,sched,to,mem,needkey in specs:
    zb=zd(src)
    try:
        lam.get_function_configuration(FunctionName=name); lam.update_function_code(FunctionName=name,ZipFile=zb); st="updated"
    except lam.exceptions.ResourceNotFoundException:
        env={"Variables":{"ANTHROPIC_KEY":akey}} if needkey else {"Variables":{}}
        lam.create_function(FunctionName=name,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",Code={"ZipFile":zb},Timeout=to,MemorySize=mem,Architectures=["x86_64"],Environment=env); st="created"
    for _ in range(25):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=name)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    if needkey and akey:
        env=(lam.get_function_configuration(FunctionName=name).get("Environment",{}).get("Variables",{}) or {})
        if env.get("ANTHROPIC_KEY")!=akey:
            env["ANTHROPIC_KEY"]=akey; lam.update_function_configuration(FunctionName=name,Environment={"Variables":env}); time.sleep(6)
    rn=name+("-daily" if "cron" in sched else "-6h")
    events.put_rule(Name=rn,ScheduleExpression=sched,State="ENABLED")
    fn=lam.get_function(FunctionName=name); events.put_targets(Rule=rn,Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try: lam.add_permission(FunctionName=name,StatementId=f"EB-{rn}",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rn}")
    except lam.exceptions.ResourceConflictException: pass
    out[name]=st
# invoke both
for n in ["justhodl-journal-grader","justhodl-devils-advocate"]:
    try: r=lam.invoke(FunctionName=n,InvocationType="RequestResponse",Payload=b"{}"); out[n+"_run"]=r.get("Payload").read().decode()[:100]
    except Exception as e: out[n+"_run"]=str(e)[:80]
open("aws/ops/reports/1343_d.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
