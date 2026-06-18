import io,json,zipfile,os,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-eurodollar-plumbing"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":2}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait():
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        time.sleep(5)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    added=set(); src="aws/lambdas/%s/source"%FN
    for root,_,files in os.walk(src):
        for f in files:
            if f.endswith(".pyc"):continue
            p=os.path.join(root,f);arc=os.path.relpath(p,src);z.write(p,arc);added.add(arc)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added: z.write(os.path.join("aws/shared",f),f)
lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:160])
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("HEALTH",d["plumbing_health"],"VERDICT",d["verdict"],"| reds:",d["red_flags"],"| yellows:",d["yellow_flags"])
ai=d["ai"]; print("AI state:",ai.get("state"),"| summary:",ai.get("summary"))
print("    short_term:",ai.get("short_term"))
for lk,lv in d["layers"].items():
    print(" ",lk,":",", ".join(f"{m['label'].split('(')[0].strip()}={m['value']}{m['unit']}[{m['status']}]" for m in lv["metrics"]))
