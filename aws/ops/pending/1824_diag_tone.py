import io,json,zipfile,os,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-fomc-reaction"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":3}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait():
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        time.sleep(5)
# pull fed-speak's working key (try both env var names)
fs=lam.get_function_configuration(FunctionName="justhodl-fed-speak").get("Environment",{}).get("Variables",{})
fskey=fs.get("ANTHROPIC_KEY") or fs.get("ANTHROPIC_API_KEY")
print("fed-speak key present:",bool(fskey),"prefix:",(fskey[:7] if fskey else None),"len:",(len(fskey) if fskey else 0))
wait()
# redeploy code (diagnostic build)
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
# set fed-speak key (cleaned)
cur=lam.get_function_configuration(FunctionName=FN).get("Environment",{}).get("Variables",{})
if fskey: cur["ANTHROPIC_API_KEY"]=fskey.strip()
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":cur}); wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:140])
d=json.loads(s3.get_object(Bucket=B,Key="data/fomc-reaction.json")["Body"].read()); sp=d["surprise"]
print("SURPRISE:",sp["label"],"| basis:",sp.get("basis"))
print("TONE:",json.dumps(sp.get("statement_tone"))[:400])
