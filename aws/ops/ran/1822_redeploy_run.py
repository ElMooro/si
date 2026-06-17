import io,json,zipfile,os,time,boto3
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-fomc-reaction"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":3}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait_active():
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return
        time.sleep(6)
wait_active()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    added=set(); src="aws/lambdas/%s/source"%FN
    for root,_,files in os.walk(src):
        for f in files:
            if f.endswith(".pyc"):continue
            p=os.path.join(root,f);arc=os.path.relpath(p,src);z.write(p,arc);added.add(arc)
    for f in os.listdir("aws/shared"):
        if f.endswith(".py") and f not in added: z.write(os.path.join("aws/shared",f),f)
lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); wait_active()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("invoke:",r["Payload"].read().decode()[:200])
d=json.loads(s3.get_object(Bucket=B,Key="data/fomc-reaction.json")["Body"].read())
sp=d["surprise"]
print("\nSURPRISE:",sp["label"],"| basis:",sp.get("basis"),"| Δ2y_bp:",sp.get("d2y_change_bp"),"2y_fresh:",sp.get("two_y_fresh"))
print("tone:",json.dumps(sp.get("statement_tone"))[:200])
print("\nREACTION MAP ("+sp["label"]+"):")
for k,v in d["reaction_map"].items():
    s=v.get("short") or {}; l=v.get("long") or {}
    su="—" if not s else f"{s['median']:+g}{v['unit']} [{s['p25']:+g}..{s['p75']:+g}] up{s['prob_up_pct']}% n{s['n']}"
    lu="—" if not l else f"{l['median']:+g}{v['unit']} [{l['p25']:+g}..{l['p75']:+g}] up{l['prob_up_pct']}% n{l['n']}"
    print(f"  {k:24} 5d {su:46} 63d {lu}")
