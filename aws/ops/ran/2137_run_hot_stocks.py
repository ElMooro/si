import boto3, json, time, io, zipfile, glob, os
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-hot-stocks-digest"; ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
def zipb():
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.writestr("lambda_function.py",open("aws/lambdas/justhodl-hot-stocks-digest/source/lambda_function.py").read())
        for f in glob.glob("aws/shared/*.py"): z.writestr(os.path.basename(f),open(f).read())
    return buf.getvalue()
try:
    lam.get_function(FunctionName=FN); print("exists; ensuring shared bundled"); lam.update_function_code(FunctionName=FN,ZipFile=zipb())
except lam.exceptions.ResourceNotFoundException:
    print("create"); lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":zipb()},Timeout=180,MemorySize=512,Architectures=["x86_64"])
    for _ in range(20):
        if lam.get_function(FunctionName=FN)["Configuration"].get("State")=="Active": break
        time.sleep(3)
    rule="justhodl-hot-stocks-digest-am"; ev.put_rule(Name=rule,ScheduleExpression="cron(30 12 * * ? *)",State="ENABLED")
    try: lam.add_permission(FunctionName=FN,StatementId="ev-"+rule,Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{rule}")
    except Exception as e: print("perm",str(e)[:40])
    ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]}])
    print("created+scheduled")
for _ in range(25):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("LastUpdateStatus")=="Successful" and c.get("State")=="Active": break
    time.sleep(3)
t=time.time(); r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["Payload"].read().decode()[:200],f"({time.time()-t:.0f}s)")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/hot-stocks-digest.json")["Body"].read())
print("narrative:",d.get("llm_narrative"),"| hot:",len(d.get("hot_stocks",[])))
print("MARKET READ:",(d.get("market_read") or "")[:160])
print("\nHOT STOCKS:")
for s in d.get("hot_stocks",[])[:8]:
    a=s.get("analyst") or {}
    print(f"  {s['ticker']:<6} heat={round(s['score'])} ven={s.get('venue_count')} bull%={s.get('bull_pct')}  NET={a.get('net','-')}")
    if a.get('bull'): print(f"        +bull: {a.get('bull','')[:80]}")
    if a.get('bear'): print(f"        -bear: {a.get('bear','')[:80]}")
    gn=s.get('good_news',[]); bn=s.get('bad_news',[])
    if gn: print(f"        good news: {(gn[0].get('title') or '')[:70]}")
    if bn: print(f"        bad news:  {(bn[0].get('title') or '')[:70]}")
print("\nwarnings:",[w['ticker'] for w in d.get('warnings',[])[:8]])
import urllib.request
def chk(u):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15) as x:return x.getcode()
    except Exception as e: return str(e)[:40]
print("\npage:",chk("https://justhodl.ai/hot-stocks.html?t="+str(int(time.time()))))
print("DONE 2137")
