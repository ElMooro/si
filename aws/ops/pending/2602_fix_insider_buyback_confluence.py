"""ops 2602 — deploy insider-buyback-confluence (buyback-engine powered), probe feeds, invoke, schedule."""
import boto3, io, zipfile, json, time, urllib.request
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-insider-buyback-confluence"
SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
def get(p):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(f"https://justhodl-data-proxy.raafouis.workers.dev/{p}?t={int(time.time())}",headers={"User-Agent":"M"}),timeout=20).read())
    except Exception as e: return {"_e":str(e)[:40]}
# probe insider feed schema
ins=get("data/insider-buys-enriched.json")
print("insider-buys-enriched top keys:", list(ins.keys())[:12] if isinstance(ins,dict) else type(ins))
for k in ("clusters","picks","hits","buys","results","by_ticker","enriched","top","data"):
    v=ins.get(k) if isinstance(ins,dict) else None
    if isinstance(v,list) and v: print(f"  list '{k}': {len(v)} rows; sample keys: {list(v[0].keys())[:8] if isinstance(v[0],dict) else v[0]}")
    elif isinstance(v,dict) and v: print(f"  dict '{k}': {len(v)} keys; sample: {list(v.keys())[:5]}")
# deploy
def wait():
    for _ in range(25):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:160])
time.sleep(2)
j=get("data/insider-buyback-confluence.json")
print("STATE:", j.get("state"), "| n_confluences:", j.get("n_confluences"), "| n_high:", j.get("n_high_conviction"))
print("feeders:", j.get("feeders"))
for c in (j.get("top_confluences") or [])[:8]:
    print(f"  {c['ticker']}: composite={c['composite_score']} ins={c['insider_score']} byb={c['buyback_score']} | {(c.get('buyback_stats') or {}).get('class')}")
# schedule
RULE=f"{FN}-daily"
try:
    ev.describe_rule(Name=RULE); print(f"  schedule {RULE} exists")
except ev.exceptions.ResourceNotFoundException:
    ev.put_rule(Name=RULE, ScheduleExpression="cron(0 14 * * ? *)", State="ENABLED", Description="Daily insider×buyback confluence")
    try: lam.add_permission(FunctionName=FN, StatementId=f"{RULE}-invoke", Action="lambda:InvokeFunction", Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
    except Exception: pass
    ev.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":f"arn:aws:lambda:{REGION}:{ACCT}:function:{FN}"}])
    print(f"  ＋ CREATted schedule {RULE}: cron(0 14 * * ? *)")
print("DONE 2602")
