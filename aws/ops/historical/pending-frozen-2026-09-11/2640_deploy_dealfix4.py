import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-deal-scanner"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}"); time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
s=j.get("summary",{})
print("summary:",{k:s.get(k) for k in ['n_prs_scanned','n_deals','n_green','n_yellow','n_ai','n_ai_mega','n_small_cap']})
print("\nDEALS (profile announcer-match):")
for d in (j.get("deals") or []):
    print(f"  {d['symbol']:6s} hl={str(d.get('highlight')):6s} ai={int(bool(d.get('ai_relevant')))} sz={d.get('deal_value_str') or '—':>10s} vsMC={d.get('vs_market_cap_pct')}% mat={d.get('materiality_pct')} | {d['title'][:56]}")
print("DONE 2640")
