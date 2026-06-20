"""ops 2008: redeploy squeeze-fuel w/ liquidity gate (retry on deploy race), verify clean board."""
import boto3, json, time, io, os, zipfile
REGION="us-east-1"; FN="justhodl-squeeze-fuel"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",REGION); s3=boto3.client("s3",REGION)
SRC=f"aws/lambdas/{FN}/source"; buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for r,_,fs in os.walk(SRC):
        for f in fs:
            if f.endswith(".py"): p=os.path.join(r,f); z.write(p,os.path.relpath(p,SRC))
zb=buf.getvalue()
for attempt in range(8):
    try:
        lam.update_function_code(FunctionName=FN,ZipFile=zb); break
    except lam.exceptions.ResourceConflictException:
        print(f"  deploy race, retry {attempt}"); time.sleep(8)
for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
print("redeployed:",c["LastModified"])
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r["StatusCode"], r["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/squeeze-fuel.json")["Body"].read())
print("\nsettlement:",d.get("si_settlement_date"),"| liquid universe:",d.get("n_finra_universe"),"| scored:",d.get("n_scored"),"| dist:",d.get("distribution"),"| picks:",len(d.get("top_picks") or []))
print("\nTOP BOARD (liquid only):")
for r in (d.get("board") or [])[:15]:
    print(f"  {r['ticker']:<6} fuel={r['score']:<5} {r['state']:<9} %flt={r.get('pct_of_float')} dtc={r.get('days_to_cover')} siΔ%={r.get('si_change_pct')} | {' · '.join((r.get('reasons') or [])[:3])}")
print("\nPICKS:", [(p['ticker'],p['score'],p['state'],p.get('pct_of_float')) for p in (d.get('top_picks') or [])])
# sanity: any remaining junk (None float AND dtc>40)?
junk=[r['ticker'] for r in (d.get('board') or []) if (r.get('days_to_cover') or 0)>40]
print("\nremaining dtc>40 (should be none):",junk[:10])
print("DONE 2008")
