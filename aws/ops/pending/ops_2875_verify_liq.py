import os, json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
R={"ops":2875,"ts":datetime.now(timezone.utc).isoformat()}
try: lam.invoke(FunctionName="justhodl-liquidity-inflection",InvocationType="Event")  # async, heavy engine
except Exception as e: R["inv_err"]=str(e)[:60]
bp=None
for _ in range(9):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
        gen=d.get("generated_at","")
        if "brain_predictors" in d and gen>R["ts"][:10]:  # updated today with new key
            bp=d.get("brain_predictors"); R["gen"]=gen; R["usd_state"]=(d.get("usd") or {}).get("state"); break
    except Exception as e: R["read_err"]=str(e)[:60]
R["brain_predictors"]=bp if bp is not None else "not-yet-updated (scheduled run will populate)"
R["status"]="LIVE" if bp else "PENDING_SCHEDULE"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2000])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2875_verify_liq.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2875 COMPLETE")
