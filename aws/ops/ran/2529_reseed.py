import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(20):
    st=lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(5)
print("LastUpdateStatus:",st)
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=json.dumps({"force":True}).encode())
print("invoke err:",r.get("FunctionError")); time.sleep(3)
br=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
d=br.get("directive") or {}; rr=br.get("regime_read") or {}
print("\n=== DIRECTIVE (now feeds 30+ engines, monthly) ===")
print("directive populated:",bool(d))
if d:
    print("  investor_profile:",str(d.get("investor_profile"))[:160])
    print("  risk_posture:",d.get("risk_posture"))
    print("  sector_tilts:",json.dumps(d.get("sector_tilts") or {})[:300])
    print("  themes:",(d.get("themes") or [])[:6])
    print("  hard_rules:",(d.get("hard_rules") or [])[:4])
    print("  watched_tickers:",(d.get("watched_tickers") or [])[:12])
    print("  signal_emphasis:",d.get("signal_emphasis"))
print("regime_read:",rr.get("regime") if isinstance(rr,dict) else None)
print("mentioned_tickers (engine-facing):",(br.get("mentioned_tickers") or [])[:15])
print("last_distill_at:",br.get("last_distill_at"),"| cadence:",br.get("distill_cadence"))
print("DONE 2529")
