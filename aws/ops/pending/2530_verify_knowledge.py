import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=120,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
for _ in range(20):
    if lam.get_function_configuration(FunctionName="justhodl-brain-sync").get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
# plain invoke (no force) -> rewrites brain.json sans mentioned_tickers, reuses cached directive (no LLM)
r=lam.invoke(FunctionName="justhodl-brain-sync",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(3)
br=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
print("=== brain output now ===")
print("has 'mentioned_tickers' field:", "mentioned_tickers" in br, " (should be False)")
d=br.get("directive") or {}
print("directive (KNOWLEDGE) intact:", bool(d))
print("  hard_rules:", len(d.get("hard_rules") or []), "rules")
print("  themes:", len(d.get("themes") or []), "themes")
print("  sector_tilts:", list((d.get("sector_tilts") or {}).keys())[:6])
print("  risk_posture:", str(d.get("risk_posture"))[:80])
print("  signal_emphasis:", len(d.get("signal_emphasis") or []), "emphases")
print("regime_read:", (br.get("regime_read") or {}).get("regime"))
print("distill_cadence:", br.get("distill_cadence"))
print("DONE 2530")
