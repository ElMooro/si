import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
def rd(k): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
print("=== CYCLE-CLOCK ===")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="RequestResponse",Payload=b"{}")
cc=rd("data/cycle-clock.json"); capc=cc.get("capital_cycle") or {}
print("capital_cycle block:",json.dumps(capc))
syn=cc.get("synthesis") or {}
print("n_risk_on:",syn.get("n_risk_on"),"n_risk_off:",syn.get("n_risk_off"))
print("=== BEST-SETUPS ===")
lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}")
bs=rd("data/best-setups.json"); book=bs.get("top_setups") or []
hits=[s for s in book if any(g.get("key") in ("BOTTLENECK_BOOM","CAPITAL_CYCLE_EARLY") for g in (s.get("signals") or []))]
print("top_setups:",len(book),"| carrying bottleneck/capital-cycle:",len(hits))
for s in hits[:10]:
    print("  ",s.get("ticker"),s.get("verdict"),"conv",s.get("conviction"),[g.get("key") for g in (s.get("signals") or [])][:7])
print("DONE 2456")
