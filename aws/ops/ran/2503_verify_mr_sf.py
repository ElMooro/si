import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-master-ranker",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/master-ranker.json")["Body"].read())
ranks=d.get("ranks") or d.get("ticker_ranks") or d.get("rankings") or []
print("n_ranks:",len(ranks))
hits=[r for r in ranks if "sector " in (r.get("rationale") or "") and ("Leading/OW" in r.get("rationale","") or "rotating in" in r.get("rationale","") or "headwind" in r.get("rationale","") or "momentum fading" in r.get("rationale",""))]
print("n_with_sectorflow_overlay:",len(hits))
for r in hits[:6]:
    rat=r.get("rationale","")
    seg=[s for s in rat.split(" · ") if "sector " in s and ("Leading" in s or "rotating" in s or "headwind" in s or "fading" in s)]
    print("  %-6s score=%-6s %s"%(r.get("ticker"),r.get("score"),seg[0] if seg else ""))
print("DONE 2503")
