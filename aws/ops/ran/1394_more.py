import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
# Check schedules for the other heavy Claude lambdas not caught before
TARGETS=["justhodl-crypto-intel","justhodl-news-sentiment","justhodl-news-wire","justhodl-earnings-sentiment","justhodl-earnings-nlp","justhodl-page-ai-commentary","justhodl-dislocation-ai","justhodl-financial-secretary","justhodl-fleet-monitor","justhodl-catalyst-classifier","justhodl-cb-stance","justhodl-fed-speak","justhodl-market-interpreter","justhodl-divergence-interpreter","justhodl-flows-ai-analysis","justhodl-my-brief","justhodl-pump-radar-brief","justhodl-pump-earnings-nlp","justhodl-sec-filing-diff"]
out={"sub_daily":[]}
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
for r in rules:
    s=r.get("ScheduleExpression")
    if not s: continue
    try:
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
            fn=t.get("Arn","").split(":function:")[-1].split(":")[0]
            if fn in TARGETS:
                sl=s.lower()
                freq = ("minute" in sl) or ("rate(1 hour" in sl) or ("rate(2 hour" in sl) or ("rate(3 hour" in sl) or ("rate(4 hour" in sl) or ("rate(6 hour" in sl) or ("rate(8 hour" in sl) or ("rate(12 hour" in sl)
                if freq: out["sub_daily"].append({"lambda":fn,"rule":r["Name"],"sched":s,"state":r.get("State")})
    except Exception: pass
open("aws/ops/reports/1394_m.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
