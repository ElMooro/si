import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
CLAUDE=set(["justhodl-brain-sync","justhodl-fed-nlp","justhodl-devils-advocate","justhodl-ai-brief-router","justhodl-morning-intelligence","justhodl-my-brief","justhodl-crypto-intel","justhodl-news-sentiment","justhodl-news-wire","justhodl-earnings-sentiment","justhodl-earnings-nlp","justhodl-page-ai-commentary","justhodl-dislocation-ai","justhodl-financial-secretary","justhodl-fleet-monitor","justhodl-weekly-ai-review","justhodl-fed-speak","justhodl-cb-stance","justhodl-sec-filing-diff","justhodl-meta-improver","justhodl-prompt-iterator","justhodl-ab-test"])
out=[]
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
            if fn in CLAUDE: out.append({"lambda":fn,"sched":s,"state":r.get("State")})
    except Exception: pass
out.sort(key=lambda x:x["lambda"])
open("aws/ops/reports/1395_c.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
