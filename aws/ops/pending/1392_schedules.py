"""Audit EventBridge schedules for all Claude-calling Lambdas — find the
frequent ones draining cost. From AWS."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
lam=boto3.client("lambda",region_name="us-east-1",config=cfg)
CLAUDE=["justhodl-ab-test","justhodl-ai-brief","justhodl-ai-brief-router","justhodl-ai-website-synthesis","justhodl-alpha-daily-brief","justhodl-auction-crisis-ai","justhodl-auction-interpreter","justhodl-brain-sync","justhodl-catalyst-classifier","justhodl-cb-stance","justhodl-crypto-intel","justhodl-debate-engine","justhodl-devils-advocate","justhodl-digest-trends-ai","justhodl-dislocation-ai","justhodl-divergence-interpreter","justhodl-earnings-nlp","justhodl-earnings-sentiment","justhodl-equity-research","justhodl-fed-nlp","justhodl-fed-speak","justhodl-financial-secretary","justhodl-fleet-monitor","justhodl-flows-ai-analysis","justhodl-investor-agents","justhodl-ka-metrics","justhodl-khalid-metrics","justhodl-market-interpreter","justhodl-meta-improver","justhodl-morning-intelligence","justhodl-my-brief","justhodl-news-sentiment","justhodl-news-wire","justhodl-nobrainer-rationale","justhodl-page-ai-commentary","justhodl-political-ai-investigation","justhodl-premortem-engine","justhodl-prompt-iterator","justhodl-pump-earnings-nlp","justhodl-pump-radar-brief","justhodl-research-critique","justhodl-sec-filing-diff","justhodl-stock-ai-research","justhodl-ticker-deep-research","justhodl-watchlist-debate","justhodl-weekly-ai-review","justhodl-page-ai-commentary"]
# list all rules, map targets → lambda
out={"scheduled":[],"frequent":[]}
paginator=events.get_paginator("list_rules")
rules=[]
nt=None
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[])
    nt=resp.get("NextToken")
    if not nt: break
for r in rules:
    sched=r.get("ScheduleExpression")
    if not sched: continue
    try:
        tg=events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[])
        for t in tg:
            arn=t.get("Arn","")
            fn=arn.split(":function:")[-1].split(":")[0] if ":function:" in arn else ""
            if fn in CLAUDE:
                row={"lambda":fn,"rule":r["Name"],"sched":sched,"state":r.get("State")}
                out["scheduled"].append(row)
                # frequent = sub-daily (rate minutes/hours, or cron with */ or multiple hours)
                s=sched.lower()
                if ("minute" in s) or ("rate(1 hour" in s) or ("rate(2 hour" in s) or ("rate(3 hour" in s) or ("rate(4 hour" in s) or ("rate(6 hour" in s) or ("/" in s):
                    out["frequent"].append(row)
    except Exception as e: pass
open("aws/ops/reports/1392_sched.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
