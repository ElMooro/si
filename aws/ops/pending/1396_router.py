import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
# find all rules targeting ai-brief-router
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
router_rules=[]
for r in rules:
    if not r.get("ScheduleExpression"): continue
    try:
        for t in events.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]):
            if "justhodl-ai-brief-router" in t.get("Arn",""):
                router_rules.append({"name":r["Name"],"sched":r["ScheduleExpression"],"state":r.get("State")})
    except Exception: pass
out={"found":router_rules,"actions":[]}
# Keep ONE (the 11:00 daily), disable the hourly + the rest to cut cost.
# Disable everything except one daily rule.
keep=None
for rr in router_rules:
    if rr["sched"]=="cron(0 11 * * ? *)" and keep is None: keep=rr["name"]; continue
for rr in router_rules:
    if rr["name"]==keep: out["actions"].append({rr["name"]:"KEPT daily 11:00"}); continue
    try:
        events.disable_rule(Name=rr["name"]); out["actions"].append({rr["name"]:"DISABLED ("+rr["sched"]+")"})
    except Exception as e: out["actions"].append({rr["name"]:"err "+str(e)[:50]})
if keep is None and router_rules:
    # if no 11:00 rule, keep the first and disable rest
    keep=router_rules[0]["name"]
    out["actions"].append({keep:"KEPT (first)"})
open("aws/ops/reports/1396_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
