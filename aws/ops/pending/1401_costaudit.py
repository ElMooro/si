"""Cost audit: list ALL enabled EventBridge schedules + their frequency, so we
can see total daily invocation load and disable non-essentials. From AWS."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
out={"enabled":[],"by_freq":{}}
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
def daily_count(s):
    s=s.lower()
    if "minute" in s:
        import re; m=re.search(r"rate\((\d+) minute",s); n=int(m.group(1)) if m else 5; return round(1440/n)
    if "rate(1 hour" in s: return 24
    if "rate(" in s and "hour" in s:
        import re; m=re.search(r"rate\((\d+) hour",s); n=int(m.group(1)) if m else 1; return round(24/n)
    if "cron(0 * " in s or "cron(* " in s: return 24
    return 1  # daily-ish cron
total=0
for r in rules:
    s=r.get("ScheduleExpression")
    if not s or r.get("State")!="ENABLED": continue
    dc=daily_count(s); total+=dc
    out["enabled"].append({"rule":r["Name"],"sched":s,"per_day":dc})
out["enabled"].sort(key=lambda x:-x["per_day"])
out["total_enabled_rules"]=len(out["enabled"])
out["est_total_invocations_per_day"]=total
out["top_burners"]=out["enabled"][:25]
open("aws/ops/reports/1401_ca.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
