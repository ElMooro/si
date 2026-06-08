"""HIBERNATE mode: disable all sub-daily schedulers to slash cost while the
system isn't in use. Fully reversible (we record every rule we disable so we can
re-enable later). Keeps a tiny set of essential daily rules running. From AWS."""
import json, boto3, re
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
# KEEP these enabled (essential daily heartbeat — cheap, keeps data fresh-ish)
KEEP=set([
  "justhodl-morning-intelligence","morning-intelligence-daily","justhodl-daily-report-v3",
  "justhodl-brain-sync-15min",      # already daily now
])
def per_day(s):
    s=s.lower()
    if "minute" in s: return 99
    if re.search(r"rate\(\d+ hour",s): 
        n=int(re.search(r"rate\((\d+) hour",s).group(1)); return round(24/n)
    if "cron(0 * " in s or "cron(* " in s: return 24
    return 1
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
disabled=[]; kept=[]
for r in rules:
    s=r.get("ScheduleExpression")
    if not s or r.get("State")!="ENABLED": continue
    if r["Name"] in KEEP: kept.append(r["Name"]); continue
    # disable anything that fires more than once/day (the cost burners)
    if per_day(s) > 1:
        try: events.disable_rule(Name=r["Name"]); disabled.append({"rule":r["Name"],"was":s})
        except Exception as e: pass
out={"disabled_count":len(disabled),"disabled":disabled,"kept_essential":kept}
# save the disable list to S3 so we can reverse it later
boto3.client("s3",region_name="us-east-1").put_object(Bucket="justhodl-dashboard-live",
    Key="ops/hibernate-disabled-rules.json",Body=json.dumps(disabled).encode(),ContentType="application/json")
open("aws/ops/reports/1402_hib.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
