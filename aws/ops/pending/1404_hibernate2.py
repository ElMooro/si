"""HIBERNATE pass 2: catch cron step-syntax (*/5, 0/4, multi-hour lists, hourly
crons) that pass-1 missed. Disable anything firing >1x/day. Append to the saved
reversal list. Keep a minimal daily heartbeat. From AWS."""
import json, boto3, re
from botocore.config import Config
cfg=Config(read_timeout=180,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
# essential daily keepers (one run/day each — cheap, keeps core data warm)
KEEP=set([
  "justhodl-morning-intelligence-daily","morning-intelligence-daily","justhodl-daily-report-v3",
  "justhodl-brain-sync-15min","justhodl-funding-plumbing-daily","justhodl-best-setups-hourly",
])
def fires_intraday(s):
    s=s.lower()
    # cron(min hour ...). Intraday if minute has / or comma-list, OR hour field has /, comma-list, range, or *
    m=re.match(r"cron\(([^ ]+) ([^ ]+) ",s)
    if not m: 
        if "rate(" in s and ("minute" in s or "hour" in s): return True
        return False
    minute,hour=m.group(1),m.group(2)
    if "/" in minute or "," in minute or minute=="*": return True
    if "/" in hour or "," in hour or "-" in hour or hour=="*": return True
    return False
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
disabled=[]
for r in rules:
    s=r.get("ScheduleExpression")
    if not s or r.get("State")!="ENABLED": continue
    if r["Name"] in KEEP: continue
    if fires_intraday(s):
        try: events.disable_rule(Name=r["Name"]); disabled.append({"rule":r["Name"],"was":s})
        except Exception: pass
# merge with existing reversal list
try: prev=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="ops/hibernate-disabled-rules.json")["Body"].read())
except Exception: prev=[]
alld=prev+disabled
s3.put_object(Bucket="justhodl-dashboard-live",Key="ops/hibernate-disabled-rules.json",Body=json.dumps(alld).encode(),ContentType="application/json")
out={"disabled_this_pass":len(disabled),"total_disabled":len(alld),"sample":[x["rule"] for x in disabled[:15]]}
open("aws/ops/reports/1404_h2.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
