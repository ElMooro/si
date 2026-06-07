"""Cost cut: dial frequent Claude-calling Lambdas down to once/day. From AWS."""
import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
out={}
# rule → new daily schedule (staggered hours UTC to avoid pile-up)
CHANGES={
  "justhodl-brain-sync-15min": "cron(0 13 * * ? *)",        # 15min → daily 13:00
  "fed-nlp-6h": "cron(30 13 * * ? *)",                       # 6h → daily 13:30
  "justhodl-devils-advocate-6h": "cron(0 14 * * ? *)",       # 6h → daily 14:00
  "justhodl-opportunity-ranker-4h": "cron(30 14 * * ? *)",   # 4h → daily 14:30
}
for rule,new in CHANGES.items():
    try:
        # preserve the rule's targets; just change schedule
        r=events.describe_rule(Name=rule)
        events.put_rule(Name=rule, ScheduleExpression=new, State=r.get("State","ENABLED"))
        out[rule]={"old":r.get("ScheduleExpression"),"new":new,"ok":True}
    except Exception as e: out[rule]={"err":str(e)[:100]}
open("aws/ops/reports/1393_t.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
