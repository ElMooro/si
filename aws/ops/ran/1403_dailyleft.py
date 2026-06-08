import json, boto3, re
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
still=[]
for r in rules:
    s=r.get("ScheduleExpression")
    if s and r.get("State")=="ENABLED": still.append({"rule":r["Name"],"sched":s})
still.sort(key=lambda x:x["rule"])
open("aws/ops/reports/1403_d.json","w").write(json.dumps({"enabled_remaining":still,"count":len(still)},indent=2,default=str)); print("done")
