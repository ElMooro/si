import json, boto3, re
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
nt=None; rules=[]
while True:
    resp=events.list_rules(**({"NextToken":nt} if nt else {}))
    rules+=resp.get("Rules",[]); nt=resp.get("NextToken")
    if not nt: break
def per_day(s):
    s=s.lower()
    if "minute" in s:
        m=re.search(r"rate\((\d+) minute",s); return round(1440/(int(m.group(1)) if m else 5))
    m=re.match(r"cron\(([^ ]+) ([^ ]+) ",s)
    if m:
        mn,hr=m.group(1),m.group(2)
        if "/" in mn: 
            st=int(re.search(r"/(\d+)",mn).group(1)); 
            hrs=24 if (hr=="*" or "/" in hr) else len(hr.split(","))
            return round(60/st)*hrs
        if hr=="*": return 24
        if "/" in hr: return round(24/int(re.search(r"/(\d+)",hr).group(1)))
        return len(hr.split(","))
    if re.search(r"rate\((\d+) hour",s): return round(24/int(re.search(r"rate\((\d+) hour",s).group(1)))
    return 1
total=0; enabled=0
for r in rules:
    s=r.get("ScheduleExpression")
    if s and r.get("State")=="ENABLED": enabled+=1; total+=per_day(s)
out={"enabled_rules":enabled,"est_invocations_per_day":total}
open("aws/ops/reports/1405_fl.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
