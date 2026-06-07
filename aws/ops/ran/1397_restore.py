import json, boto3
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
events=boto3.client("events",region_name="us-east-1",config=cfg)
out={}
# Re-enable the legitimate DAILY routed tasks (these are real features, not dupes).
# Keep ONLY the hourly uptime-monitor disabled (24x/day Claude on an uptime check = waste;
# but if it doesn't call Claude it's cheap — still, hourly→daily is the cost win).
RE_ENABLE=["justhodl-alerts-digest-close-daily","justhodl-alerts-digest-daily","justhodl-frontrun-skill-daily","justhodl-opportunity-ranker-4h"]
for rule in RE_ENABLE:
    try: events.enable_rule(Name=rule); out[rule]="RE-ENABLED (daily — legit feature)"
    except Exception as e: out[rule]="err "+str(e)[:50]
# uptime-monitor: keep it but make it DAILY instead of hourly (was the real waste)
try:
    r=events.describe_rule(Name="justhodl-uptime-monitor-hourly")
    events.put_rule(Name="justhodl-uptime-monitor-hourly",ScheduleExpression="cron(0 12 * * ? *)",State="ENABLED")
    out["justhodl-uptime-monitor-hourly"]="hourly→daily 12:00, re-enabled"
except Exception as e: out["uptime"]="err "+str(e)[:50]
open("aws/ops/reports/1397_r.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
