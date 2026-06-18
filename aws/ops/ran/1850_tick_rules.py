import boto3, json
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
rules=[]
for pg in ev.get_paginator("list_rules").paginate():
    rules.extend(pg["Rules"])
sched=[r for r in rules if r.get("ScheduleExpression")]
print("total EB rules=%d  scheduled=%d"%(len(rules),len(sched)))
print("\n=== ALL SCHEDULED RULES (state / schedule / name) ===")
for r in sorted(sched, key=lambda x:(x.get("State",""),x["Name"])):
    print("  [%-8s] %-26s %s"%(r.get("State",""), r["ScheduleExpression"], r["Name"]))
# tick rules specifically -> their targets/input
print("\n=== rules whose target is justhodl-scheduler ===")
for r in rules:
    try: tg=ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[])
    except Exception: tg=[]
    if any("justhodl-scheduler" in t.get("Arn","") for t in tg):
        inp=""
        for t in tg:
            if t.get("Input"): inp=t["Input"].replace("\n","")[:30]
        print("  [%-8s] %-22s %-26s tick=%s"%(r.get("State",""), r.get("ScheduleExpression","(none)"), r["Name"], inp))
# manifest blast radius
try:
    m=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="config/schedule-manifest.json")["Body"].read())
    print("\n=== manifest engine counts per tick ===")
    tot=0
    for t,lst in (m.get("ticks") or {}).items():
        tot+=len(lst); print("  %-12s %d"%(t,len(lst)))
    print("  TOTAL registered=%d  disabled-list=%d"%(tot,len(m.get("disabled") or [])))
except Exception as e:
    print("manifest err:",e)
