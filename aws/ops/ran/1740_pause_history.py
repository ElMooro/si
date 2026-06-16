import boto3, datetime, json
ct=boto3.client("cloudtrail",region_name="us-east-1")
events=boto3.client("events",region_name="us-east-1")
end=datetime.datetime.now(datetime.timezone.utc); start=end-datetime.timedelta(days=21)

print("=== (A) EventBridge pause/delete actions in last 21 days (CloudTrail) ===")
seen=[]
for ename in ["DisableRule","DeleteRule","RemoveTargets"]:
    try:
        tok=None; cnt=0
        while True:
            kw=dict(LookupAttributes=[{"AttributeKey":"EventName","AttributeValue":ename}],StartTime=start,EndTime=end,MaxResults=50)
            if tok: kw["NextToken"]=tok
            r=ct.lookup_events(**kw)
            for e in r.get("Events",[]):
                cnt+=1
                try: det=json.loads(e["CloudTrailEvent"])
                except: det={}
                rp=det.get("requestParameters",{}) or {}
                rule=rp.get("name") or rp.get("rule") or "?"
                when=e["EventTime"].astimezone(datetime.timezone.utc)
                days=round((end-when).total_seconds()/86400,1)
                who=(det.get("userIdentity",{}) or {}).get("arn","").split("/")[-1] or "?"
                seen.append((days,ename,rule,when.strftime("%Y-%m-%d %H:%M"),who))
            tok=r.get("NextToken")
            if not tok or cnt>200: break
    except Exception as ex: print(f"  {ename}: lookup err {str(ex)[:60]}")
seen.sort()
if not seen: print("  (no DisableRule/DeleteRule/RemoveTargets events found in window)")
for days, en,rule,when,who in seen:
    print(f"  {days:5}d ago  {en:14} {rule:42} {when}  by {who}")

print("\n=== (B) ALL currently DISABLED schedule rules + their lambda targets (the pause inventory) ===")
dis=[]
for pg in events.get_paginator("list_rules").paginate():
    for r in pg["Rules"]:
        if r.get("State")=="DISABLED" and r.get("ScheduleExpression"):
            tg=[]
            try:
                for t in events.list_targets_by_rule(Rule=r["Name"])["Targets"]:
                    if ":function:" in t.get("Arn",""): tg.append(t["Arn"].split(":function:")[1].split(":")[0])
            except: pass
            dis.append((r["Name"],r.get("ScheduleExpression"),tg))
print(f"  {len(dis)} disabled scheduled rules:")
for n,se,tg in sorted(dis):
    print(f"   {n:46} {se:24} -> {','.join(tg) or '(no lambda target)'}")
