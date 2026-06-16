import boto3, datetime, json, urllib.request
events=boto3.client("events",region_name="us-east-1")
ct=boto3.client("cloudtrail",region_name="us-east-1")
end=datetime.datetime.now(datetime.timezone.utc); start=end-datetime.timedelta(days=21)

print("====== #2: CURRENT DISABLED rules (the cost-pause inventory) ======")
dis=[]
for pg in events.get_paginator("list_rules").paginate():
    for r in pg["Rules"]:
        if r.get("State")=="DISABLED":
            tgts=[]
            try:
                for t in events.list_targets_by_rule(Rule=r["Name"])["Targets"]:
                    if ":function:" in t.get("Arn",""): tgts.append(t["Arn"].split(":function:")[1].split(":")[0])
            except: pass
            dis.append((r["Name"], r.get("ScheduleExpression",""), ",".join(tgts) or "?"))
print(f"  {len(dis)} disabled rules:")
for n,s,t in sorted(dis): print(f"   • {n:42} {s:24} -> {t}")

print("\n====== #2: CloudTrail — who paused what, last 21d ======")
for ev in ("DisableRule","RemoveTargets","DeleteRule"):
    try:
        r=ct.lookup_events(LookupAttributes=[{"AttributeKey":"EventName","AttributeValue":ev}],StartTime=start,EndTime=end,MaxResults=25)
        rows=r.get("Events",[])
        print(f"  {ev}: {len(rows)} events")
        for e in rows[:12]:
            try:
                d=json.loads(e["CloudTrailEvent"]); rn=(d.get("requestParameters") or {}).get("name") or (d.get("requestParameters") or {}).get("rule") or "?"
            except: rn="?"
            print(f"     {e['EventTime'].strftime('%m-%d %H:%M')} {e.get('Username','?'):16} {rn}")
    except Exception as ex: print(f"  {ev}: err {str(ex)[:50]}")

print("\n====== #3: ECB host/series probe (data-api vs dead sdw-wsrest) ======")
def probe(host, series):
    url=f"https://{host}/service/data/{series}?format=csvdata&lastNObservations=1"
    try:
        req=urllib.request.Request(url, headers={"User-Agent":"JustHodl/1.0"})
        with urllib.request.urlopen(req,timeout=20) as r: return r.status, len(r.read())
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:40]
for series in ["CISS/M.U2.Z0Z.4F.EC.SOVCISS_CI.IDX","CISS/D.U2.Z0Z.4F.EC.SS_BM.CON","CISS/D.U2.Z0Z.4F.EC.SS_EM.CON"]:
    s_new=probe("data-api.ecb.europa.eu",series)
    print(f"   {series:38} data-api -> {s_new}")
print("   (dead host sdw-wsrest.ecb.europa.eu expected to DNS-fail — confirming swap target works)")
