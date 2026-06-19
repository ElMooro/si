import boto3, hashlib, time
ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1")
def is_intraday(expr):
    e=expr.strip()
    if e.startswith("rate("):
        try: n,unit=e[5:-1].strip().split()
        except: return False
        n=int(n); unit=unit.lower()
        if unit.startswith("minute"): return True
        if unit.startswith("hour"): return n<24
        return False
    if e.startswith("cron("):
        f=e[5:-1].split()
        if len(f)!=6: return False
        mn,hr=f[0],f[1]
        if hr=="*" or "/" in hr or "," in hr: return True
        if mn=="*" or "/" in mn or "," in mn: return True
        return False
    return False
def daily_cron(name):
    h=int(hashlib.md5(name.encode()).hexdigest(),16)
    return "cron(%d %d * * ? *)"%(h%60, 11+(h%11))   # minute 0-59, hour 11-21 UTC
allrules=[]
pag=ev.get_paginator("list_rules")
for pg in pag.paginate(): allrules+=pg["Rules"]
sched=[r for r in allrules if r.get("ScheduleExpression") and not r.get("ManagedBy")]
converted=[]; enabled=[]; left=0; errors=[]
for r in sched:
    name=r["Name"]; expr=r["ScheduleExpression"]; state=r.get("State")
    new=expr; chg=is_intraday(expr)
    if chg: new=daily_cron(name)
    if chg or state!="ENABLED":
        try:
            ev.put_rule(Name=name,ScheduleExpression=new,State="ENABLED",Description=(r.get("Description") or "daily")[:500])
            if chg: converted.append((name,expr,new))
            if state!="ENABLED": enabled.append((name,state))
        except Exception as e: errors.append((name,str(e)[:90]))
    else: left+=1
print("=== SCHEDULE NORMALIZATION (everything -> daily, all enabled) ===")
print("total scheduled rules:",len(sched))
print("converted intraday -> daily:",len(converted))
print("re-enabled (were disabled):",len(enabled))
print("left as-is (already daily/weekly + enabled):",left)
print("errors:",len(errors))
print("\n-- intraday -> daily (all) --")
for n,o,nw in sorted(converted): print("  %-46s %-22s -> %s"%(n,o,nw))
print("\n-- re-enabled (all) --")
for n,st in sorted(enabled): print("  %-46s (was %s)"%(n,st))
if errors:
    print("\n-- ERRORS --")
    for n,e in errors: print("  %s: %s"%(n,e))
# refresh stale-output engines now (async, no timeout) so we can verify health next
refresh=["justhodl-dealer-gex","justhodl-options-gamma","justhodl-etf-flows","justhodl-polygon-options-flow",
 "justhodl-vol-regime","justhodl-rotation-chain","justhodl-exchange-flows","justhodl-event-flow-monitor",
 "justhodl-bond-vol","justhodl-vol-target-unwind","justhodl-etf-fund-flows"]
print("\n=== REFRESH STALE ENGINES NOW (async) ===")
ok=0
for fn in refresh:
    try: lam.invoke(FunctionName=fn,InvocationType="Event"); ok+=1; print("  invoked",fn)
    except Exception as e: print("  SKIP %s: %s"%(fn,str(e)[:60]))
print("async-invoked %d/%d"%(ok,len(refresh)))
