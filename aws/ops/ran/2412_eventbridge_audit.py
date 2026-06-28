import boto3
ev=boto3.client("events","us-east-1")
rules=[]; tok=None
while True:
    kw={"Limit":100}
    if tok: kw["NextToken"]=tok
    r=ev.list_rules(**kw)
    rules.extend(r.get("Rules",[]))
    tok=r.get("NextToken")
    if not tok: break
enabled=[r for r in rules if r.get("State")=="ENABLED"]
disabled=[r for r in rules if r.get("State")!="ENABLED"]
print("total rules:",len(rules),"| ENABLED:",len(enabled),"| DISABLED:",len(disabled))
print("\n=== DISABLED rules (%d) ==="%len(disabled))
for r in disabled[:80]:
    print("  ",r.get("Name"),"|",r.get("ScheduleExpression") or r.get("EventPattern","")[:30],"|",r.get("State"))
# map finviz + a few stale engines to their rule state
print("\n=== schedule state for sampled stale engines ===")
names=[r.get("Name") for r in rules]
for eng in ["finviz","edge-discovery","meta-improver","causality","bagger","decisive","secretary","digest-trends","capital-inflows","dark-pool","earnings-quality"]:
    matches=[(r.get("Name"),r.get("State"),r.get("ScheduleExpression")) for r in rules if eng in r.get("Name","").lower()]
    if matches:
        for n,st,sc in matches[:3]: print("  %s -> %s %s"%(n,st,sc))
    else:
        print("  %s -> NO RULE FOUND (schedule deleted?)"%eng)
print("DONE 2412")
