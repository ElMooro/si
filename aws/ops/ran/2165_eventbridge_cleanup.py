import boto3, json, time
ev=boto3.client("events","us-east-1"); lam=boto3.client("lambda","us-east-1")
# enumerate all rules on default bus
rules=[]; tok=None
while True:
    kw={"NextToken":tok} if tok else {}
    r=ev.list_rules(Limit=100,**kw); rules+=r.get("Rules",[])
    tok=r.get("NextToken")
    if not tok: break
total=len(rules)
print(f"TOTAL RULES (default bus): {total}")

def lambda_exists(arn):
    if ":function:" not in arn: return True  # non-lambda target — treat as live
    fn=arn.split(":function:")[1].split(":")[0]
    try: lam.get_function(FunctionName=fn); return True
    except lam.exceptions.ResourceNotFoundException: return False
    except Exception: return True

empty=[]; dangling=[]; healthy=0; managed=0; eventpattern_empty=[]
for ru in rules:
    name=ru["Name"]
    if ru.get("ManagedBy"): managed+=1; continue
    tg=ev.list_targets_by_rule(Rule=name).get("Targets",[])
    scheduled=bool(ru.get("ScheduleExpression"))
    if not tg:
        (empty if scheduled else eventpattern_empty).append(name)
    else:
        if scheduled and all(not lambda_exists(t["Arn"]) for t in tg):
            dangling.append((name,[t["Id"] for t in tg]))
        else:
            healthy+=1
print(f"  managed(AWS): {managed} | healthy: {healthy}")
print(f"  EMPTY scheduled rules (0 targets): {len(empty)}")
print(f"  DANGLING scheduled rules (all targets→deleted lambda): {len(dangling)}")
print(f"  empty event-pattern rules (left alone): {len(eventpattern_empty)}")

# protect the orphan rule ma200 currently rides on (we'll re-home it next)
PROTECT={"aiapi-hourly-collection"}
deleted=0
for name in empty:
    if name in PROTECT: continue
    try: ev.delete_rule(Name=name,Force=True); deleted+=1; print(f"   deleted EMPTY: {name}")
    except Exception as e: print(f"   skip {name}: {str(e)[:40]}")
for name,ids in dangling:
    if name in PROTECT: continue
    try:
        ev.remove_targets(Rule=name,Ids=ids,Force=True)
        ev.delete_rule(Name=name,Force=True); deleted+=1; print(f"   deleted DANGLING: {name}")
    except Exception as e: print(f"   skip {name}: {str(e)[:40]}")
print(f"\nFREED {deleted} rule slots. New total ~{total-deleted}/300")
print("DONE 2165")
