import boto3, json, time
lam=boto3.client("lambda","us-east-1"); ev=boto3.client("events","us-east-1")
s3=boto3.client("s3","us-east-1")
FN="justhodl-supply-inflection-scanner"
# does the function exist? when last modified? is there a rule?
try:
    c=lam.get_function_configuration(FunctionName=FN)
    print("function exists:", c["FunctionName"], "| last mod:", c["LastModified"][:10], "| state:", c.get("State"))
except Exception as e: print("function ERR:", str(e)[:60])
rules=ev.list_rule_names_by_target(TargetArn=f"arn:aws:lambda:us-east-1:857687956942:function:{FN}").get("RuleNames",[]) if True else []
try:
    rules=[]
    for r in ev.list_rules(NamePrefix="justhodl-supply-inflection").get("Rules",[]): rules.append((r["Name"],r.get("ScheduleExpression"),r.get("State")))
    print("schedule rules:", rules or "NONE")
except Exception as e: print("rules ERR:", str(e)[:50])
# invoke it and see what it returns + where it writes
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    payload=r["Payload"].read().decode()[:300]
    print("invoke result:", payload)
except Exception as e: print("invoke ERR:", str(e)[:120])
# check candidate output keys for the live engines we'll fuse
for f,lists in [("bottleneck-boom",["candidates","top_candidates","board","scored","top_picks"]),
                ("chokepoint",["confirmed_chokepoint_book","highest_conviction_book","structural_names"]),
                ("narrative-vs-tape",["quiet_accumulation"]),
                ("revenue-acceleration",["all_qualifying"])]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        for L in lists:
            arr=d.get(L)
            if isinstance(arr,list) and arr and isinstance(arr[0],dict):
                print(f"\n{f}.{L} (n={len(arr)}) keys={list(arr[0].keys())[:9]}")
                for it in arr[:4]:
                    print("   ", it.get("ticker") or it.get("symbol"), {k:it.get(k) for k in list(it.keys())[:5] if k not in ('ticker','symbol')})
                break
    except Exception as e: print(f"{f} ERR {str(e)[:40]}")
print("DONE 2225")
