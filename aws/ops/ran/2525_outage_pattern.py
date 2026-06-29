import boto3, time, datetime as dt
logs=boto3.client("logs","us-east-1")
now=int(time.time()*1000); start=now-14*24*3600*1000
ENGINES=["justhodl-morning-intelligence","justhodl-ai-chat","justhodl-page-ai-commentary",
         "justhodl-cycle-clock","justhodl-brain-sync","justhodl-my-brief"]
for fn in ENGINES:
    gl=f"/aws/lambda/{fn}"
    try:
        days={}
        tok=None; n=0
        for _ in range(6):
            kw=dict(logGroupName=gl,startTime=start,
                    filterPattern='?"400" ?"credit balance" ?"invalid_request" ?"AI router" ?"[AI]"',limit=1000)
            if tok: kw["nextToken"]=tok
            r=logs.filter_log_events(**kw)
            for e in r.get("events",[]):
                d=dt.datetime.utcfromtimestamp(e["timestamp"]/1000).strftime("%m-%d")
                days[d]=days.get(d,0)+1; n+=1
            tok=r.get("nextToken")
            if not tok: break
        if days:
            span=sorted(days)
            print(f"{fn}: {n} err events on {len(days)} days -> {span[0]}..{span[-1]}")
            print("    per-day:", dict(sorted(days.items())))
        else:
            print(f"{fn}: no 400/credit/AI-error events in 14d")
    except logs.exceptions.ResourceNotFoundException:
        print(f"{fn}: (no log group)")
    except Exception as e:
        print(f"{fn}: ERR {str(e)[:80]}")
# also: capture an ACTUAL 400 body now is impossible (works now); instead show a sample full error line if any engine logged the body
print("\n=== sample error lines (any engine logging the BODY/reason) ===")
for fn in ["justhodl-cycle-clock","justhodl-ai-chat"]:
    try:
        ev=logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}",startTime=start,
            filterPattern='?"credit balance" ?"invalid_request" ?"insufficient"',limit=5).get("events",[])
        for e in ev[:3]:
            d=dt.datetime.utcfromtimestamp(e["timestamp"]/1000).isoformat()
            print(f"  {fn} {d}: {e['message'][:200].strip()}")
    except Exception as ex: print(f"  {fn}: {str(ex)[:60]}")
print("DONE 2525")
