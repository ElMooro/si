import boto3, json, time, urllib.request
ev=boto3.client("events","us-east-1")
# confirm schedules
for r in ["justhodl-ma200-reclaim-daily","justhodl-crypto-ma200-daily"]:
    try:
        d=ev.describe_rule(Name=r); n=len(ev.list_targets_by_rule(Rule=r).get("Targets",[]))
        print(f"  {r}: {d.get('ScheduleExpression')} {d.get('State')} targets={n}")
    except Exception as e: print(f"  {r}: {str(e)[:40]}")
# confirm orphan gone
try: ev.describe_rule(Name="aiapi-hourly-collection"); print("  orphan STILL EXISTS")
except Exception: print("  orphan aiapi-hourly-collection: retired ✓")
# page live
time.sleep(120)
try:
    r=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/ma200-radar.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
    b=r.read(1200).decode("utf-8","ignore")
    print("  page:",r.getcode(),"crypto_section=",("Crypto" in b or "₿" in b or "200-DMA" in b))
except Exception as e: print("  page:",str(e)[:50])
print("DONE 2168")
