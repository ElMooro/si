"""ops 2598 — verify/create buyback-engine schedule; verify live page buyback panel + confluence fusion."""
import boto3, urllib.request, json, time
REGION="us-east-1"; ACCT="857687956942"
ev=boto3.client("events",region_name=REGION); lam=boto3.client("lambda",region_name=REGION)
FN="justhodl-buyback-engine"; RULE="justhodl-buyback-engine-daily"; SCHED="cron(30 13 * * ? *)"
try:
    r=ev.describe_rule(Name=RULE); tg=ev.list_targets_by_rule(Rule=RULE).get("Targets",[])
    print(f"  ✓ {RULE} EXISTS: {r.get('ScheduleExpression')} {r.get('State')} targets={len(tg)}")
except ev.exceptions.ResourceNotFoundException:
    ev.put_rule(Name=RULE, ScheduleExpression=SCHED, State="ENABLED", Description="Daily buyback intelligence")
    try:
        lam.add_permission(FunctionName=FN, StatementId=f"{RULE}-invoke", Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCT}:rule/{RULE}")
    except Exception: pass
    ev.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":f"arn:aws:lambda:{REGION}:{ACCT}:function:{FN}"}])
    print(f"  ＋ CREATED {RULE}: {SCHED} -> {FN}")
def get(u):
    return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
# live page has buyback panel?
try:
    html=get(f"https://justhodl.ai/attention.html?cb={int(time.time())}")
    print("  live page Corporate Buybacks panel:", "OK" if "Corporate Buybacks" in html else "MISS",
          "| loads corporate_buybacks:", "OK" if "corporate_buybacks" in html else "MISS")
except Exception as e: print("  page err:", str(e)[:60])
# confluence output now has corporate_buybacks panel + buyback subscore?
try:
    j=json.loads(get(f"https://justhodl-data-proxy.raafouis.workers.dev/data/attention-confluence.json?t={int(time.time())}"))
    print("  confluence corporate_buybacks panel:", len(j.get("panels",{}).get("corporate_buybacks",[])))
    print("  buyback in smart_families:", "buyback" in j.get("scoring",{}).get("smart_families",{}))
    # buyback-engine feed live?
    bb=json.loads(get(f"https://justhodl-data-proxy.raafouis.workers.dev/data/buyback-engine.json?t={int(time.time())}"))
    print("  buyback-engine feed: n_scored", bb.get("n_scored"), "pumps", len(bb.get("high_conviction_pumps",[])), "counts", bb.get("counts"))
except Exception as e: print("  feed err:", str(e)[:80])
print("DONE 2598")
