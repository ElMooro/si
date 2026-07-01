import urllib.request, json, boto3, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
# 1) feed via proxy (what the page fetches)
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/deal-scanner.json?cb={int(time.time())}"))
s=j.get("summary",{})
print("FEED via proxy: n_deals",s.get("n_deals"),"green",s.get("n_green"),"ai",s.get("n_ai"),"ai_mega",s.get("n_ai_mega"),"| updated",j.get("generated_at"))
# fields the page needs on each deal
need=["symbol","name","title","highlight","ai_relevant","vs_market_cap_pct","materiality_pct","deal_value_str","why","cap_bucket","url","age_h"]
d0=(j.get("deals") or [{}])[0]
print("first deal field coverage:",{k:(k in d0) for k in need})
print("  sample why:",str(d0.get("why"))[:80])
# 2) schedule (auto-update)
ev=boto3.client("events",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
try:
    pol=lam.get_policy(FunctionName="justhodl-deal-scanner")["Policy"]
    import re
    rules=set(re.findall(r'rule/([A-Za-z0-9_-]+)',pol))
    print("EventBridge rules attached:",rules or "NONE")
    for r in rules:
        try:
            rr=ev.describe_rule(Name=r); print(f"   {r}: {rr.get('ScheduleExpression')} [{rr.get('State')}]")
        except Exception as e: print("   ",r,"describe err",str(e)[:50])
except Exception as e: print("policy err:",str(e)[:80])
# 3) live page reads the right feed + structure
html=get(f"https://justhodl.ai/deal-scanner.html?cb={int(time.time())}")
for k,n in {"reads deal-scanner.json":"deal-scanner.json","filters ai_relevant":"ai_relevant","reads summary.n_deals":"n_deals","highlight green tier":'highlight===\"green\"'}.items():
    print(f"  page [{'OK' if n in html else 'MISS'}] {k}")
print("DONE 2641")
