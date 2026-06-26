import boto3, json
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
sigs=d.get("signals")
print("signals type:", type(sigs).__name__)
items=[]
if isinstance(sigs,dict):
    for name,v in sigs.items():
        if isinstance(v,dict): items.append((name,v))
elif isinstance(sigs,list):
    for v in sigs:
        if isinstance(v,dict): items.append((v.get("name") or v.get("id"),v))
print(f"INPUT SIGNALS (n={len(items)}):")
if items: print("  sample item keys:", list(items[0][1].keys())[:12])
def sc(v): return v.get("score") or v.get("inflection_score") or v.get("tightness_score") or 0
for name,v in sorted(items,key=lambda x:-sc(x[1])):
    print(f"   {str(name)[:22]:<22} score={sc(v)} dir={v.get('direction') or v.get('signal')} "
          f"chg90={v.get('chg_90d') or v.get('pct_90d') or v.get('change_90d')} "
          f"etfs={v.get('themes') or v.get('etfs') or v.get('beneficiary_etfs') or v.get('tickers')}")
bt=d.get("by_theme")
if isinstance(bt,dict):
    print("\nby_theme keys:", list(bt.keys())[:15])
    for k,v in list(bt.items())[:6]: print(f"   {k}: {json.dumps(v)[:120]}")
# piggyback rule search
print("\nPIGGYBACK candidates (ENABLED scheduled, <5 targets):")
for r in ev.list_rules().get("Rules",[]):
    if r.get("State")!="ENABLED" or not r.get("ScheduleExpression"): continue
    se=r["ScheduleExpression"]
    if any(x in se for x in ("cron(0 7","cron(0 6","cron(30 6","cron(0 8","cron(0 5","rate(1 day")):
        try: n=len(ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]))
        except Exception: n=99
        if n<5: print(f"   {r['Name']} | {se} | targets={n}")
print("DONE 2229")
