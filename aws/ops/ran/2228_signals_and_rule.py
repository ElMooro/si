import boto3, json
ev=boto3.client("events","us-east-1"); s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
sigs=d.get("signals") or []
print(f"INPUT SIGNALS (n={len(sigs)}), by score:")
if sigs and isinstance(sigs[0],dict):
    print("  item keys:", list(sigs[0].keys()))
    for s in sorted(sigs,key=lambda x:-(x.get('score') or 0)):
        print(f"   {str(s.get('name') or s.get('id'))[:22]:<22} score={s.get('score')} dir={s.get('direction')} "
              f"chg90={s.get('chg_90d') or s.get('pct_90d')} etfs={s.get('themes') or s.get('etfs') or s.get('tickers')}")
bt=d.get("by_theme")
if isinstance(bt,dict):
    print("\nby_theme (input -> beneficiary ETFs):")
    for k,v in list(bt.items())[:12]: print(f"   {k}: {v if not isinstance(v,dict) else list(v.items())[:4]}")
# find an ENABLED daily-ish rule with <5 targets to piggyback on
print("\nPIGGYBACK candidates (ENABLED, scheduled, <5 targets):")
cand=[]
for r in ev.list_rules().get("Rules",[]):
    if r.get("State")!="ENABLED": continue
    se=r.get("ScheduleExpression","")
    if not se: continue
    try:
        n=len(ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[]))
    except Exception: n=99
    if n<5 and ("cron(0 7" in se or "cron(0 6" in se or "cron(30 6" in se or "cron(0 8" in se or "rate(1 day" in se):
        cand.append((r["Name"],se,n))
for c in cand[:12]: print("   ",c)
print("DONE 2228")
