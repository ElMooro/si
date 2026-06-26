import boto3, json
s3=boto3.client("s3","us-east-1")
def g(f): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
# supply-inflection by_theme structure
si=g("supply-inflection")
bt=si.get("by_theme") or {}
print("supply-inflection by_theme keys:", list(bt.keys())[:20])
k0=list(bt.keys())[0] if bt else None
if k0: print("  sample by_theme['%s']:"%k0, json.dumps(bt[k0])[:200])
print("  summary:", json.dumps(si.get("summary"))[:200])
sig0=list((si.get("signals") or {}).values())[0] if si.get("signals") else {}
print("  signal sample:", json.dumps(sig0)[:220])
# bottleneck-boom ranks score field
bb=g("bottleneck-boom"); r=bb.get("ranks") or []
print("\nbottleneck-boom ranks[0]:", json.dumps(r[0])[:300] if r else "none")
# chokepoint criticality + discount
cp=g("chokepoint")
print("\nchokepoint scoring:", json.dumps(cp.get("scoring"))[:160])
cb=cp.get("cheap_chokepoint_book") or []
print("chokepoint cheap_chokepoint_book[0]:", json.dumps(cb[0])[:250] if cb else "none")
ac=cp.get("all_chokepoints") or []
print("chokepoint all_chokepoints[0]:", json.dumps(ac[0])[:200] if ac else "none")
# narrative-vs-tape quiet
nt=g("narrative-vs-tape"); qa=nt.get("quiet_accumulation") or []
print("\nnarrative quiet_accumulation[0]:", json.dumps(qa[0])[:200] if qa else "none")
# themes-detected phase
td=g("themes-detected"); th=td.get("themes") or []
print("\nthemes-detected[0]:", json.dumps(th[0])[:220] if th else "none")
print("DONE 2226")
