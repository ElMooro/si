import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
def p(label,v): print(f"  {label}: {v}")

print("════════ WHERE ARE WE IN THE CYCLE? ════════")
d=g("data/us-cycle.json")
print("\n[US CYCLE]")
p("cycle_score",d.get("cycle_score"))
pil=d.get("pillars")
if isinstance(pil,dict):
    for k,v in pil.items():
        if isinstance(v,dict): p(k,{kk:v.get(kk) for kk in ("score","label","phase","read","state") if kk in v})
p("alerts",d.get("alerts"))

d=g("data/global-business-cycle.json")
print("\n[GLOBAL BUSINESS CYCLE]")
p("aggregate",d.get("aggregate"))
p("interpretation",str(d.get("interpretation"))[:300])

d=g("data/macro-nowcast.json")
print("\n[MACRO NOWCAST]")
for k in ("normalized_score","regime","regime_color"): p(k,d.get(k))
p("regime_spy_performance",d.get("regime_spy_performance"))

d=g("data/regime.json")
print("\n[MACRO REGIME]")
p("current",d.get("current"))

d=g("data/regime-playbook.json")
print("\n[REGIME PLAYBOOK]")
for k in ("regime_key","current_fingerprint"): p(k,str(d.get(k))[:200])

print("\n════════ IS THERE A LIQUIDITY SQUEEZE? ════════")
d=g("data/global-liquidity.json")
print("\n[GLOBAL LIQUIDITY]")
for k in ("global_liquidity_index","fed_net_liquidity","regime","regime_read","global_impulse_13w_pct"): p(k,str(d.get(k))[:160])

d=g("data/liquidity-flow.json")
print("\n[LIQUIDITY FLOW]")
for k in ("regime","interpretation"): p(k,str(d.get(k))[:220])
p("deltas",d.get("deltas"))

d=g("data/liquidity-credit-engine.json")
print("\n[LIQUIDITY & CREDIT ENGINE]")
for k in ("regime","composite"): p(k,str(d.get(k))[:200])
p("interpretation",str(d.get("interpretation"))[:220])

d=g("data/crisis-composite.json")
print("\n[CRISIS COMPOSITE]")
for k in ("master_crisis_score","defcon_level","defcon_name","trend"): p(k,d.get(k))
p("primary_drivers",str(d.get("primary_drivers"))[:240])

d=g("data/crisis-canaries.json")
print("\n[CRISIS CANARIES]")
for k in ("composite_score","level","red_count","level_v3"): p(k,d.get(k))

d=g("data/global-stress.json")
print("\n[GLOBAL STRESS]")
for k in ("global_stress_index","global_stress_level","stress_momentum"): p(k,str(d.get(k))[:120])

d=g("data/systemic-stress.json")
print("\n[SYSTEMIC STRESS]")
p("headline",str(d.get("headline"))[:200])
p("composite",d.get("composite"))
print("\nDONE 2087")
