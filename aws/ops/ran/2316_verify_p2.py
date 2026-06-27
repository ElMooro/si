import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:50]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen; polling for v2.1...")
d=None
for i in range(16):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.1": d=cur; print(f"  t+{(i+1)*12}s v2.1 (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if d:
    cy=d.get("cycle") or {}; lq=d.get("liquidity") or {}; an=d.get("analogs") or {}
    print("\nheadline:", cy.get("headline_phase"), cy.get("headline_quadrant"), "| macro-regime:", cy.get("macro_regime_quadrant"))
    nl=lq.get("net_liquidity") or {}
    print("net liquidity: net", nl.get("net_tn"),"T (WALCL",nl.get("walcl_tn"),"− RRP",nl.get("rrp_tn"),"− TGA",nl.get("tga_tn"),") Δ13w",nl.get("net_13w_delta_bn"),"B as_of",nl.get("as_of"))
    print("flickers:", lq.get("flickers"))
    near=an.get("nearest") or []
    print("analogs nearest:", [(a.get('date'),round(a.get('similarity',0)*100),a.get('forward_63d_pct')) for a in near])
    print("analog dir:", an.get("directional_call"), "| unprec:", an.get("unprecedentedness"))
    print("verdict:", (d.get("verdict") or "")[:260])
else: print("NO v2.1:", json.dumps(doc())[:160])
print("DONE 2316")
