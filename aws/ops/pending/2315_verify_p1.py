import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:50]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen cycle-clock; polling...")
d=None
for i in range(16):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.0": d=cur; print(f"  t+{(i+1)*12}s v2.0 wrote (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if d:
    cy=d.get("cycle") or {}; rk=d.get("risk") or {}
    co=cy.get("coordinates"); tr=cy.get("trail") or []
    print("\ncoordinates:", co, "| trail pts:", len(tr), ("["+tr[0]['m']+"→"+tr[-1]['m']+"]" if tr else ""))
    print("quadrant_2d:", cy.get("quadrant_2d"), "| recession_prob:", cy.get("recession_prob_pct"))
    print("yield_curve:", cy.get("yield_curve_regime"), "| credit:", cy.get("credit_regime"), "| sector:", cy.get("sector_risk_appetite"))
    print("surprise_tilt:", json.dumps(cy.get("surprise_tilt")))
    al=cy.get("asset_leadership") or {}
    print("asset_leadership:", al.get("clock_phase"), "LEAD=", al.get("lead"), "| system_leaders=", al.get("system_leaders"))
    print("RORO:", rk.get("roro_score"), rk.get("read"), "| posture:", rk.get("posture"))
    print("verdict:", (d.get("verdict") or "")[:240])
    print("divergences:", len(d.get("divergences") or []))
else: print("NO v2.0 doc — current:", json.dumps(doc())[:200])
print("DONE 2315")
