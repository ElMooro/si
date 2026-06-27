import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen v2.2 (AI may take 20-60s)...")
d=None
for i in range(20):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.2": d=cur; print(f"  t+{(i+1)*12}s v2.2 wrote (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if not d: print("NO v2.2:", json.dumps(doc())[:160]); print("DONE 2320"); raise SystemExit
cy=d.get("cycle") or {}; ai=d.get("ai")
print("\n=== NEW SIGNALS ===")
print("sahm:", cy.get("sahm"))
print("yield_curve_decomp:", json.dumps(cy.get("yield_curve_decomp")))
print("vol_regime:", cy.get("vol_regime"), "| dollar_regime:", cy.get("dollar_regime"), "| eps_breadth:", cy.get("eps_revision_breadth"))
print("next_3m_quadrant_odds:", json.dumps(cy.get("next_3m_quadrant_odds")))
print("\nverdict:", (d.get("verdict") or "")[:280])
print("\n=== AI SYNTHESIS ===")
if not ai: print("  ⚠ AI is null (GLM unavailable)")
else:
    print("  executive_read:", (ai.get("executive_read") or "")[:400])
    print("  regime_call:", ai.get("regime_call"))
    print("  bull_case:", json.dumps(ai.get("bull_case"))[:300])
    print("  bear_case:", json.dumps(ai.get("bear_case"))[:300])
    pos=ai.get("positioning") or {}
    print("  positioning.own:", json.dumps(pos.get("own"))[:200])
    print("  positioning.reduce:", json.dumps(pos.get("reduce"))[:200])
    print("  positioning.sizing:", pos.get("sizing"))
    print("  watch:", json.dumps(ai.get("watch"))[:300])
    print("  divergence_reads:", json.dumps(ai.get("divergence_reads"))[:350])
    print("  liquidity_read:", ai.get("liquidity_read"))
    print("  bottom_line:", ai.get("bottom_line"))
print("DONE 2320")
