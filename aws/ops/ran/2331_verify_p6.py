import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen v2.4...")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.4": d=cur; print(f"  t+{(i+1)*12}s v2.4 (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if not d: print("NO v2.4:",json.dumps(doc())[:150]); print("DONE 2331"); raise SystemExit
rfv=d.get("rates_fed_vol") or {}; ps=d.get("positioning") or {}; gd=d.get("growth_depth") or {}; car=d.get("cross_asset_risk") or {}; gl=d.get("global_liquidity") or {}
print("\n=== RATES/FED/VOL ===")
print("  Fed:",rfv.get("fed_tone"),"| drift:",rfv.get("fed_drift"),"z",rfv.get("fed_drift_z"))
print("  MOVE:",rfv.get("move_level"),"pctile",rfv.get("move_pctile"),rfv.get("move_regime"),"| bondvol:",rfv.get("bond_vol_regime"),rfv.get("bond_vol_posture"))
print("  equity vol:",rfv.get("equity_vol_regime"),rfv.get("equity_vol_score"),"| vov:",rfv.get("vov_state"),"| tail:",rfv.get("tail_state"))
print("=== POSITIONING ===")
print("  AAII:",ps.get("aaii_bull_pct"),"bull /",ps.get("aaii_bear_pct"),"bear z",ps.get("aaii_spread_z"),"|",ps.get("aaii_extreme"))
print("  retail:",ps.get("retail_regime"),"| credit-eq:",ps.get("credit_equity_state"),"| breadth:",ps.get("breadth_thrust_state"),"| gold-eq:",ps.get("gold_equity_state"))
print("=== GROWTH DEPTH ===")
print("  labor:",gd.get("labor_regime"),"| activity:",gd.get("activity_index"),gd.get("activity_regime"),"| consumer:",gd.get("consumer_index"),gd.get("consumer_regime"))
print("  bank stress:",gd.get("bank_stress_score"),gd.get("bank_stress_regime"),"| reserves/GDP:",gd.get("reserves_to_gdp_pct"),"%")
print("=== GLOBAL LIQ G4 ===")
print("  china:",json.dumps(gl.get("china")),"| pulse:",(gl.get("pulse") or {}).get("liquidity_regime"),(gl.get("pulse") or {}).get("liquidity_score"))
print("  inflection usd:",(gl.get("inflection") or {}).get("usd_state"))
print("=== CROSS-ASSET RISK ===")
print("  yen-carry:",(car.get("yen_carry") or {}).get("unwind_score"),(car.get("yen_carry") or {}).get("unwind_label"))
print("  corr regime:",(car.get("correlation_regime") or {}).get("r20d"),"| top alert:",(car.get("correlation_regime") or {}).get("top_alert"))
print("  TIC:",(car.get("tic") or {}).get("regime"),"| commodity:",(car.get("commodity_curve") or {}).get("regime"))
print("\ndivergences:",len(d.get("divergences") or []))
for dv in (d.get("divergences") or []): print("   -",dv[:95])
print("AI:","OK" if d.get("ai") else "null(quota)")
print("DONE 2331")
