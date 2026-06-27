import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:60]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen v2.5...")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.5": d=cur; print(f"  t+{(i+1)*12}s v2.5 (dur {cur.get('duration_s')}s)"); break
    print(f"  t+{(i+1)*12}s...")
if not d: print("NO v2.5:",json.dumps(doc())[:150]); print("DONE 2337"); raise SystemExit
rfv=d.get("rates_fed_vol") or {}; ps=d.get("positioning") or {}; ss=d.get("stress_scenarios") or {}
fp=rfv.get("fed_path") or {}
print("\n=== FED PATH ===")
print("  current midpoint:",fp.get("current_midpoint"),"| next FOMC:",fp.get("next_date"),f"({fp.get('next_days')}d)","implied",fp.get("implied_move_bps"),"bps → post",fp.get("post_rate_pct"),"%")
print("  6mo:",json.dumps(fp.get("summary_6mo"))[:160])
print("  tail:",rfv.get("tail_gauge"),rfv.get("tail_regime"),"| valuation",rfv.get("tail_valuation"))
print("=== COT ===")
print("  ",json.dumps(ps.get("cot")))
print("=== STRESS SCENARIOS ===")
print("  top:",json.dumps(ss.get("top")))
for sc in (ss.get("scenarios") or []): print(f"   - {sc.get('key')} {sc.get('prob_pct')}%  W:{sc.get('winners')} L:{sc.get('losers')}")
print("  asset impact winners:",json.dumps(ss.get("asset_impact_winners")))
print("  tail:",ss.get("tail_gauge"),ss.get("tail_regime"),"| EA CISS:",ss.get("ea_ciss_regime"),"| corr:",ss.get("correlation_signal"),ss.get("correlation_z"))
print("\ndivergences:",len(d.get("divergences") or []))
for dv in (d.get("divergences") or []): print("   -",dv[:88])
print("AI:","OK" if d.get("ai") else "null(quota)")
print("DONE 2337")
