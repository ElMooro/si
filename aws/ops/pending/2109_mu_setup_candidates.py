import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
d=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read())
print("thesis:",str(d.get("thesis"))[:180])
sm=d.get("summary",{})
print("universe=%s priced=%s candidates=%s small_mid_candidates=%s"%(sm.get("n_universe"),sm.get("n_priced"),sm.get("n_candidates"),sm.get("n_small_mid_candidates")))
ranked=d.get("all_ranked",[])
# candidates = is_candidate True; show small/mid first (where a 10x is arithmetically possible)
cand=[r for r in ranked if r.get("is_candidate")]
def row(r):
    return {
      "sym":r.get("symbol"),"layer":r.get("layer"),"cap":r.get("cap_bucket"),
      "mcap_$B":round((r.get("market_cap") or 0)/1e9,1),
      "growth%":r.get("growth_pct"),"fwd_growth%":r.get("fwd_growth_pct"),
      "ev_sales":r.get("ev_sales"),"ev_sales_fair":r.get("ev_sales_implied"),
      "disc_to_fair%":r.get("discount_to_implied_pct"),"unpriced_z":r.get("unpriced_z"),
      "ret_3m%":r.get("ret_3m_pct"),"small_mid":r.get("is_small_mid"),
      "accel":r.get("accelerating"),"est_rising":r.get("estimates_rising"),
      "ai_deal":r.get("ai_deal"),"insider_buy":r.get("insider_buying"),
      "smart_money":r.get("smart_money_backed"),"why":str(r.get("why") or "")[:120]
    }
print("\n=== SMALL/MID-CAP 'MU SETUP' CANDIDATES (10x arithmetically possible) ===")
smc=[r for r in cand if r.get("is_small_mid")]
smc.sort(key=lambda r:(r.get("composite") or 0),reverse=True)
for r in smc: print(json.dumps(row(r),default=str))
print("\n=== LARGE/MEGA candidates (re-rate maybe, but NOT a 10x — too big) ===")
big=[r for r in cand if not r.get("is_small_mid")]
big.sort(key=lambda r:(r.get("composite") or 0),reverse=True)
for r in big[:10]: print(json.dumps({k:row(r)[k] for k in ("sym","layer","cap","mcap_$B","growth%","disc_to_fair%","why")},default=str))
# layer heat for context
ais=json.loads(s3.get_object(Bucket=B,Key="data/ai-infra-stack.json")["Body"].read())
print("\n=== LAYER HEAT (1m) — where the money's moving ===")
for L in sorted(ais.get("stack",[]),key=lambda x:-(x.get("layer_heat_1m_pct") or 0)):
    print(f"  {L.get('layer'):<20} {L.get('layer_heat_1m_pct'):>6}%  ({L.get('n_names')} names, {L.get('n_small_cap')} small-cap)")
print("DONE 2109")
