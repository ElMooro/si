import boto3, json
s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_ERR":str(e)[:40]}
def show(name, paths):
    d=gj(f"data/{name}.json")
    print(f"\n■ {name}")
    if "_ERR" in d: print("   ERR",d["_ERR"]); return
    for p in paths:
        cur=d; ok=True
        for seg in p.split("."):
            if isinstance(cur,dict) and seg in cur: cur=cur[seg]
            else: ok=False; break
        print(f"   {p} = {json.dumps(cur)[:150]}" if ok else f"   {p} = (absent)")
show("fed-speak",["aggregate","latest_hawkish"])
show("fed-nlp",["drift"])
show("bond-vol",["risk_posture","term_structure","composite_percentile","trend"])
show("vol-regime",["composite_regime","regime_counts"])
show("aaii-sentiment",["latest","z_scores","extremes","interpretation"])
show("retail-sentiment",["market_regime","market_regime_signal"])
show("credit-equity-divergence",["state","current_metrics","regime_explanation"])
show("breadth-thrust",["current_readings","forward_expectations","signal_strength"])
show("gold-equity-rotation",["current_metrics","regime_explanation"])
show("china-liquidity",["money","credit_impulse","regime_read"])
show("liquidity-pulse",["summary","composites","transitions"])
show("liquidity-inflection",["lead_estimates","usd","china"])
show("cross-asset-regime",["regime_20d","regime_60d","correlation_breaks","alerts"])
show("yen-carry",["unwind_risk_score","unwind_risk_label","carry_regime","positioning","triggers"])
show("tic-flows",["composite_tic_stress","interpretation","net_purchases"])
show("labor-leading",["claims","interpretation"])
show("activity-nowcast",["activity_index","momentum"])
show("consumer-pulse",["pulse_index"])
show("bank-stress",["bank_stress_score","reserve_adequacy"])
show("commodity-curves",["composite_regime","composite_signal"])
print("\nDONE 2330")
