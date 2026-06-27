import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_ERR":str(e)[:60]}
print("=== FEDWATCH invoke response ===")
try:
    r=lam.invoke(FunctionName="justhodl-fedwatch-rate-probability",InvocationType="RequestResponse",Payload=b"{}")
    print("  FunctionError:", r.get("FunctionError"))
    body=r["Payload"].read().decode()[:500]
    print("  payload:", body)
    time.sleep(2)
    d=gj("data/fedwatch.json")
    print("  fedwatch.json now:", "STILL MISSING" if "_ERR" in d else "keys="+str(list(d.keys())[:16]))
    if "_ERR" not in d:
        for k in ("base_rate","current_target","next_meeting","implied_cuts","probabilities","summary","headline","path","expected_moves","meetings"):
            if k in d: print(f"     {k}: {json.dumps(d[k])[:170]}")
except Exception as e: print("  invoke err:", str(e)[:160])

print("\n=== stress-scenarios full ===")
d=gj("data/stress-scenarios.json")
if "_ERR" not in d:
    print("  top_scenario:", json.dumps(d.get("top_scenario")))
    for sc in (d.get("scenarios") or [])[:6]:
        print(f"   - {sc.get('key')} {sc.get('probability_pct')}%  {sc.get('name')}")
    print("  scenario[0] keys:", list((d.get('scenarios') or [{}])[0].keys()))
    ai=d.get("asset_impact")
    print("  asset_impact type:", type(ai).__name__, "| sample:", json.dumps(ai)[:260])

print("\n=== tail-risk ===")
d=gj("data/tail-risk.json")
if "_ERR" not in d:
    for k in ("system_tail_gauge","tail_regime","tail_valuation","interpretation","thesis"):
        if k in d: print(f"  {k}: {json.dumps(d[k])[:200]}")
print("\n=== ciss-stress ===")
d=gj("data/ciss-stress.json")
if "_ERR" not in d:
    for k in ("ea_composite","ea_regime","ea_composite_date"):
        if k in d: print(f"  {k}: {json.dumps(d[k])[:120]}")
print("\n=== correlation-breaks ===")
d=gj("data/correlation-breaks.json")
if "_ERR" not in d:
    for k in ("frobenius_z_score_1y","signal"):
        if k in d: print(f"  {k}: {json.dumps(d[k])[:160]}")
print("DONE 2335")
