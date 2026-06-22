import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
T7={"WYFI","APLD","CIFR","HUT","CORZ","WULF","IREN"}

arr=g("data/ai-rerating-radar.json")
print("=== AI-RERATING-RADAR ===")
print("thesis:",str(arr.get("thesis"))[:280])
print("summary:",json.dumps(arr.get("summary"),default=str)[:400])
print("regression:",json.dumps(arr.get("regression"),default=str)[:300])
ranked=arr.get("all_ranked",[])
print(f"\nall_ranked n={len(ranked)} — item keys:",list(ranked[0].keys()) if ranked else "—")
print("\n-- YOUR 7 in the rerating radar --")
for it in ranked:
    if (it.get("ticker") or "").upper() in T7:
        print("   ",json.dumps({k:it.get(k) for k in list(it)[:10]},default=str)[:300])
print("\n-- TOP 15 ranked (whatever the radar's sort is) --")
for it in ranked[:15]:
    print("   ",json.dumps({k:it.get(k) for k in list(it)[:8]},default=str)[:240])

print("\n=== miners_to_ai layer detail ===")
ais=g("data/ai-infra-stack.json")
for L in ais.get("stack",[]):
    if L.get("layer") in ("miners_to_ai","neocloud","memory","power_grid"):
        print("   ",json.dumps({k:L.get(k) for k in ("layer","layer_heat_1m_pct","n_names","n_small_cap","top_names","leaders","names")},default=str)[:260])

print("\n=== MASTER-RANKER keys + top ===")
mr=g("data/master-ranker.json")
if "_err" not in mr:
    print("keys:",list(mr.keys())[:20])
    for ak in list(mr.keys()):
        v=mr.get(ak)
        if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0]):
            print(f"  using .{ak} (n={len(v)}): top 15:")
            for i,it in enumerate(v[:15]):
                print(f"    #{i+1} {it.get('ticker') or it.get('symbol')} {json.dumps({k:it.get(k) for k in list(it) if k in ('score','verdict','conviction','rank_score','composite','tier')},default=str)[:80]}")
            break

print("\n=== CEG resilience verify ===")
res=g("data/resilience.json")
found=False
for r in res.get("all_resilient",[]):
    if r.get("ticker")=="CEG":
        found=True;print("   CEG:",json.dumps({k:r.get(k) for k in ("resilience","stage","mean_abnormal_on_adverse_pct","adverse_hit_rate_pct","flow_confirmed","dominant_adverse_type")},default=str))
print("   CEG in all_resilient:",found)
print("DONE 2105")
