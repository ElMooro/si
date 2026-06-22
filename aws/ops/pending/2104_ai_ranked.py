import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}

print("=== AI-INFRA-STACK ===")
ais=g("data/ai-infra-stack.json")
if "_err" not in ais:
    print("keys:",list(ais.keys())[:20])
    for ak in ("stack","layers","names","tickers","top_picks","leaders","ranked","components"):
        v=ais.get(ak)
        if isinstance(v,list) and v:
            print(f".{ak} n={len(v)}:")
            for it in v[:14]:
                if isinstance(it,dict): print("   ",json.dumps({k:it.get(k) for k in list(it)[:6]},default=str)[:200])
        elif isinstance(v,dict) and v:
            print(f".{ak}: {json.dumps(v,default=str)[:400]}")
else: print(ais)

print("\n=== AI-RERATING-RADAR ===")
arr=g("data/ai-rerating-radar.json")
if "_err" not in arr:
    print("keys:",list(arr.keys())[:20])
    for ak in ("top_picks","ranked","names","leaders","candidates","rerating","results","board"):
        v=arr.get(ak)
        if isinstance(v,list) and v:
            print(f".{ak} n={len(v)}:")
            for it in v[:14]:
                if isinstance(it,dict): print("   ",json.dumps({k:it.get(k) for k in list(it)[:7]},default=str)[:220])
else: print(arr)

print("\n=== MASTER-RANKER top 20 (where do AI names rank overall?) ===")
mr=g("data/master-ranker.json")
if "_err" not in mr:
    for ak in ("ranked","top","rankings","names","board","top_picks","leaders"):
        v=mr.get(ak)
        if isinstance(v,list) and v:
            for i,it in enumerate(v[:20]):
                if isinstance(it,dict):
                    t=it.get("ticker") or it.get("symbol")
                    sc=it.get("score") or it.get("rank_score") or it.get("composite")
                    print(f"   #{i+1} {t} score={sc}")
            break

print("\n=== BEST-SETUPS: the AI names + their grade/conviction ===")
bs=g("data/best-setups.json")
AIish={"NVDA","AMD","AVGO","MU","SMCI","DELL","ANET","VRT","MRVL","ARM","LRCX","KLAC","AMAT","GOOGL","MSFT","AMZN","META","ASML","ORCL","PLTR","WDC","CEG","STX","PWR","NOW","SMH"}
def walk(o,out):
    if isinstance(o,dict):
        t=o.get("ticker") or o.get("symbol")
        if isinstance(t,str) and t.upper() in AIish: out.append(o)
        for v in o.values(): walk(v,out)
    elif isinstance(o,list):
        for v in o: walk(v,out)
if "_err" not in bs:
    out=[];walk(bs,out);seen=set()
    for it in out:
        t=it.get("ticker") or it.get("symbol")
        if t in seen: continue
        seen.add(t)
        print(f"   {t}: {json.dumps({k:it.get(k) for k in list(it) if k in ('ticker','verdict','conviction','grade','score','setup','confluence','n_eff','tier','reason')},default=str)[:240]}")

print("\n=== CEG (only AI-adjacent name showing RESILIENCE) ===")
res=g("data/resilience.json")
if "_err" not in res:
    for r in (res.get("all_resilient",[])+res.get("about_to_boom",[])):
        if r.get("ticker")=="CEG":
            print("   ",json.dumps({k:r.get(k) for k in ("ticker","resilience","stage","abnormal_basis","mean_abnormal_on_adverse_pct","adverse_hit_rate_pct","flow_confirmed","flow_score","dark_pool_state","dominant_adverse_type")},default=str)); break

print("\n=== DARK-POOL accumulation detail (DELL/NTAP/SNOW) ===")
dp=g("data/dark-pool.json")
if "_err" not in dp:
    for it in (dp.get("board",[])+dp.get("top_accumulation",[])):
        if it.get("ticker") in {"DELL","NTAP","SNOW"}:
            print("   ",json.dumps({k:it.get(k) for k in ("ticker","state","dark_pool_pct","dark_accel","score")},default=str))

print("\n=== CYCLE CLOCK (real fields) ===")
cc=g("data/cycle-clock.json")
if "_err" not in cc:
    print("   keys:",list(cc.keys())[:18])
    print("   ",json.dumps({k:cc[k] for k in list(cc)[:14] if not isinstance(cc[k],(list,dict))},default=str)[:400])
print("DONE 2104")
