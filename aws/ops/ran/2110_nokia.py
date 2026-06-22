import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
T={"NOK"}
def find(obj,want,path="",hits=None):
    if hits is None: hits=[]
    if isinstance(obj,dict):
        tk=obj.get("ticker") or obj.get("symbol") or obj.get("Ticker")
        if isinstance(tk,str) and tk.upper() in want:
            hits.append((path,{k:obj[k] for k in list(obj)[:12] if k not in ("chart","_chart","names","edges","nodes")}))
        for k,v in obj.items(): find(v,want,path+"."+k,hits)
    elif isinstance(obj,list):
        for it in obj: find(it,want,path,hits)
    return hits
files=["resilience.json","dark-pool.json","best-setups.json","master-ranker.json","boom-radar.json","squeeze-fuel.json","options-analytics.json","analyst-actions.json","estimate-revisions.json","capital-flow.json","flow-lookthrough.json","supply-chain-graph.json","equity-confluence.json","ai-rerating-radar.json","ai-infra-stack.json","signal-board.json"]
print("=== NOK across the system ===")
any_hit=False
for f in files:
    d=g("data/"+f)
    if "_err" in d: print(f"[{f}] missing"); continue
    h=find(d,T)
    if h:
        any_hit=True
        seen=set();uniq=[]
        for p,ctx in h:
            key=json.dumps(ctx,sort_keys=True,default=str)
            if key not in seen: seen.add(key);uniq.append((p,ctx))
        print(f"\n[{f}]")
        for p,ctx in uniq[:3]: print(f"   {p[:40]}: {json.dumps(ctx,default=str)[:300]}")
    else:
        print(f"[{f}] — NOK absent")
# is NOK even in the AI universe?
ais=g("data/ai-infra-stack.json")
if "_err" not in ais:
    allnames=[]
    for L in ais.get("stack",[]):
        for n in (L.get("names") or []):
            allnames.append((n.get("symbol"),L.get("layer")))
    nok=[x for x in allnames if x[0]=="NOK"]
    print("\nNOK in ai-infra-stack universe:",nok or "NO")
arr=g("data/ai-rerating-radar.json")
if "_err" not in arr:
    nok=[r for r in arr.get("all_ranked",[]) if r.get("symbol")=="NOK"]
    print("NOK in ai-rerating-radar:",[{k:r.get(k) for k in ("symbol","layer","cap_bucket","market_cap","growth_pct","ev_sales","discount_to_implied_pct","is_candidate","why")} for r in nok] or "NO (not in scored universe)")
print("\nany_hit:",any_hit)
print("DONE 2110")
