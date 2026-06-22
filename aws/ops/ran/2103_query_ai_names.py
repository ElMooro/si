import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
TARGETS={"WYFI","APLD","CIFR","HUT","CORZ","WULF","IREN"}
# broader AI/datacenter/semis watch to detect what the system surfaces on its own
AIish={"NVDA","AMD","AVGO","MU","SMCI","DELL","ANET","VRT","CRWV","NBIS","MRVL","TSM","ARM","CLS","COHR","CIEN","SMH","PLTR","ORCL","CEG","VST","TLN","OKLO","SNOW","NOW","GOOGL","MSFT","AMZN","META","ASML","LRCX","KLAC","AMAT","PSTG","NTAP","WDC","STX","CRDO","ALAB","POWL","ETN","PWR"}
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:40]}
def find_tickers(obj, want, path="", hits=None):
    if hits is None: hits={}
    if isinstance(obj,dict):
        tk=obj.get("ticker") or obj.get("symbol") or obj.get("Ticker")
        if isinstance(tk,str) and tk.upper() in want:
            hits.setdefault(tk.upper(),[]).append((path, {k:obj[k] for k in list(obj)[:8] if k not in ("chart","_chart","events_shrugged","adverse_by_type","top_holds")}))
        for k,v in obj.items(): find_tickers(v,want,path+"."+k,hits)
    elif isinstance(obj,list):
        for it in obj: find_tickers(it,want,path,hits)
    return hits

print("=== data/ inventory (relevant engines) ===")
keys=[o["Key"] for o in s3.list_objects_v2(Bucket=B,Prefix="data/").get("Contents",[])]
rel=[k for k in keys if any(x in k for x in ("resilience","dark-pool","best-setups","master-ranker","boom-radar","squeeze","capital-flow","cap-flow","fund-flows","sector-emergence","regime-map","cycle-clock","options-analytics","analyst-actions","estimate-revisions","signal-board","engine-alpha","signal-scorecard","ai-","semis"))]
print("\n".join(sorted(rel)))

print("\n\n========== WHAT THE SYSTEM SAYS ABOUT YOUR 7 NAMES ==========")
for k in ["data/resilience.json","data/dark-pool.json","data/best-setups.json","data/master-ranker.json","data/boom-radar.json","data/squeeze-fuel.json","data/options-analytics.json","data/analyst-actions.json","data/estimate-revisions.json","data/signal-board.json"]:
    d=g(k)
    if "_err" in d: print(f"\n[{k.split('/')[-1]}] (missing)"); continue
    hits=find_tickers(d,TARGETS)
    if hits:
        print(f"\n[{k.split('/')[-1]}]")
        for tk,occ in hits.items():
            seen=set();uniq=[]
            for p,ctx in occ:
                key=json.dumps(ctx,sort_keys=True,default=str)
                if key not in seen: seen.add(key);uniq.append((p,ctx))
            for p,ctx in uniq[:2]:
                print(f"   {tk}: {json.dumps(ctx,default=str)[:240]}")
    else:
        print(f"\n[{k.split('/')[-1]}] — none of the 7 present")

print("\n\n========== MACRO POSTURE (the backdrop for this theme) ==========")
cc=g("data/cycle-clock.json")
if "_err" not in cc:
    print("CYCLE CLOCK:",json.dumps({k:cc.get(k) for k in ("phase","clock_phase","investment_clock_phase","cycle_phase","liquidity_squeeze_risk","squeeze_risk","posture","confidence")},default=str)[:300])
rm=g("data/regime-map.json")
if "_err" not in rm:
    print("REGIME MAP:",json.dumps({k:rm.get(k) for k in ("risk_on","risk_on_score","regime","posture","sector_tilt","tilt","breadth_pct")},default=str)[:300])
se=g("data/sector-emergence.json")
if "_err" not in se:
    em=se.get("emerging_now") or se.get("emerging") or []
    secs=se.get("sectors") or []
    techrow=[s for s in secs if isinstance(s,dict) and "tech" in str(s.get("sector","")).lower()]
    print("SECTOR EMERGENCE emerging_now:",json.dumps(em,default=str)[:200])
    print("  Technology row:",json.dumps(techrow[:1],default=str)[:240])

print("\n\n========== WHAT'S ACTUALLY PROVEN (engine-alpha) ==========")
ea=g("data/engine-alpha.json")
if "_err" not in ea:
    print("alpha_proven_signals:",ea.get("alpha_proven_signals"))
    print("n_proven:",ea.get("n_alpha_proven"),"n_negative:",ea.get("n_alpha_negative"))

print("\n\n========== AI/DATACENTER NAMES THE SYSTEM SURFACES ON ITS OWN ==========")
for k in ["data/resilience.json","data/dark-pool.json","data/best-setups.json","data/master-ranker.json","data/boom-radar.json","data/squeeze-fuel.json","data/capital-flow.json","data/capital-flow-radar.json"]:
    d=g(k)
    if "_err" in d: continue
    hits=find_tickers(d,AIish)
    if hits:
        print(f"\n[{k.split('/')[-1]}] AI-ish names present: {sorted(hits.keys())}")
print("DONE 2103")
