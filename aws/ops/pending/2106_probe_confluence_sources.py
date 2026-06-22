import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
def probe(fn, lists):
    d=g("data/"+fn)
    if "_err" in d: print(f"\n[{fn}] MISSING ({d['_err']})"); return
    print(f"\n[{fn}] keys={list(d.keys())[:16]}")
    for L in lists:
        v=d.get(L)
        if isinstance(v,list) and v:
            it=v[0]
            tk = it.get("ticker") or it.get("symbol") if isinstance(it,dict) else None
            print(f"   .{L} n={len(v)} hasTicker={'Y' if tk else 'N'} keys={list(it.keys())[:8] if isinstance(it,dict) else type(it).__name__}")
            if isinstance(it,dict): print(f"      sample: {json.dumps({k:it.get(k) for k in list(it)[:7]},default=str)[:200]}")
for fn,ls in [
  ("squeeze-fuel.json",["top_picks","board","top","candidates","names"]),
  ("boom-radar.json",["top_picks","about_to_boom","booming","candidates","board"]),
  ("options-analytics.json",["top_picks","signals","bullish","names","board"]),
  ("capital-flow.json",["leaders","top_inflows","inflows","complexes","top_picks","board"]),
  ("flow-lookthrough.json",["top_picks","names","leaders","board"]),
  ("supply-chain-graph.json",["supply_chain_laggards","top_picks","laggards"]),
  ("estimate-revisions.json",["top_picks","upward_revisions","estimate_strength_leaders"]),
  ("earnings-tracker.json",["pead_signals"]),
]:
    probe(fn,ls)

print("\n\n=== SCORECARD signal_type naming (so gating maps correctly) ===")
sc=g("data/signal-scorecard.json")
if "_err" not in sc:
    sts=[r.get("signal_type") for r in sc.get("scorecard",[])]
    eqs=[s for s in sts if any(x in str(s).lower() for x in ("resil","dark","squeeze","boom","option","revision","analyst","flow","supply","earn","pead"))]
    print("equity-roster-ish signal_types:",eqs[:30])
    print("sample alpha_status rows:",[(r.get("signal_type"),r.get("alpha_status"),r.get("net_info_ratio")) for r in sc.get("scorecard",[])[:6]])
ea=g("data/engine-alpha.json")
if "_err" not in ea:
    print("alpha_proven_signals:",ea.get("alpha_proven_signals"))
    print("engines keys sample:",list((ea.get('engines') or {}).keys())[:20])
print("DONE 2106")
