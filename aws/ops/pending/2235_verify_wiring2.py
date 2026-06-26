import json, boto3, sys, os
s3=boto3.client("s3","us-east-1")
# ---- PROVE master-ranker plumbing: import its own build_ticker_index against live S3 ----
mr_dir="aws/lambdas/justhodl-master-ranker/source"
sys.path.insert(0, mr_dir)
# also add shared in case of bundled imports
for cand in ["aws/shared","aws/lambdas/justhodl-master-ranker/source"]:
    if os.path.isdir(cand): sys.path.insert(0, cand)
try:
    import lambda_function as MR
    idx, feeds = MR.build_ticker_index()
    feed_loaded = bool(feeds.get("scarcity_radar"))
    carriers=[(sym, s["scarcity_radar"]) for sym,s in idx.items() if "scarcity_radar" in s]
    print(f"MR feed loaded: {feed_loaded} | counts in feed:", (feeds.get('scarcity_radar') or {}).get('counts'))
    print(f"MR index names carrying scarcity_radar: {len(carriers)}")
    for sym,info in carriers[:6]:
        # how many total systems does this name have (rank potential)?
        print(f"   {sym}: tier={info.get('tier')} comp={info.get('score')} vertical={info.get('vertical')} | total_systems_on_name={len(idx[sym])}")
except Exception as e:
    import traceback; print("MR import/verify failed:", str(e)[:120]); traceback.print_exc()
# ---- conviction (correct key) ----
cv=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/conviction.json")["Body"].read())
setups=cv.get("setups") or []
sc=[s for s in setups if "shortage" in str(s.get("subject","")).lower() or "scarcity" in str(s.get("subject","")).lower()]
print("\nCONVICTION setups:",[s.get("subject") for s in setups])
print("scarcity subject:",[(s.get("subject"),s.get("direction"),str(s.get("rationale") or s.get("read"))[:70]) for s in sc])
sn=cv.get("single_names") or []
print("single_names from scarcity-radar:",[(n.get("ticker"),n.get("verdict"),n.get("score")) for n in sn if n.get("source")=="scarcity-radar" or str(n.get("verdict","")).startswith("SHORTAGE")])
# show the engine-level read for Scarcity Radar
eng=cv.get("engines") or cv.get("engine_reads") or []
print("Scarcity Radar engine read:",[ (e.get("engine"),e.get("signal"),e.get("read")) for e in eng if "scarcity" in str(e.get("engine","")).lower()])
print("DONE 2235")
