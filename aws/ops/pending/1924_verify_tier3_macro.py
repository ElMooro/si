import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}

# fast sync
for fn in ["justhodl-massive-signals","justhodl-eurodollar-stress","justhodl-crisis-composite"]:
    try:
        r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse"); print(fn,"->",r["Payload"].read().decode()[:110])
    except Exception as e: print(fn,"ERR",str(e)[:90])
# slow async (LLM / multi-asset)
for fn in ["justhodl-eurodollar-plumbing","justhodl-carry-surface"]:
    try: lam.invoke(FunctionName=fn,InvocationType="Event"); print(fn,"-> async dispatched")
    except Exception as e: print(fn,"ERR",str(e)[:90])
print("waiting 75s for async..."); time.sleep(75)

print("\n--- massive-signals: FX now populated? ---")
d=rd("data/massive-signals.json"); print("  market.fx_signals:",(d.get("market") or {}).get("fx_signals"),"| futures:",(d.get("market") or {}).get("futures_signals"))

print("\n--- eurodollar-stress: real-time FX in composite? ---")
d=rd("data/eurodollar-stress.json"); sig=[s for s in d.get("signals",[]) if s.get("id")=="usd_momentum_rt"]
print("  composite_score:",d.get("composite_score"),"severity:",d.get("severity"),"n_signals_used:",d.get("n_signals_used"))
print("  usd_momentum_rt signal:",json.dumps(sig[0]) if sig else "MISSING")

print("\n--- eurodollar-plumbing: massive_fx + AI verdict? ---")
d=rd("data/eurodollar-plumbing.json"); mfx=d.get("massive_fx") or {}
print("  plumbing_health:",d.get("plumbing_health"),"verdict:",d.get("verdict"))
print("  massive_fx:",json.dumps(mfx)[:240])
ai=d.get("ai") or {}; print("  ai.short_term:",(ai.get("short_term") or ai.get("error") or "")[:140])

print("\n--- crisis-composite: massive_cross_asset? ---")
d=rd("data/crisis-composite.json"); print("  defcon:",d.get("defcon_level"),d.get("defcon_name"),"score:",d.get("master_crisis_score"))
print("  massive_cross_asset:",json.dumps(d.get("massive_cross_asset") or {})[:240])

print("\n--- carry-surface: massive_fx majors? ---")
d=rd("data/carry-surface.json"); mfx=d.get("massive_fx") or {}
print("  n_assets:",d.get("n_assets"),"| massive_fx.regime_signals:",mfx.get("regime_signals"),"| usd20d:",mfx.get("usd_synthetic_20d_pct"))
print("  majors:",json.dumps(mfx.get("majors") or {})[:200])
