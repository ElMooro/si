import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
def show(name,key,fields):
    print(f"===== {name} ({key}) =====")
    try:
        d=g(key); print(" topkeys:",list(d.keys())[:16] if isinstance(d,dict) else type(d).__name__)
        for f in fields:
            v=d.get(f) if isinstance(d,dict) else None
            if v is not None: print(f"  .{f}:",json.dumps(v)[:300])
    except Exception as e: print(" ERR",str(e)[:70])
show("polygon-fx-regime","data/polygon-fx-regime.json",["fx_roro","regime_signals","regime_metrics"])
show("dollar-radar","data/dollar-radar.json",["headline","regime","dxy","signal","summary","state","score"])
show("risk-regime","data/risk-regime.json",["risk_regime_score","posture","regime","components","headline"])
show("cross-asset-regime","data/cross-asset-regime.json",["regime","state","headline","summary","assets","signals"])
# etf category rotation labels (cross-asset categories)
try:
    etf=g("data/etf-true-flows.json"); cr=etf.get("category_rotation") or []
    print("etf category_rotation labels:",[c.get("category") for c in cr][:20])
except Exception as e: print("etf ERR",str(e)[:50])
print("DONE 2514")
