import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
def show(name,key,fields):
    print(f"===== {name} ({key}) =====")
    try:
        d=g(key); print(" topkeys:",list(d.keys())[:14] if isinstance(d,dict) else type(d).__name__)
        for f in fields:
            v=d.get(f) if isinstance(d,dict) else None
            if v is not None: print(f"  .{f}:",json.dumps(v)[:280])
    except Exception as e: print(" ERR",str(e)[:70])
show("capital-inflows","data/capital-inflows.json",["headline","regime","by_asset_class"])
show("polygon-fx-regime","data/polygon-fx-regime.json",["fx_roro_score","regime","dollar","carry","pairs","summary","dxy"])
show("gold-equity-rotation","data/gold-equity-rotation.json",["state","signal_strength","current_metrics"])
show("tic-flows","data/tic-flows.json",["headline","summary","foreign_holdings","by_country","net"])
show("credit-stress","data/credit-stress.json",["regime","hy_oas","ig_oas","headline","summary"])
print("===== dark-pool top_distribution =====")
try:
    dp=g("data/dark-pool.json"); td=dp.get("top_distribution") or []
    print(" n:",len(td),"sample:",json.dumps(td[0])[:240] if td else "none")
except Exception as e: print(" ERR",str(e)[:60])
print("DONE 2513")
