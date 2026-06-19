import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
fx=json.loads(s3.get_object(Bucket=B,Key="data/polygon-fx-regime.json")["Body"].read())
print("FX regime_signals:",json.dumps(fx.get("regime_signals")))
print("FX regime_metrics:",json.dumps(fx.get("regime_metrics"))[:600])
pd=fx.get("pair_data") or {}
print("FX pair_data keys:",list(pd.keys()))
for k in list(pd.keys())[:3]:
    print("  %s: %s"%(k, json.dumps(pd[k])[:240]))
fut=json.loads(s3.get_object(Bucket=B,Key="data/polygon-futures-curves.json")["Body"].read())
print("\nFUT signals:",fut.get("signals"))
prod=fut.get("product_data") or {}
print("FUT product_data keys:",list(prod.keys()))
for k in list(prod.keys())[:4]:
    print("  %s: %s"%(k, json.dumps(prod[k])[:240]))
