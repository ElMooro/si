import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
try:
    cfg=lam.get_function(FunctionName="justhodl-crypto-options")["Configuration"]
    print("function exists | LastModified:",cfg["LastModified"],"| timeout:",cfg["Timeout"])
except Exception as e:
    print("function NOT created yet:",str(e)[:80]); print("DONE 2380"); raise SystemExit
r=lam.invoke(FunctionName="justhodl-crypto-options",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:160])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-options.json")["Body"].read())
for ccy in ("btc","eth"):
    c=d.get(ccy) or {}
    if c.get("_error"): print(ccy.upper(),"ERROR:",c["_error"]); continue
    print(f"\n{ccy.upper()}: underlying ${c.get('underlying')} | {c.get('n_options')} opts / {c.get('n_expiries')} expiries")
    print("  positioning:",c.get("positioning"),"| rr_30d:",c.get("rr_30d"),"| term:",c.get("term_regime"),"slope",c.get("term_slope_iv"))
    for lab in ("7d","30d","90d"):
        s=(c.get("surface") or {}).get(lab)
        if s: print(f"  {lab} (dte {s['dte']}): ATM {s['atm_iv']} | RR25 {s['rr_25d']} (C {s['c25_iv']}/P {s['p25_iv']}) | fly {s['butterfly_25d']} | P/C OI {s['put_call_oi']}")
    print("  interp:",(c.get("interpretation") or "")[:130])
print("\nhistory_n:",d.get("history_n"),"| composite regime:",d.get("crypto_options_regime"))
print("DONE 2380")
