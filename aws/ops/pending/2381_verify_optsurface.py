import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
# confirm function exists
try:
    cfg=lam.get_function_configuration(FunctionName="justhodl-crypto-options-surface")
    print("function created OK | timeout",cfg["Timeout"],"| LastModified",cfg["LastModified"])
except Exception as e:
    print("function MISSING:",str(e)[:100]); print("DONE 2381"); raise SystemExit
r=lam.invoke(FunctionName="justhodl-crypto-options-surface",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"),"| resp:",r["Payload"].read().decode()[:160])
time.sleep(3)
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-options-surface.json")["Body"].read())
for c in ("btc","eth"):
    s=d.get(c) or {}
    h=s.get("headline_30d") or {}; t=s.get("term_structure") or {}
    print(f"\n{c.upper()}: underlying ${s.get('underlying')} | strikes {s.get('n_strikes')}")
    print(f"  30d headline: ATM {h.get('atm_iv')} | 25dC {h.get('iv_25d_call')} 25dP {h.get('iv_25d_put')} | RR25 {h.get('rr_25d')} | BF25 {h.get('bf_25d')}")
    print(f"  read: {s.get('interpretation')}")
    print(f"  term: 7d {t.get('atm_7d')} / 30d {t.get('atm_30d')} / 90d {t.get('atm_90d')} | slope {t.get('slope_90_7')} -> {t.get('regime')}")
print("\nhistory_n:",d.get("history_n"),"| _diag:",d.get("_diag"))
# show the full expiry ladder for BTC
print("BTC expiry ladder (days: ATM | RR25):")
for r2 in (d.get("btc") or {}).get("expiries",[])[:10]:
    print(f"   {r2['days']:>6}d  ATM {r2['atm_iv']}  RR25 {r2['rr_25d']}")
print("DONE 2381")
