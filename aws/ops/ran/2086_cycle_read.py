import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def grab(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
KW=("phase","stage","cycle","regime","score","level","label","trend","direction",
    "headline","read","summary","squeeze","liquidity","recession","late","tightening",
    "easing","net_liquidity","posture","state","signal","verdict","status","z","pctile","percentile")
def dump(k, maxlen=120):
    d=grab(k)
    if "_err" in d: print(f"\n=== {k} === MISSING ({d['_err']})"); return
    print(f"\n=== {k} === gen={str(d.get('generated_at') or d.get('updated_at') or d.get('asof') or '')[:16]}")
    print("  top keys:", list(d.keys())[:22])
    def walk(o,pre="",depth=0):
        if depth>1: return
        if isinstance(o,dict):
            for kk,vv in o.items():
                if any(w in kk.lower() for w in KW):
                    if isinstance(vv,(int,float,bool)) or (isinstance(vv,str) and len(vv)<maxlen):
                        print(f"    {pre}{kk} = {vv}")
                    elif isinstance(vv,dict):
                        walk(vv,pre+kk+".",depth+1)
for k in ["data/us-cycle.json","data/global-business-cycle.json","data/global-liquidity.json",
          "data/liquidity-pulse.json","data/liquidity-inflection.json","data/liquidity-flow.json",
          "data/liquidity-credit-engine.json","data/liquidity-capacity.json","data/global-stress.json",
          "data/crisis-composite.json","data/regime.json","data/regime-playbook.json",
          "data/crisis-canaries.json","data/macro-nowcast.json","data/global-macro.json",
          "data/cross-asset-regime.json","data/systemic-stress.json"]:
    dump(k)
print("\nDONE 2086")
