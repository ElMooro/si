import boto3, json
s3=boto3.client("s3","us-east-1")
def peek(key, fields=None, depth_keys=True):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
    except Exception as e:
        print(f"\n### {key}: MISSING/ERR {str(e)[:50]}"); return
    print(f"\n### {key}  (gen {str(d.get('generated_at') or d.get('as_of') or '?')[:16]})")
    if depth_keys: print("  top keys:", list(d.keys())[:18])
    for f in (fields or []):
        cur=d
        for p in f.split('.'):
            cur=cur.get(p) if isinstance(cur,dict) else None
            if cur is None: break
        if isinstance(cur,(dict,list)): cur=(list(cur.keys())[:8] if isinstance(cur,dict) else cur[:4])
        print(f"   {f} = {cur}")
peek("data/risk-regime.json", ["risk_regime_score","posture","regime","components","drivers","pillars"])
peek("data/yield-curve.json", ["spreads.2s10s","2s10s","3m10y","recession_prob","inversion","signal","curve"])
peek("data/credit-stress.json", ["hy_oas","ig_oas","level","score","spreads","signal"])
peek("data/sovereign-fiscal.json", ["tga","issuance","debt_to_penny","avg_interest","net_issuance","interpretation"])
peek("data/settlement-fails.json", ["total_fails","percentile","level","fails","interpretation"])
peek("data/fomc-reaction.json", ["stance","tone","statement_tone","reaction_map","latest"])
peek("data/historical-analogs.json", ["analogs","top_analog","nearest","matches"])
peek("data/regime-playbook.json", ["current_fingerprint","playbook","tilts","asset_classes","current"])
peek("data/regime-composite.json", ["regime","score","composite","label"])
peek("data/sector-rotation.json", ["leaders","laggards","signal","cyclical_vs_defensive","phase"])
peek("data/macro-surprise.json", ["surprise_index","score","trend","level"])
peek("data/fed-liquidity.json", ["net_liquidity","walcl","rrp","tga","reserves","components","change_13w"])
peek("data/dollar-radar.json", ["dxy","signal","level","trend"])
peek("data/vix-curve.json", ["term_structure","contango","backwardation","vix","signal"])
peek("data/signal-scorecard.json", ["proven","engines","summary"])
print("\nDONE 2314")
