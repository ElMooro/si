import boto3, json
s3=boto3.client("s3","us-east-1")
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_ERR":str(e)[:40]}
WIRE=["fed-speak","fed-nlp","move-index","bond-vol","vol-regime","vvix-vov-regime","skew-tail-hedging",
 "aaii-sentiment","retail-sentiment","pump-positioning",
 "breadth-divergence","breadth-thrust","credit-equity-divergence","concentration-liquidity","gold-equity-rotation",
 "china-liquidity","liquidity-inflection","liquidity-pulse","cross-asset-regime","yen-carry","tic-flows",
 "labor-leading","activity-nowcast","consumer-pulse","bank-stress","stock-valuations","commodity-curves","seasonality","regime-anomaly"]
HEAD=["regime","regime_read","signal","verdict","state","label","score","value","headline","read","status",
 "composite","composite_score","zscore","z","percentile","pctile","extreme","bull_bear","bull_bear_spread",
 "net_pct","reading","level","summary","call","direction","stance","tilt","bias","risk","trigger","flag","spread_bps","spread","value_pct","pct"]
for n in WIRE:
    d=gj(f"data/{n}.json")
    if "_ERR" in d: print(f"\n■ {n}: {d['_ERR']}"); continue
    keys=[k for k in d.keys() if k not in ("generated_at","version","duration_s","schema_version","method","elapsed_s","as_of","as_of_date","data_sources","generated")]
    print(f"\n■ {n}  keys={keys[:16]}")
    shown=0
    for hk in HEAD:
        if hk in d and shown<5:
            v=d[hk]; vs=json.dumps(v)[:110]
            print(f"    {hk}: {vs}"); shown+=1
print("\n=== resolve MISSING (alt filenames) ===")
for alt in ["fedwatch","rate-probability","fed-rate-probability","fedwatch-probability","rate-cut-odds",
            "cot","cot-extremes","cftc","cftc-positioning","cot-tracker","positioning","cot-deep-view","cftc-cot"]:
    d=gj(f"data/{alt}.json")
    print(f"  data/{alt}.json:", "MISSING" if "_ERR" in d else f"EXISTS keys={list(d.keys())[:10]}")
print("DONE 2329")
