"""ops 2548 — probe the high-value unused feeds for the deep risk-page expansion."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
CAND = [
    "credit-stress","credit-equity-divergence","liquidity-credit-engine","eva-spread",
    "settlement-fails","failed-pattern-reversal",
    "global-liquidity","liquidity-pulse","liquidity-inflection","repo-lending","funding-plumbing","china-liquidity",
    "yen-carry","crypto-stablecoin-peg","fx-decomposition","boj-detail","snb-detail","ecb-detail",
    "yield-curve","move-index","bond-vol","auction-crisis","auction-grades","auction-tenor-signals","vix-curve",
    "tail-risk","tail-hedge","skew-tail-hedging","vvix-vov-regime",
    "ciss-stress","systemic-stress","bank-stress","cds-monitor","cds-proxy",
    "breadth-thrust","breadth-divergence","institutional-positions","flow-confluence",
    "macro-nowcast","macro-surprise","global-business-cycle",
]
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())
    except Exception as e: return {"_MISSING": str(e)[:40]}
def compact(d):
    if not isinstance(d, dict): return str(d)[:70]
    out=[]
    for k,v in list(d.items())[:16]:
        if isinstance(v,(int,float,str,bool)) or v is None:
            sv=str(v); out.append(f"{k}={sv[:34]+'…' if len(sv)>34 else sv}")
        elif isinstance(v,list): out.append(f"{k}=[{len(v)}]")
        elif isinstance(v,dict): out.append(f"{k}={{{','.join(list(v)[:4])}}}")
    return " · ".join(out)
for k in CAND:
    d=rd(k)
    if "_MISSING" in d: print(f"✗ {k}"); continue
    print(f"\n✓ {k}\n   {compact(d)[:330]}")
    for kk,v in d.items():
        if isinstance(v,list) and v and isinstance(v[0],dict):
            print(f"   {kk}[0]: {compact(v[0])[:180]}"); break
print("\nDONE 2548")
