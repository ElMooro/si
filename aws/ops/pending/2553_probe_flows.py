"""ops 2553 — probe flow engines to wire the 3 directional flows the user named:
Treasury<->equity rotation, overseas/offshore dollar flow, FX currency flows."""
import boto3, json
s3 = boto3.client("s3", "us-east-1")
B = "justhodl-dashboard-live"
CAND = ["cross-asset-flow-state","gold-equity-rotation","money-flow-state","sector-flow-state",
        "liquidity-flow","rotation-radar","rotation-chain","capital-flow-radar","capital-flow",
        "correlation-breaks","correlation-surface","cot-tracker","cot-extremes-scanner",
        "carry-surface","etf-true-flows","etf-fund-flows"]
def rd(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=f"data/{k}.json")["Body"].read())
    except Exception as e: return {"_MISSING": str(e)[:40]}
def compact(d, n=18):
    if not isinstance(d, dict): return str(d)[:80]
    out=[]
    for k,v in list(d.items())[:n]:
        if isinstance(v,(int,float,str,bool)) or v is None:
            sv=str(v); out.append(f"{k}={sv[:40]+'…' if len(sv)>40 else sv}")
        elif isinstance(v,list): out.append(f"{k}=[{len(v)}]")
        elif isinstance(v,dict): out.append(f"{k}={{{','.join(list(v)[:5])}}}")
    return " · ".join(out)
for k in CAND:
    d=rd(k)
    if "_MISSING" in d: print(f"✗ {k}"); continue
    print(f"\n✓ {k}\n   {compact(d)[:360]}")
    # show first list-of-dicts element (the flow rows)
    for kk,v in d.items():
        if isinstance(v,list) and v and isinstance(v[0],dict):
            print(f"   {kk}[0]: {compact(v[0],10)[:200]}"); break
    # show nested dicts that look like rotation/flow
    for kk in ["asset_class_rotation","rotation","by_asset_class","flows","pairs","currencies",
               "fx","stock_bond","equity_bond","positioning","by_currency","legs","custody"]:
        if isinstance(d.get(kk),(dict,list)):
            v=d[kk]
            if isinstance(v,list) and v and isinstance(v[0],dict):
                print(f"   .{kk}[0]: {compact(v[0],8)[:170]}")
            elif isinstance(v,dict):
                print(f"   .{kk}: {compact(v,8)[:200]}")
print("\nDONE 2553")
