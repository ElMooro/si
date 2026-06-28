import urllib.request, json
def post(body, timeout=20):
    req=urllib.request.Request("https://api.hyperliquid.xyz/info", data=json.dumps(body).encode(),
        headers={"Content-Type":"application/json","User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=timeout) as r: return json.loads(r.read())
print("=== metaAndAssetCtxs (OI + funding + premium per perp) ===")
try:
    d=post({"type":"metaAndAssetCtxs"})
    meta,ctxs=d[0],d[1]
    uni=meta.get("universe",[])
    print("universe size:",len(uni))
    idx={u["name"]:i for i,u in enumerate(uni)}
    for sym in ["BTC","ETH","SOL"]:
        if sym in idx:
            c=ctxs[idx[sym]]
            print(f"  {sym}: OI={c.get('openInterest')} funding={c.get('funding')} premium={c.get('premium')} markPx={c.get('markPx')} dayNtlVlm={c.get('dayNtlVlm')} oraclePx={c.get('oraclePx')}")
    # total OI across all (in USD ~ openInterest*markPx)
    tot=0.0
    for i,u in enumerate(uni):
        try: tot+=float(ctxs[i].get("openInterest") or 0)*float(ctxs[i].get("markPx") or 0)
        except: pass
    print("  TOTAL OI across all HL perps (USD):",round(tot))
except Exception as e: print("  err:",str(e)[:120])
print("=== liquidations available? try a few endpoints ===")
for body in [{"type":"liquidations"},{"type":"recentTrades","coin":"BTC"}]:
    try:
        r=post(body); s=json.dumps(r)[:160]; print(f"  {body}: {s}")
    except Exception as e: print(f"  {body}: ERR {str(e)[:80]}")
print("DONE 2431")
