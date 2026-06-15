"""Audit confirmation-signal feeds: structure + coverage for bottleneck tickers."""
import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
TEST={"MU","VST","DELL","CEG","NRG","AVGO","ARM","HPE"}
def load(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:80]}
def keyset(d):
    # try to find a ticker->record mapping or list of {ticker/symbol}
    if isinstance(d,dict):
        for kk in ("by_ticker","tickers","positions","data","stocks","items","rows","names","chains"):
            if kk in d and isinstance(d[kk],dict): return ("dict:"+kk, set(d[kk].keys()))
            if kk in d and isinstance(d[kk],list):
                ts={(x.get("ticker") or x.get("symbol")) for x in d[kk] if isinstance(x,dict)}
                return ("list:"+kk, {t for t in ts if t})
        # maybe top-level is the map
        if all(isinstance(v,(dict,list,int,float)) for v in list(d.values())[:5]) and len(d)>20:
            return ("toplevel-map", set(d.keys()))
    if isinstance(d,list):
        ts={(x.get("ticker") or x.get("symbol")) for x in d if isinstance(x,dict)}
        return ("list-root", {t for t in ts if t})
    return ("?", set())
for k in ("data/short-interest.json","data/insider-buys-enriched.json","data/insider-radar.json",
          "data/13f-positions.json","data/rotation-chains.json","data/estimate-revisions-latest.json"):
    d=load(k)
    if "_err" in d: print(f"{k}: ERR {d['_err']}"); continue
    shape,keys=keyset(d)
    cov=TEST & {str(x).upper() for x in keys}
    print(f"\n{k}\n  top-keys: {list(d.keys())[:8] if isinstance(d,dict) else 'LIST len '+str(len(d))}")
    print(f"  shape={shape} n_keys={len(keys)} | bottleneck coverage: {sorted(cov)}")
    # show a sample record for a covered ticker
    if cov and isinstance(d,dict):
        for kk in ("by_ticker","tickers","positions","data","stocks","items","rows","chains"):
            if isinstance(d.get(kk),dict):
                t=next(iter(cov)); rec=d[kk].get(t) or d[kk].get(t.lower())
                if rec: print(f"  sample {t}: {json.dumps(rec)[:240]}"); break
        else:
            t=next(iter(cov)); 
            if t in d: print(f"  sample {t}: {json.dumps(d[t])[:240]}")
