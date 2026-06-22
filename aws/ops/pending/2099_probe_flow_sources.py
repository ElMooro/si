import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
def probe(k):
    d=g(k)
    if "_err" in d: print(f"\n[{k}] {d['_err']}"); return None
    print(f"\n[{k}] v={d.get('version')} gen={str(d.get('generated_at'))[:16]} keys={list(d.keys())[:20]}")
    for ak in ("top_picks","accumulation","distribution","names","items","results","signals","by_ticker","tickers","quiet_accumulation"):
        v=d.get(ak)
        if isinstance(v,list) and v:
            print(f"   .{ak}[0] keys: {list(v[0].keys()) if isinstance(v[0],dict) else type(v[0]).__name__}")
            print(f"   .{ak}[0]: {json.dumps(v[0],default=str)[:320]}")
        elif isinstance(v,dict) and v:
            kk=list(v.keys())[:3]
            print(f"   .{ak} (dict, {len(v)} keys) e.g. {kk[0]}: {json.dumps(v[kk[0]],default=str)[:240]}")
    return d
dp=probe("data/dark-pool.json")
# what tickers does dark-pool cover, and what's the accumulation field?
if dp and "_err" not in dp:
    for k,v in dp.items():
        if isinstance(v,list) and v and isinstance(v[0],dict) and "ticker" in v[0]:
            sigs=set()
            for it in v[:200]:
                for fk in ("signal","label","state","direction","flow"):
                    if fk in it: sigs.add(f"{fk}={it[fk]}")
            print(f"   list '{k}' n={len(v)} sample signal-fields: {list(sigs)[:8]}")
probe("data/signal-harvester.json")
print("DONE 2099")
