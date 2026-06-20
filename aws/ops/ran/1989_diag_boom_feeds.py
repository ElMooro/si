import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for k in ["data/squeeze-pretrigger.json","data/52wk-quality-breakout.json"]:
    print("="*60); print(k)
    try:
        j=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        if isinstance(j,dict):
            print("  top-level keys:",list(j.keys())[:25])
            for kk,vv in j.items():
                if isinstance(vv,list) and vv:
                    samp=vv[0]
                    print(f"    [{kk}] len={len(vv)} sample={json.dumps(samp)[:160] if isinstance(samp,(dict,list)) else samp}")
        elif isinstance(j,list):
            print("  LIST len",len(j),"sample:",json.dumps(j[0])[:200] if j else "empty")
    except Exception as e: print("  ERR",type(e).__name__,e)
# overlap diagnostics across loaded dims
print("="*60); print("DIMENSION TICKER OVERLAP")
def tset(key,lks):
    try: j=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
    except Exception: return set()
    out=set()
    for lk in lks:
        for it in (j.get(lk) or []):
            t=it.get("ticker") if isinstance(it,dict) else (it if isinstance(it,str) else None)
            if t: out.add(t.upper())
    return out
A=tset("data/analyst-actions.json",["most_bullish","guidance_raises","pt_raises"])
E=tset("data/estimate-revisions.json",["estimate_strength_leaders","upward_revisions"])
F=tset("data/flow-lookthrough.json",["actual_accumulation","top_picks"])
print(f"  ANALYST={len(A)} ESTIMATE={len(E)} FLOW={len(F)}")
print(f"  A∩E={len(A&E)} A∩F={len(A&F)} E∩F={len(E&F)} A∩E∩F={len(A&E&F)}")
print("  A∩F sample:",list(A&F)[:10])
print("  E∩F sample:",list(E&F)[:10])
print("DONE 1989")
