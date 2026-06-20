"""ops 2034: is rotation-chain alive (alpha coverage)? are supply-chain-linkage edges empty?"""
import json, boto3
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def load(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:100]}
print("="*60);print("rotation-chain.json (the supplier-lag alpha)");print("="*60)
rc=load("data/rotation-chains.json") if "_err" in load("data/rotation-chain.json") else load("data/rotation-chain.json")
for k in ("data/rotation-chain.json","data/rotation-chains.json"):
    d=load(k)
    if "_err" not in d:
        print(f"  {k}: gen={d.get('generated_at') or d.get('as_of')}")
        print("   keys:",list(d.keys())[:12])
        ch=d.get("chains") or d.get("rotations") or d.get("themes") or []
        if ch:
            c0=ch[0] if isinstance(ch,list) else ch
            print("   sample chain:",json.dumps(c0)[:400])
        for nk in ("next_up","laggards","ready_to_rotate","opportunities","actionable"):
            if d.get(nk): print(f"   {nk}:",json.dumps(d[nk])[:300]); break
        break
    else: print(f"  {k}: {d['_err']}")
print("\n"+"="*60);print("supply-chain-linkage entries — suppliers/customers empty?");print("="*60)
d=load("data/supply-chain-linkage.json")
ents=d.get("entries") or []
print("  universe_size:",d.get("universe_size"),"entries:",len(ents),"systemic_hubs:",d.get("n_systemic_hubs"))
nonempty_sup=nonempty_cust=0
for e in ents[:400]:
    if e.get("suppliers"): nonempty_sup+=1
    if e.get("customers"): nonempty_cust+=1
print("  entries w/ suppliers:",nonempty_sup,"| w/ customers:",nonempty_cust,"(of",len(ents),"checked)")
if ents:
    print("  sample entry:",json.dumps(ents[0])[:400])
print("DONE 2034")
