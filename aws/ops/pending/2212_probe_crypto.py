import boto3, json, re
s3=boto3.client("s3","us-east-1")
def probe(f):
    for key in (f"data/{f}.json",):
        try:
            d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=key)["Body"].read())
            head={k:v for k,v in d.items() if isinstance(v,(int,float,str,bool)) and re.search(r'score|regime|state|signal|level|risk',k,re.I)}
            print(f"\n{f}: keys={list(d.keys())[:11]}")
            if head: print(f"    headline={json.dumps(head)[:120]}")
            for k,v in d.items():
                if isinstance(v,list) and v and isinstance(v[0],dict):
                    it=v[0]
                    idk=[x for x in it.keys() if any(w in x.lower() for w in ("ticker","symbol","coin","name","asset"))]
                    if idk:
                        cand=[x for x in it.keys() if any(w in x.lower() for w in ("score","signal","state","funding","ret","mom","rank","change","flag","narrative","phase"))]
                        print(f"    '{k}' n={len(v)} id={idk} scoreish={cand[:6]}")
        except Exception as e: print(f"{f}: ERR {str(e)[:40]}")
for f in ["crypto-ma200","crypto-emergence","crypto-funding","crypto-narratives","onchain-ratios",
          "crypto-opportunities","altseason","crypto-cycle-risk","crypto-liquidity","stablecoin-flow"]:
    probe(f)
print("\nDONE 2212")
