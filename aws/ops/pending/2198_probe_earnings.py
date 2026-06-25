import boto3, json
s3=boto3.client("s3","us-east-1")
def probe(f):
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        ga=str(d.get("generated_at",""))[:10]
        print(f"\n{f} (gen {ga}): keys={[k for k in d.keys()][:10]}")
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0] or "symbol" in v[0]):
                ik=list(v[0].keys())
                # candidate score/direction keys
                cand=[x for x in ik if any(w in x.lower() for w in ("score","surprise","direction","signal","drift","tone","sentiment","z","beat","crush","pead","strength","rank"))]
                print(f"    '{k}' n={len(v)} keys={ik[:9]} | scoreish={cand[:6]}")
    except Exception as e: print(f"{f}: ERR {str(e)[:45]}")
for f in ["earnings-whisper","earnings-tone-velocity","earnings-iv-crush","earnings-nlp",
          "earnings-sentiment","pead-detector","post-earnings-mean-rev","pump-earnings-nlp",
          "sector-earnings-diffusion","earnings-cascade","earnings-quality","earnings-tracker"]:
    probe(f)
print("\nDONE 2198")
