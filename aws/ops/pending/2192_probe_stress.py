import boto3, json
s3=boto3.client("s3","us-east-1")
files=["bank-stress","systemic-stress","credit-equity-divergence","crisis-canaries",
       "cross-asset-confirm","concentration-liquidity","cds-monitor","correlation-breaks",
       "plumbing-stress","firm-stress","liquidity-inflection"]
for f in files:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{f}.json")["Body"].read())
        # headline fields likely to carry a score/signal/regime
        head={k:v for k,v in d.items() if isinstance(v,(int,float,str)) and re.search(r'score|signal|regime|level|state|z|stress|status|label|risk',k,re.I)} if False else {}
        import re as _re
        head={k:v for k,v in d.items() if isinstance(v,(int,float,str,bool)) and _re.search(r'score|signal|regime|level|state|stress|status|label|risk|verdict|z_',k,_re.I)}
        ga=str(d.get("generated_at",""))[:16]
        print(f"{f}: gen {ga} keys={list(d.keys())[:8]}")
        if head: print(f"    headline: {json.dumps(head)[:160]}")
    except Exception as e:
        print(f"{f}: ERR {str(e)[:40]}")
print("DONE 2192")
