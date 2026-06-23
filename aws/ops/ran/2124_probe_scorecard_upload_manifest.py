import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def peek(k,label):
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        print(f"\n[{label}] {k} top-keys:", list(d.keys())[:16] if isinstance(d,dict) else f"LIST n={len(d)}")
        return d
    except Exception as e:
        print(f"\n[{label}] {k}: MISSING ({str(e)[:40]})"); return None

ea=peek("data/engine-alpha.json","engine-alpha")
if ea:
    print("  proven_signals:", ea.get("alpha_proven_signals"))
    print("  other alpha keys:", [k for k in ea.keys() if 'alpha' in k.lower() or 'proven' in k.lower() or 'negative' in k.lower()])
    # find a per-engine list with forward stats
    for key in ("engines","per_engine","scored","results","all"):
        v=ea.get(key)
        if isinstance(v,list) and v:
            print(f"  sample row [{key}]:", json.dumps(v[0],default=str)[:380]); break

for cand in ["data/signal-scorecard.json","data/scorecard.json","data/engine-scorecard.json","data/signal-outcomes.json"]:
    d=peek(cand,"scorecard?")
    if d:
        for key in ("engines","per_engine","scored","results","scorecard","rows"):
            v=d.get(key) if isinstance(d,dict) else None
            if isinstance(v,list) and v:
                print(f"  scorecard sample row [{key}]:", json.dumps(v[0],default=str)[:420]); break
        break

# existing ai-commentary coverage in S3
ks=[o["Key"] for o in s3.get_paginator("list_objects_v2").paginate(Bucket=B,Prefix="data/ai-commentary/").search("Contents") if o]
top=[k for k in ks if "/history/" not in k]
print("\nexisting ai-commentary pages in S3:", len(top), "->", [k.split("/")[-1].replace(".json","") for k in top][:20])

# upload the page manifest for the engine
man=json.load(open("aws/page-ai/page-manifest.json"))
s3.put_object(Bucket=B,Key="data/page-ai-manifest.json",Body=json.dumps(man).encode(),ContentType="application/json")
print("\nuploaded data/page-ai-manifest.json:",len(man),"pages")
print("DONE 2124")
