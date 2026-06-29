import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-chain-linkage.json")["Body"].read())
e=d.get("entries") or []
print("n_entries:",len(e),"type:",type(e).__name__)
if isinstance(e,list) and e:
    print("entry[0] keys:",sorted(e[0].keys()))
    print("entry[0]:",json.dumps(e[0])[:500])
    # find ones with concentration flags
    flagged=[x for x in e if x.get("concentration_flags") or x.get("severity")]
    print("n_flagged:",len(flagged))
    if flagged: print("flagged sample:",json.dumps(flagged[0])[:400])
elif isinstance(e,dict):
    k=list(e.keys())[:2]; print("dict keys sample:",k,"->",json.dumps(e[k[0]])[:400] if k else "")
print("DONE 2488")
