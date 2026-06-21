import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for k in ("data/crypto-cycle-risk.json","data/crypto-funding.json"):
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
        print(f"=== {k} ===")
        def walk(o,pre="",depth=0):
            if depth>2: return
            if isinstance(o,dict):
                for kk,vv in list(o.items())[:25]:
                    if isinstance(vv,(int,float)) or (isinstance(vv,str) and len(vv)<40):
                        print(f"  {pre}{kk} = {vv}")
                    elif isinstance(vv,dict):
                        print(f"  {pre}{kk}: (dict)"); walk(vv,pre+kk+".",depth+1)
                    elif isinstance(vv,list) and vv and isinstance(vv[0],dict):
                        print(f"  {pre}{kk}: [list of {len(vv)}] sample keys: {list(vv[0].keys())[:8]}")
                    elif isinstance(vv,list):
                        print(f"  {pre}{kk}: [list {len(vv)}]")
        walk(d)
    except Exception as e:
        print(f"=== {k} === ERR {e}")
print("DONE 2072")
