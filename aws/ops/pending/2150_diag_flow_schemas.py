import boto3, json
s3=boto3.client("s3","us-east-1")
def g(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:45]}
def shape(name,k):
    d=g(k)
    if "_err" in d: print(f"{name}: MISSING {d['_err']}"); return
    print(f"\n{name} [{k}] keys:",list(d.keys())[:13] if isinstance(d,dict) else type(d).__name__)
    if isinstance(d,dict):
        for key,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                print(f"   list '{key}' n={len(v)} item-keys={list(v[0].keys())[:11]}")
            elif isinstance(v,dict) and v:
                fk=list(v.keys())[0]
                if isinstance(v.get(fk),dict) and isinstance(fk,str) and fk.isupper() and len(fk)<=6:
                    print(f"   MAP '{key}' n={len(v)} val-keys={list(v[fk].keys())[:9]}")
for n,k in [("dark-pool","data/dark-pool.json"),("smart-money-clusters","data/smart-money-clusters.json"),
            ("13f-positions","data/13f-positions.json"),("institutional-positions","data/institutional-positions.json"),
            ("finra-short","data/finra-short.json"),("short-interest","data/short-interest.json"),
            ("capital-flow","data/capital-flow.json"),("flow-lookthrough","data/flow-lookthrough.json"),
            ("stealth-accumulation","data/stealth-accumulation.json"),("flow-anomaly","data/flow-anomaly.json"),
            ("13f-aggregate","data/13f-aggregate.json")]:
    shape(n,k)
print("\nDONE 2150")
