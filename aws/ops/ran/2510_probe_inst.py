import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
for key in ["screener/smart-money-holdings.json","screener/smart-money.json"]:
    print("=====",key,"=====")
    try:
        d=g(key); print("type:",type(d).__name__,"keys:",list(d.keys())[:12] if isinstance(d,dict) else f"LIST[{len(d)}]")
        body=d if isinstance(d,list) else (d.get("holdings") or d.get("stocks") or d.get("positions") or d.get("by_ticker") or d.get("data") or d)
        if isinstance(body,list) and body: print(" sample:",json.dumps(body[0])[:340])
        elif isinstance(body,dict):
            k0=list(body.keys())[:1]; 
            if k0: print(" sample:",json.dumps({k0[0]:body[k0[0]]})[:340])
        if isinstance(d,dict): print(" gen:",d.get("generated_at") or d.get("as_of") or d.get("updated_at"))
    except Exception as e: print(" ERR",str(e)[:80])
print("DONE 2510")
