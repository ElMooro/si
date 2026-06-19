import boto3, json, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
def load(key):
    try:
        h=s3.head_object(Bucket=B,Key=key); age=(now-h["LastModified"]).total_seconds()/3600
        d=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
        return d, age
    except Exception as e:
        return {"_err":str(e)[:50]}, None
def listkey(d):
    # find the main per-ticker list
    for k,v in d.items():
        if isinstance(v,list) and v and isinstance(v[0],dict) and any(t in v[0] for t in ("symbol","ticker")):
            return k,v
    return None,None
for key in ["data/estimate-revisions.json","data/eps-revision-velocity.json","data/stock-valuations.json",
            "data/fundamentals.json","data/finra-short.json","data/analyst-consensus.json","data/forward-orders.json"]:
    d,age=load(key)
    print("\n===== %s  (%s) ====="%(key, "%.0fh%s"%(age," STALE" if age and age>30 else "") if age is not None else "MISSING"))
    if "_err" in d: print("  ",d["_err"]); continue
    print("  top keys:",list(d.keys())[:14])
    lk,lst=listkey(d)
    if lk:
        print("  main list '%s' len=%d  sample fields:"%(lk,len(lst)))
        e=lst[0]; print("   ",{k:(round(v,3) if isinstance(v,float) else v) for k,v in list(e.items())[:18]})
    else:
        # maybe dict keyed by ticker
        for k,v in d.items():
            if isinstance(v,dict) and len(v)>20:
                sk=list(v.keys())[0]; print("  dict '%s' keyed by ticker? sample[%s]=%s"%(k,sk,json.dumps(v[sk],default=str)[:160])); break
