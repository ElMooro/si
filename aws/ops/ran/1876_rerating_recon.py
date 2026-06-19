import boto3, json, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
def peek(key):
    try:
        h=s3.head_object(Bucket=B,Key=key); age=(now-h["LastModified"]).total_seconds()/3600
        d=json.loads(s3.get_object(Bucket=B,Key=key)["Body"].read())
        return d, age
    except Exception as e: return {"_err":str(e)[:40]}, None
# the ingredients the Re-Rating Radar needs to fuse
for label,key,listkeys,fields in [
 ("estimate-revisions","data/estimate-revisions.json",["upgrades","all_qualifying","revisions","top","up","accelerating"],["symbol","revision","eps_revision","rev_revision","trend","direction","n_up","n_down"]),
 ("revenue-acceleration","data/revenue-acceleration.json",["all_qualifying","top","accelerating"],["symbol","rev_growth","accel","yoy"]),
 ("forward-orders","data/forward-orders.json",["all_qualifying","top","names"],["symbol","rpo","backlog","growth"]),
 ("valuations","data/valuations.json",["all_qualifying","names","screen","cheap","ncav"],["symbol","pe","ev_sales","peg","fwd_pe","growth"]),
 ("fundamentals-xray","data/fundamentals-engine.json",["names","all","companies"],["symbol","dcf_gap","peg","fwd_pe","growth"]),
 ("ai-infra-stack","data/ai-infra-stack.json",["stack"],["symbol"]),
]:
    d,age=peek(key)
    if "_err" in d: print("\n%-22s MISSING (%s)"%(label,d["_err"])); continue
    print("\n%-22s %.0fh  top-keys=%s"%(label,age,list(d.keys())[:10]))
    # find a list of records and dump field names of first record
    found=None
    for lk in listkeys:
        v=d.get(lk)
        if isinstance(v,list) and v and isinstance(v[0],dict): found=(lk,v); break
        if isinstance(v,dict):
            for lk2 in listkeys:
                vv=v.get(lk2)
                if isinstance(vv,list) and vv and isinstance(vv[0],dict): found=(lk+"."+lk2,vv); break
            if found: break
    summ=d.get("summary")
    if isinstance(summ,dict) and not found:
        for lk in listkeys:
            vv=summ.get(lk)
            if isinstance(vv,list) and vv and isinstance(vv[0],dict): found=("summary."+lk,vv); break
    if found:
        lk,v=found
        print("   [%s] n=%d record_fields=%s"%(lk,len(v),list(v[0].keys())[:14]))
        print("   sample:",json.dumps(v[0],default=str)[:240])
    else:
        print("   (no obvious record list among %s)"%listkeys)
