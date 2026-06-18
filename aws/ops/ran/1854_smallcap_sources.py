import boto3, json, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
keys=["data/microcap-float-squeeze.json","data/finra-short.json","data/short-interest.json",
      "data/volatility-squeeze.json","data/revenue-acceleration.json","data/pre-pump-signals.json"]
def sh(v):
    if isinstance(v,dict): return "{%s}"%", ".join(list(v.keys())[:14])
    if isinstance(v,list): return "[%d] e0=%s"%(len(v), sh(v[0]) if v else "")
    return type(v).__name__
for k in keys:
    try:
        o=s3.get_object(Bucket=B,Key=k); age=(now-o["LastModified"]).total_seconds()/86400.0
        j=json.loads(o["Body"].read())
        print("\n## %s (%.1fd)"%(k,age))
        if isinstance(j,dict):
            for kk,vv in list(j.items())[:16]: print("  %-22s %s"%(kk,sh(vv)[:120]))
            # find the main list of names and show one element fully
            for kk,vv in j.items():
                if isinstance(vv,list) and vv and isinstance(vv[0],dict) and any(t in str(vv[0].keys()).lower() for t in ("ticker","symbol")):
                    print("  >>> sample %s[0]: %s"%(kk, json.dumps(vv[0],default=str)[:300])); break
        else: print("  ROOT",sh(j)[:150])
    except Exception as e: print("\n## %s ERR %s"%(k,str(e)[:60]))
