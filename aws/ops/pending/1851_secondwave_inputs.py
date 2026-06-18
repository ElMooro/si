import boto3, json, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
keys=["data/theme-rotation.json","data/theme-cascade.json","data/themes.json",
      "data/beta-laggards.json","data/sympathetic-momentum.json",
      "data/stealth-accumulation.json","data/stock-exposure-lookup.json",
      "data/supply-chain-linkage.json","data/universe.json","data/options-flow.json",
      "data/short-pressure.json","data/dix.json","data/velocity-acceleration.json"]
def shape(v):
    if isinstance(v,dict): return "{%s}"%", ".join(list(v.keys())[:14])
    if isinstance(v,list): return "[%d] e0=%s"%(len(v), shape(v[0]) if v else "")
    return repr(v)[:40] if not isinstance(v,(int,float)) else type(v).__name__
for k in keys:
    try:
        o=s3.get_object(Bucket=B,Key=k); age=(now-o["LastModified"]).total_seconds()/86400.0
        j=json.loads(o["Body"].read())
        print("\n## %s  (%.1fd old, %dB)"%(k,age,o["ContentLength"]))
        if isinstance(j,dict):
            for kk,vv in list(j.items())[:16]:
                print("   %-24s %s"%(kk, shape(vv)[:140]))
        else:
            print("   ROOT %s"%shape(j)[:170])
    except Exception as e:
        print("\n## %s  ERR %s"%(k,str(e)[:70]))
