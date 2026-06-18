import urllib.request, json, time, boto3
B="justhodl-dashboard-live"; s3=boto3.client("s3","us-east-1")
def get(u,t=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=t);return r.getcode(),r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,"code",0),str(e)[:60]
for i in range(8):
    c,b=get("https://justhodl.ai/sector-flow.html")
    if c==200 and "Category Rotation" in b: print("OK sector-flow live w/ category rotation section"); break
    print("  attempt %d code=%s"%(i+1,c)); time.sleep(20)
else: print("PENDING code=%s"%c)
# confirm rotation_alerts dict shape so the defensive render is right
sr=json.loads(s3.get_object(Bucket=B,Key="data/sector-rotation.json")["Body"].read())
ra=sr.get("rotation_alerts")
print("rotation_alerts keys:", list(ra.keys()) if isinstance(ra,dict) else type(ra).__name__)
if isinstance(ra,dict):
    for k,v in ra.items():
        print("  %s: %s%s"%(k, type(v).__name__, (" len=%d e0=%s"%(len(v), json.dumps(v[0],default=str)[:120])) if isinstance(v,list) and v else (" = %s"%json.dumps(v,default=str)[:80])))
