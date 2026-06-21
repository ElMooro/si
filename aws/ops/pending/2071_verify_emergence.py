import urllib.request, time, json, boto3
def get(u):
    for _ in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=20) as r:
                return r.getcode(), r.read().decode("utf-8","replace")
        except Exception: time.sleep(15)
    return None,""
ts=str(int(time.time()))
for pg,marker in [("sector-emergence.html","sector-emergence.json"),("crypto-emergence.html","crypto-emergence.json"),("crypto-risk.html","crypto-emergence.html")]:
    c,b=get(f"https://justhodl.ai/{pg}?t={ts}")
    print(f"{pg}: {c} | has '{marker}': {marker in b}")
c,b=get(f"https://justhodl.ai/directory.html?t={ts}")
print("directory: sector-emergence",'/sector-emergence.html' in b,"| crypto-emergence",'/crypto-emergence.html' in b,"| 296",'all 296 pages' in b)
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
se=json.loads(s3.get_object(Bucket=B,Key="data/sector-emergence.json")["Body"].read())
ce=json.loads(s3.get_object(Bucket=B,Key="data/crypto-emergence.json")["Body"].read())
print("feeds: sector-emergence ok",se.get("ok"),"emerging",se.get("emerging_now"),"| crypto-emergence",ce.get("complex_stage"))
print("DONE 2071")
