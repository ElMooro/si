import urllib.request, json, time
def get(u,t=15):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=t)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except Exception as e:
        return getattr(e,"code",0), str(e)[:80]
# 1) confirm the live JSON carries the highlight fields
import boto3
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
sm=d["summary"]; g=sm.get("green_highlights",[])
print("JSON: deals=%s green=%s yellow=%s  has_highlight_field=%s has_vs_mc=%s"%(
    sm["n_deals"],sm.get("n_green"),sm.get("n_yellow"),
    "highlight" in (d["deals"][0] if d["deals"] else {}), "vs_market_cap_pct" in (d["deals"][0] if d["deals"] else {})))
print("GREEN:",[(x["symbol"],x.get("vs_market_cap_pct"),x.get("materiality_pct")) for x in g])
# 2) confirm the page is live on justhodl.ai (Pages deploy may lag — retry)
url="https://justhodl.ai/deal-scanner.html"
for i in range(9):
    code,body=get(url)
    if code==200 and "Big Orders" in body:
        print("PAGE LIVE: %s (%d) bytes=%d  contains green-tier logic=%s"%(url,code,len(body),"vs Market Cap" in body))
        break
    print("  attempt %d: code=%s"%(i+1,code)); time.sleep(20)
else:
    print("PAGE not confirmed live yet (Pages deploy lag) — code=%s"%code)
