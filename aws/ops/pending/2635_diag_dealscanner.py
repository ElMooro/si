"""ops 2635 — diagnose deal-scanner: what do FMP /stable/news endpoints actually return + why 0 deals."""
import urllib.request, json, boto3
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def g(url):
    try:
        return json.loads(urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=25).read())
    except Exception as e:
        return {"__err__":str(e)[:160]}

print("="*60)
print("1) FMP /stable/news/press-releases-latest")
d=g(f"https://financialmodelingprep.com/stable/news/press-releases-latest?page=0&limit=5&apikey={FMP}")
if isinstance(d,list):
    print("  returned",len(d),"items. First item keys:",list(d[0].keys()) if d else "EMPTY")
    if d: print("  sample:",json.dumps(d[0])[:400])
else: print("  NOT A LIST:",str(d)[:200])

print("\n2) FMP /stable/news/stock-latest")
d=g(f"https://financialmodelingprep.com/stable/news/stock-latest?page=0&limit=5&apikey={FMP}")
if isinstance(d,list):
    print("  returned",len(d),"items. First item keys:",list(d[0].keys()) if d else "EMPTY")
    if d: print("  sample:",json.dumps(d[0])[:400])
else: print("  NOT A LIST:",str(d)[:200])

# try alternate known /stable/ news endpoint names
print("\n3) Alternate endpoint probes:")
for ep in ["news/press-releases-latest","press-releases-latest","news/stock-latest","news/general-latest","fmp-articles","news/press-releases?symbol=NVDA"]:
    d=g(f"https://financialmodelingprep.com/stable/{ep}?page=0&limit=2&apikey={FMP}")
    n = len(d) if isinstance(d,list) else "ERR/"+str(d.get('__err__') or d)[:60] if isinstance(d,dict) else "?"
    print(f"   /stable/{ep:42s} -> {n}")

print("\n4) Invoke deal-scanner Lambda + inspect output")
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
r=lam.invoke(FunctionName="justhodl-deal-scanner",InvocationType="RequestResponse",Payload=b"{}")
print("  invoke:",r.get("StatusCode"),r.get("FunctionError"))
import time; time.sleep(2)
try:
    j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/deal-scanner.json")["Body"].read())
    print("  feed keys:",list(j.keys()))
    print("  scanned_count:",j.get("scanned_count"),"| counts:",{k:v for k,v in j.items() if isinstance(v,int)})
    for k in ["deals","ai_deals","green_deals"]:
        if k in j: print(f"    {k}: {len(j[k])}")
    print("  source note:",j.get("sources"))
except Exception as e: print("  feed read err:",str(e)[:120])
print("DONE 2635")
