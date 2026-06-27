import urllib.request, json, boto3
UA={"User-Agent":"Mozilla/5.0 (verify)"}
def fetch(u):
    try:
        return urllib.request.urlopen(urllib.request.Request(u,headers=UA),timeout=20).read().decode("utf-8","ignore")
    except Exception as e: return "ERR "+str(e)[:60]
# 1) live page has the new code (Cloudflare Pages rebuilt)?
html=fetch("https://justhodl.ai/bottleneck-boom.html")
markers=["bestpick","entryTiming","riskAdj","AEROSPACE","'Confirm'","INORGANIC","qMark","maturity","24-mo pressure"]
print("LIVE PAGE markers:")
for m in markers: print(f"  {m}: {'YES' if m in html else 'no'}")
# 2) data files fresh + complete
s3=boto3.client("s3","us-east-1")
bb=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
rj=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
ip=bb.get("industry_pressure") or {}
print("\nBOOM json: groups=",list(ip.keys())," all have history+direction:",all((v.get('history') and (v.get('direction') or v.get('ip_yoy_z') is not None)) for v in ip.values()))
print("RESEARCH json: gen=",rj.get('generated_at')[:19]," has target_record:",rj.get('target_record') is not None," maturity:",bool((rj.get('track_record') or {}).get('maturity')))
bt=rj.get("by_ticker") or {}
nconf=sum(1 for r in bt.values() if r.get('confluence') is not None)
print(f"  confluence on {nconf}/{len(bt)} ; fwd_val on {sum(1 for r in bt.values() if r.get('fwd_val'))}/{len(bt)}")
print("DONE 2309")
