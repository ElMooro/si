import boto3, json, urllib.request, urllib.parse
from datetime import date, timedelta
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
POLY="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception: return {}
def poly(path,params=None):
    p=dict(params or {});p["apiKey"]=POLY
    u="https://api.polygon.io"+path+"?"+urllib.parse.urlencode(p)
    return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh"}),timeout=30).read())

# rebuild catalyst (ticker,date,type)
cats=[]
et=g("data/earnings-tracker.json")
for it in (et.get("recent_results_30d") or []):
    eps=it.get("eps_surprise_pct");rev=it.get("revenue_surprise_pct")
    if (isinstance(eps,(int,float)) and eps<-0.5) or (isinstance(rev,(int,float)) and rev<-0.5):
        cats.append((it.get("ticker"),(it.get("filing_date") or "")[:10],"EARNINGS_MISS"))
aa=g("data/analyst-actions.json")
for it in (aa.get("downgrades") or []):
    if str(it.get("rating_dir","")).upper()=="DOWNGRADE": cats.append((it.get("ticker"),it.get("date","")[:10],"DOWNGRADE"))
for it in (aa.get("pt_cuts") or []): cats.append((it.get("ticker"),it.get("date","")[:10],"PT_CUT"))
for it in (aa.get("guidance_cuts") or []): cats.append((it.get("ticker"),it.get("date","")[:10],"GUIDANCE_CUT"))
print("total catalysts:",len(cats))

# grouped-daily for last ~8 trading days to get returns
end=date.today();days=[]
d=end-timedelta(days=14)
gd_by_date={}
while d<=end:
    if d.weekday()<5:
        try:
            j=poly("/v2/aggs/grouped/locale/us/market/stocks/%s"%d,{"adjusted":"true"})
            if j.get("results"):
                gd_by_date[d.isoformat()]={r["T"]:r for r in j["results"]}
                days.append(d.isoformat())
        except Exception: pass
    d+=timedelta(days=1)
days=sorted(gd_by_date)
print("grouped days available:",days[-6:] if len(days)>=6 else days)

def ret_on(tk,dstr):
    # map to first trading day >= dstr
    td=next((x for x in days if x>=dstr),None)
    if not td: return None
    i=days.index(td)
    if i==0: return None
    cur=gd_by_date[td].get(tk); prev=gd_by_date[days[i-1]].get(tk)
    if not cur or not prev: return None
    return td,(cur["c"]/prev["c"]-1)*100, cur["c"]*cur["v"]

held=fell=nodata=0
print("\nticker | type | catdate | mapped | day_return | liquid?")
for tk,dt,ty in cats:
    r=ret_on(tk,dt)
    if r is None: nodata+=1; continue
    td,ret,dvol=r
    liq = dvol and dvol>25e6
    tag = "HELD" if ret>-1 else "fell"
    if ret>-1: held+=1
    else: fell+=1
    if ty in ("EARNINGS_MISS","GUIDANCE_CUT") or liq or ret>-1:
        print(f"  {tk:<6} {ty:<13} {dt} -> {td} | {ret:+.1f}% | {'liq' if liq else 'thin'} | {tag}")
print(f"\nSUMMARY: held(>-1%) {held} | fell {fell} | nodata {nodata}")
print("=> if some HELD on liquid names, the idio path is valid (just rare/very recent); if all fell, idio=0 is reality")
print("DONE 2097")
