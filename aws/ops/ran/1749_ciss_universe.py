import urllib.request, boto3
def get(url,t=50):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:60]
# full CISS universe (all countries + U2, daily + monthly)
st,body=get("https://data-api.ecb.europa.eu/service/data/CISS?format=csvdata&lastNObservations=1")
keys={}
if st==200:
    lines=body.splitlines(); hdr=lines[0].split(","); ki=hdr.index("KEY"); ti=hdr.index("TIME_PERIOD")
    for ln in lines[1:]:
        c=ln.split(",")
        if len(c)>max(ki,ti): keys[c[ki]]=c[ti]
print(f"CISS flow: {len(keys)} total series")
# group by area + indicator suffix
from collections import Counter
areas=Counter(k.split(".")[2] for k in keys); print("  areas:", dict(areas))
freqs=Counter(k.split(".")[1] for k in keys); print("  freqs:", dict(freqs))
fresh=sum(1 for d in keys.values() if d>="2026-01-01"); print(f"  fresh (2026+): {fresh}/{len(keys)}")
# CLIFS flow
st2,body2=get("https://data-api.ecb.europa.eu/service/data/CLIFS?format=csvdata&lastNObservations=1")
n2=len(body2.splitlines())-1 if st2==200 else 0
print(f"CLIFS flow: http={st2} ~{n2} series")
# earliest date check for EA composite (does it reach 1996/1997?)
st3,body3=get("https://data-api.ecb.europa.eu/service/data/CISS/D.U2.Z0Z.4F.EC.SS_CIN.IDX?format=csvdata&firstNObservations=1")
print("EA composite earliest obs:", body3.splitlines()[-1].split(",")[ {h:i for i,h in enumerate(body3.splitlines()[0].split(','))}.get('TIME_PERIOD',5) ] if st3==200 and len(body3.splitlines())>1 else st3)
