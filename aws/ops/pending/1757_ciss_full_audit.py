import urllib.request
def get(url,t=60):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:60]
st,body=get("https://data-api.ecb.europa.eu/service/data/CISS?format=csvdata&lastNObservations=1")
rows=[]
if st==200:
    lines=body.splitlines(); h={x:i for i,x in enumerate(lines[0].split(","))}
    ki,ti=h["KEY"],h["TIME_PERIOD"]
    for ln in lines[1:]:
        c=ln.split(",")
        if len(c)>max(ki,ti): rows.append((c[ki],c[ti]))
CUT="2025-07-01"
# parse key: CISS.FREQ.AREA.x.x.x.INDICATOR.SUFFIX
def parse(k):
    p=k.split("."); return (p[1] if len(p)>1 else "?", p[2] if len(p)>2 else "?", p[-2] if len(p)>=2 else "?")
print(f"TOTAL CISS series: {len(rows)}\n")
print(f"{'KEY':52} {'FREQ':4} {'AREA':5} {'INDICATOR':14} {'LATEST':11} KEPT?")
kept=dropped=0
by_area_kept={}
for k,t in sorted(rows,key=lambda x:(x[0].split('.')[2],x[0])):
    fr,area,ind=parse(k)
    keep = t>=CUT
    if keep: kept+=1; by_area_kept[area]=by_area_kept.get(area,0)+1
    else: dropped+=1
    flag="keep" if keep else "DROP"
    # only print US, CN, and any DROPped, plus a few
    if area in ("US","CN","GB","GR","CZ","DK","HU","PL","SE") or not keep:
        print(f"{k:52} {fr:4} {area:5} {ind:14} {t:11} {flag}")
print(f"\nKEPT={kept} DROPPED={dropped}")
print("US present?", any(r[0].split('.')[2]=='US' for r in rows), "| US kept?", by_area_kept.get('US',0))
print("CN present?", any(r[0].split('.')[2]=='CN' for r in rows), "| CN kept?", by_area_kept.get('CN',0))
print("areas kept:", by_area_kept)
