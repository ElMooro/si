import urllib.request, re
def get(url,t=50):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com","Accept":"application/xml"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:90]

print("=== datastructure ECB_SUP1 (dimensions + codelists) ===")
st,body=get("https://data-api.ecb.europa.eu/service/datastructure/ECB/ECB_SUP1?references=all")
print("  http",st,"bytes",len(body) if isinstance(body,str) else "-")
if st==200:
    dims=re.findall(r'<str:Dimension id="([^"]+)"',body)
    print("  dimensions:", dims)
    # codes whose names mention confidence/sentiment/etc
    codes=re.findall(r'<str:Code id="([^"]+)">\s*(?:<[^>]+>\s*)*?<com:Name[^>]*>([^<]+)</com:Name>',body)
    pat=re.compile(r'confiden|sentiment|economic sentiment|industr|servic|consum|retail|construct|employ',re.I)
    hits=[(i,n) for i,n in codes if pat.search(n)]
    print(f"  {len(codes)} codes total; survey-relevant:")
    for i,n in hits[:60]: print(f"    {i:18} {n[:60]}")

print("\n=== try serieskeysonly on SUP flowRef (sample keys) ===")
for flow in ["SUP","ECB_SUP1"]:
    st,body=get(f"https://data-api.ecb.europa.eu/service/data/{flow}/?detail=serieskeysonly&format=csvdata")
    ls=body.splitlines() if isinstance(body,str) else []
    print(f"  {flow}: http={st} lines={len(ls)}")
    for ln in ls[1:9]: print("     "+ln[:70])
    if st==200 and len(ls)>1: break
