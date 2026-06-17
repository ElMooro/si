import urllib.request, json
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl Research raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:90]
print("=== TIC current/historical sources ===")
for url in ["https://ticdata.treasury.gov/Publish/mfhhis01.txt",
            "https://home.treasury.gov/system/files/206/mfhhis01.txt",
            "https://ticdata.treasury.gov/Publish/slt3d.txt",
            "https://home.treasury.gov/sites/default/files/206/mfh.txt"]:
    st,b=get(url)
    if st==200:
        # find the most recent year/month mentioned near the top header
        head=b[:1500]
        print(f"  OK {url[-38:]:40} len={len(b)}")
        # print the month/year header lines
        for ln in b.splitlines()[:12]:
            if any(m in ln for m in ['202','Country','Jan','Apr','Feb','Mar']): print("     |",ln[:120])
    else: print(f"  {st} {url[-38:]}")
print("\n=== AMBS nested record (ambs.auctions[0]) ===")
st,b=get("https://markets.newyorkfed.org/api/ambs/all/results/details/last/2.json")
j=json.loads(b); a=j["ambs"]["auctions"]
print("  n=",len(a),"keys=",list(a[0].keys()))
print("  sample=",{k:a[0][k] for k in list(a[0].keys())[:14]})
print("\n=== seclending nested record (seclending.operations[0]) ===")
st,b=get("https://markets.newyorkfed.org/api/seclending/all/results/details/last/2.json")
j=json.loads(b); o=j["seclending"]["operations"]
print("  n=",len(o),"keys=",list(o[0].keys()))
print("  sample=",{k:o[0][k] for k in list(o[0].keys())[:14]})
