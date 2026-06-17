import urllib.request, json
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
def get(url,t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
NY="https://markets.newyorkfed.org/api/pd"
print("=== PD timeseries list — entries mentioning FAIL ===")
b=get(NY+"/list/timeseries.json")
if b:
    j=json.loads(b)
    # structure discovery
    print("top keys:",list(j.keys())[:5])
    arr=j.get("pd",{}).get("timeseries") or j.get("timeseries") or []
    if not arr:
        # maybe it's a flat list under some key
        for k,v in j.items():
            if isinstance(v,list): arr=v; print("using list key:",k); break
            if isinstance(v,dict):
                for k2,v2 in v.items():
                    if isinstance(v2,list): arr=v2; print("using list key:",k,k2); break
    print("n series:",len(arr))
    fails=[x for x in arr if 'fail' in json.dumps(x).lower()]
    for x in fails[:40]:
        print("  ",x.get("keyid") or x.get("key") or x, "|", (x.get("label") or x.get("description") or "")[:80])
else: print("list fetch failed")
