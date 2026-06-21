import urllib.request, json, time
def get(u):
    req=urllib.request.Request(u,headers={"User-Agent":"justhodl/1.0"})
    with urllib.request.urlopen(req,timeout=60) as r:
        return json.loads(r.read())
# DefiLlama total stablecoin circulating history (free, no auth)
print("=== probe: stablecoins.llama.fi/stablecoincharts/all ===")
try:
    d=get("https://stablecoins.llama.fi/stablecoincharts/all")
    print("type:",type(d).__name__,"| points:",len(d) if isinstance(d,list) else "n/a")
    if isinstance(d,list) and d:
        print("first point keys:",list(d[0].keys()))
        print("sample first:",json.dumps(d[0])[:300])
        print("sample last:",json.dumps(d[-1])[:300])
        # date span
        def dt(x):
            t=x.get("date"); 
            return time.strftime("%Y-%m-%d",time.gmtime(int(t))) if t else "?"
        print("span:",dt(d[0]),"->",dt(d[-1]))
        # extract total circulating USD over time
        def tot(x):
            v=x.get("totalCirculatingUSD") or x.get("totalCirculating") or {}
            if isinstance(v,dict): return v.get("peggedUSD")
            return v
        last=tot(d[-1]); prior30=tot(d[-31]) if len(d)>31 else None
        print(f"latest total stablecoin mcap: ${last/1e9:.1f}B" if last else "latest: ?")
        if last and prior30: print(f"30d change: {(last/prior30-1)*100:+.1f}%")
except Exception as e:
    print("ERR all:",str(e)[:150])
print("DONE 2079")
