import urllib.request, json
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(path):
    url="https://financialmodelingprep.com/stable/%s%sapikey=%s"%(path, "&" if "?" in path else "?", FMP)
    try:
        raw=urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"jh"}),timeout=20).read()
        d=json.loads(raw)
        if isinstance(d,list): return "LIST[%d] e0keys=%s e0=%s"%(len(d), list(d[0].keys()) if d else [], json.dumps(d[0],default=str)[:260] if d else "")
        if isinstance(d,dict): return "DICT keys=%s %s"%(list(d.keys())[:10], json.dumps(d,default=str)[:200])
        return str(d)[:200]
    except Exception as e:
        body=""
        try: body=e.read().decode()[:120]
        except Exception: pass
        return "ERR %s %s"%(str(e)[:70], body)
for p in ["news/press-releases-latest?page=0&limit=5","news/stock-latest?page=0&limit=5",
          "news/general-latest?page=0&limit=5","press-releases?symbol=NVDA&limit=3",
          "news/press-releases?symbol=NVDA&limit=3","news/stock?symbols=NVDA&limit=3",
          "income-statement?symbol=AIP&limit=1"]:
    print("\n### /stable/%s"%p); print("  ", get(p))
