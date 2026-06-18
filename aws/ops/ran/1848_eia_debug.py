import urllib.request, json
KEY="trvQDpg2GdvBixLeieVMyaQwsnkFQlYSuecVm4Pl"
def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl/1.0"}),timeout=25) as r:
            return 200, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]
    except Exception as e:
        return -1, "%s %s"%(type(e).__name__,e)

# 1) route metadata -> facets + frequencies
c,b=get("https://api.eia.gov/v2/petroleum/pri/spt?api_key=%s"%KEY)
print("=== route meta petroleum/pri/spt [%s] ==="%c)
if c==200:
    j=json.loads(b).get("response",{})
    print("facets:",[f.get("id") for f in j.get("facets",[])])
    print("freqs:",[f.get("id") for f in j.get("frequency",[])])
    print("data cols:", list((j.get("data") or {}).keys()) if isinstance(j.get("data"),dict) else j.get("data"))
else:
    print(b)

# 2) try the data call with facet 'series'
for fac in ["series","seriesId","product"]:
    c,b=get("https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key=%s&frequency=daily&data[0]=value&facets[%s][]=RWTC&sort[0][column]=period&sort[0][direction]=desc&length=2"%(KEY,fac))
    print("\n=== data facet=%s [%s] ==="%(fac,c), b[:220])

# 3) Cushing stocks route meta
c,b=get("https://api.eia.gov/v2/petroleum/stoc/wstk?api_key=%s"%KEY)
print("\n=== route meta petroleum/stoc/wstk [%s] ==="%c)
if c==200:
    j=json.loads(b).get("response",{}); print("facets:",[f.get("id") for f in j.get("facets",[])])
