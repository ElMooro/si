import urllib.request, urllib.parse, time
def g(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh-gdelt"}),timeout=20)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e: return e.code, e.read().decode("utf-8","ignore")[:200]
    except Exception as e: return 0,str(e)[:120]
base="https://api.gdeltproject.org/api/v2/doc/doc?"
tests=[
 ("timelinetone 3months quoted", {"query":'"artificial intelligence"',"mode":"timelinetone","timespan":"3months","format":"json"}),
 ("timelinetone 1month quoted",   {"query":'"AI data center"',"mode":"timelinetone","timespan":"1month","format":"json"}),
 ("timelinetone unquoted",        {"query":'artificial intelligence datacenter',"mode":"timelinetone","timespan":"1month","format":"json"}),
 ("timelinevol 1month",           {"query":'"AI data center"',"mode":"timelinevol","timespan":"1month","format":"json"}),
 ("artlist sanity",               {"query":'"AI data center"',"mode":"artlist","maxrecords":"3","timespan":"1week","format":"json"}),
]
for name,params in tests:
    u=base+urllib.parse.urlencode(params)
    c,b=g(u); print("\n[%s] HTTP %s len=%d"%(name,c,len(b))); print("  head:",b[:300].replace(chr(10)," "))
    time.sleep(6)
