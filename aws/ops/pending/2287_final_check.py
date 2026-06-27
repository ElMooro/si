import urllib.request, time
def get(u,t=25):
    try:
        with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Cache-Control":"no-cache"}),timeout=t) as r: return r.status,r.read().decode("utf-8","replace")
    except Exception as e: return None,str(e)[:50]
s,html=get("https://justhodl.ai/why.html")
print("page status:", s, "len", len(html))
print("new loader (nogen poll):", "?nogen=1" in html or "nogen=1" in html)
print("async trigger:", "async=1" in html)
print("generate-then-poll msg:", "generating institutional-grade research" in html)
print("old direct-lambda Phase2 GONE:", "calling Lambda directly" not in html)
print("old dead-end msg GONE:", "Popular pre-cached names" not in html)
# quick proxy sanity: warm read fast
t=time.time(); s2,_=get("https://justhodl-data-proxy.raafouis.workers.dev/equity-research/PLTR.json?nogen=1&v="+str(int(time.time())))
print(f"proxy warm read PLTR: {s2} in {time.time()-t:.1f}s")
print("DONE 2287")
