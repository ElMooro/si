import urllib.request
def get(u,t=30):
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai","Cache-Control":"no-cache"}),timeout=t) as r: return r.status,r.read().decode("utf-8","replace")
s,html=get("https://justhodl.ai/why.html")
print("page ->",s,"len",len(html))
print("aiUnavailable present:", "function aiUnavailable" in html)
print("aiPending present:", "function aiPending" in html)
print("call-sites in source:", html.count("aiPending('"))
print("DONE 2282")
