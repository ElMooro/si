import urllib.request
def get(u,t=30):
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai","Cache-Control":"no-cache"}),timeout=t) as r: return r.status,r.read().decode("utf-8","replace")
s,html=get("https://justhodl.ai/why.html")
print("page ->",s,"len",len(html))
print("renderOptions present:", "function renderOptions" in html)
print("wired in report:", "html += renderOptions(d);" in html)
print("OMON header:", "Options-Implied Expectations" in html)
print("renderAnalystRatings still present:", "function renderAnalystRatings" in html)
print("DONE 2281")
