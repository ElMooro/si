import urllib.request
for u in ["https://justhodl.ai/bottleneck-boom.html"]:
    try:
        r=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"})
        html=urllib.request.urlopen(r,timeout=20).read().decode("utf-8","ignore")
        import re
        m=re.search(r"const head=\[\[[^\]]+\]", html)
        print("live head array:", m.group(0)[:90] if m else "NOT FOUND")
        print("has OLD bug pattern ('l','ticker','Name'):", "['l','ticker','Name']" in html)
        print("has FIXED pattern ('ticker','Name','l'):", "['ticker','Name','l']" in html)
        # cache headers
        print("cache-control:", urllib.request.urlopen(r,timeout=20).headers.get("cache-control"))
    except Exception as e:
        print(u,"ERR",str(e)[:120])
