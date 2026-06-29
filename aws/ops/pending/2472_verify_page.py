import urllib.request
for url in ["https://justhodl.ai/bottleneck-boom.html"]:
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (JustHodl verify)"})
        h=urllib.request.urlopen(req,timeout=25).read().decode("utf-8","replace")
        print(url,"len",len(h))
        for marker in ["Leading Bottleneck Radar","function renderLeading","id=\"leading\"","forward_state"]:
            print("  %-28s %s"%(marker, "FOUND" if marker in h else "MISSING"))
    except Exception as e:
        print("ERR",str(e)[:90])
print("DONE 2472")
