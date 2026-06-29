import urllib.request
for host in ["https://justhodl.ai/sector-flow.html","https://elmooro.github.io/si/sector-flow.html"]:
    try:
        req=urllib.request.Request(host,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)","Cache-Control":"no-cache"})
        html=urllib.request.urlopen(req,timeout=30).read().decode("utf-8","ignore")
        found="FOUND" if "Relative Rotation Graph" in html else "MISSING"
        print(f"{host}\n   {found} | renderRRG={'Y' if 'renderRRG' in html else 'N'} | bytes={len(html)}")
    except Exception as e:
        print(f"{host}\n   ERR {str(e)[:70]}")
print("DONE 2493")
