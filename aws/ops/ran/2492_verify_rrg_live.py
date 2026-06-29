import urllib.request
req=urllib.request.Request("https://justhodl.ai/sector-flow.html",headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
html=urllib.request.urlopen(req,timeout=30).read().decode("utf-8","ignore")
for m in ["Relative Rotation Graph","renderRRG","rrgQuad","Improving","RS momentum"]:
    print(f"  {'FOUND' if m in html else 'MISSING':7} {m}")
print("page bytes:",len(html))
print("DONE 2492")
