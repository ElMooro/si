import urllib.request
UA={"User-Agent":"Mozilla/5.0 (verify)"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
hits=sum(m in html for m in ["clockGI","growth × inflation","Best assets for this phase","Fed net liquidity","RORO","recDial","Nearest historical analogs","QHERO"])
print("markers present:", hits, "/ 8")
print("page bytes:", len(html))
print("DONE 2319")
