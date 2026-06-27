import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
M=["The read — rules-based synthesis","Risk-on drivers","Risk-off drivers","Own — what's leading","Key risk","Cycle trajectory — the clock","day","deterministic, runs without","theRead","trajectorySection"]
hits=[m for m in M if m in html]
print("page markers:",len(hits),"/",len(M),"→ missing:",[m for m in M if m not in html])
print("page bytes:",len(html))
print("DONE 2346")
