import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
M=["Rates, the Fed & the volatility complex","Positioning, sentiment & market internals","Growth & recession depth","Cross-asset risk & regime","PBOC / China","tileGrid","liquidity pulse","Yen-carry unwind","Leading labor","MOVE · bond vol"]
hits=[m for m in M if m in html]
print("page markers:", len(hits), "/", len(M), "→ missing:", [m for m in M if m not in html])
print("page bytes:", len(html))
print("DONE 2333")
