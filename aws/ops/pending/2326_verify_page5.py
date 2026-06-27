import urllib.request
UA={"User-Agent":"Mozilla/5.0"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
M=["crossAssetSection","Cross-asset confirmation","forwardCone","Forward path","scenarioPlaybook","Scenario playbook","globalLiqPanel","Global liquidity","sparkline","TAPE "]
hits=[m for m in M if m in html]
print("page markers:", len(hits), "/", len(M))
print("missing:", [m for m in M if m not in html])
print("page bytes:", len(html))
print("DONE 2326")
