import urllib.request
p=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/retail/",headers={"User-Agent":"Mozilla/5.0"}),timeout=20).read().decode("utf-8","ignore")
checks={"hottest hero":'id="hottest"' in p,"momentum panel":'id="momentum-confirmed"' in p,"divergence panel":'id="divergence"' in p,"renderHottest":"function renderHottest" in p,"buzzBadge":"function buzzBadge" in p,"hooked":"renderHottest();" in p,"table heat col":"t-heat" in p,"buzz css":".bz-mom" in p}
print("PAGE /retail/ live:")
for k,v in checks.items(): print(f"  {'OK' if v else 'MISS'}  {k}")
