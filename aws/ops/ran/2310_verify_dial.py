import urllib.request
UA={"User-Agent":"Mozilla/5.0 (verify)"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
for m in ["cyclePoint","dialD","cycleT","/100 now",">OPEN<",">PEAK<",">CLOSE<","Still rising — early"]:
    print(f"  {m}: {'YES' if m in html else 'no'}")
# confirm the old sparkline-trend label is gone from pressure cards
print("  old '24-mo pressure trend' removed:", "24-mo pressure trend" not in html)
print("DONE 2310")
