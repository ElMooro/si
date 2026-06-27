import urllib.request
UA={"User-Agent":"Mozilla/5.0 (verify)"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
print("Industry bottleneck window:", "Industry bottleneck window" in html)
print("function pressureViz:", "function pressureViz" in html)
print("windowBlock wiring:", "${windowBlock}" in html or "windowBlock" in html)
print("DONE 2312")
