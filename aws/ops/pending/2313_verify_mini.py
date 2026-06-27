import urllib.request
UA={"User-Agent":"Mozilla/5.0 (verify)"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottleneck-boom.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
print("function pressureMini:", "function pressureMini" in html)
print("'Window' header:", "'group_pressure','Window'" in html)
print("inline dial wired in row:", "pressureMini(ig)" in html)
print("DONE 2313")
