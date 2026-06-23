import urllib.request, time
for u in ["ma200-radar"]:
    try:
        r=urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/{u}.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
        body=r.read(800).decode("utf-8","ignore")
        print(f"{u}.html -> {r.getcode()} renders={('Retest' in body or '200-DMA' in body)}")
    except Exception as e: print(f"{u}.html -> {str(e)[:60]}")
print("DONE 2164")
