import urllib.request, time
for u in ["hot-stocks","options-confluence","flow-confluence"]:
    try:
        r=urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/{u}.html",headers={"User-Agent":"Mozilla/5.0 jh"}),timeout=15)
        body=r.read(400).decode("utf-8","ignore")
        ok="Confluence" in body or "Brief" in body or "<title>" in body
        print(f"  {u}.html -> {r.getcode()}  renders={ok}")
    except Exception as e:
        print(f"  {u}.html -> {str(e)[:50]}")
print("DONE 2155")
