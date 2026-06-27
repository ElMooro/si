import urllib.request, re
UA={"User-Agent":"Mozilla/5.0"}
html=urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/cycle-clock.html",headers=UA),timeout=20).read().decode("utf-8","ignore")
m=re.search(r'function summaryStrip.{0,40}?return `(<div class="strip">.*?</div>\s*`)', html, re.S)
print("summaryStrip body found:", bool(m))
# show the cell labels present in the strip template
for lab in ["Posture","Quadrant","Recession","Net liquidity","Tail"]:
    print(f"  label '{lab}' in template:", (">"+lab+"<") in html or (lab+"<") in html)
print("DONE 2353")
