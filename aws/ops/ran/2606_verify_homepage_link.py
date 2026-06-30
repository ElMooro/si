import urllib.request, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
html=get(f"https://justhodl.ai/?cb={int(time.time())}")
print("homepage bytes:", len(html))
print("  [%s] links /buybacks.html" % ("OK" if "buybacks.html" in html else "MISS"))
print("  [%s] BUYBACKS pill label" % ("OK" if "💰 BUYBACKS" in html else "MISS"))
print("  [%s] links /attention.html (sanity)" % ("OK" if "attention.html" in html else "MISS"))
# and confirm the target page still serves
bb=get(f"https://justhodl.ai/buybacks.html?cb={int(time.time())}")
print("  [%s] /buybacks.html serves (Corporate Buyback Board)" % ("OK" if "Corporate Buyback Board" in bb else "MISS"))
print("DONE 2606")
