import urllib.request
def get(u):
    with urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Cache-Control":"no-cache"}),timeout=20) as r: return r.read().decode("utf-8","replace")
html=get("https://justhodl.ai/bottleneck-boom.html")
print("Fwd P/E column:", "'Fwd P/E'" in html or '"Fwd P/E"' in html or "Fwd P/E" in html)
print("Target column:", "['fwd_upside','Target']" in html)
print("upTitle price prediction:", "Price prediction → Bull" in html)
print("colspan 9:", 'colspan="9"' in html)
print("decorate ranks:", "rr.fwd_upside=_fv.tp_base_upside_pct" in html)
print("DONE 2292")
