"""ops 2643 — verify nav-manifest.json + jh-nav-drawer.js + regenerated directory.html live."""
import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")

m = json.loads(get(f"https://justhodl.ai/nav-manifest.json?cb={int(time.time())}"))
print("nav-manifest.json LIVE:", m.get("n_pages"), "pages,", len(m.get("categories",[])), "categories")
for c in m["categories"]: print(f"  {c['name']}: {c['count']}")

js = get(f"https://justhodl.ai/jh-nav-drawer.js?cb={int(time.time())}")
print(f"\njh-nav-drawer.js LIVE: {len(js)} bytes, guard present: {'__jhNavDrawer' in js}")

dhtml = get(f"https://justhodl.ai/directory.html?cb={int(time.time())}")
print(f"\ndirectory.html LIVE: {len(dhtml)} bytes")
print("  new 8-category taxonomy present:", "Crypto & Digital" in dhtml, "System & Meta" in dhtml)
print("  old 9-cat 'Meta, Status & Misc' gone:", "Meta, Status & Misc" not in dhtml)
print("  dead stub NOT listed:", "catalysts.html" not in dhtml and "positioning.html" not in dhtml)
print("DONE 2643")
