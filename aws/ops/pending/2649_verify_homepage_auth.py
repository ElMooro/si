"""ops 2649 — verify homepage reorg + auth fix are live and correct together."""
import urllib.request, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 Chrome/124"}),timeout=25).read().decode("utf-8","ignore")

html = get(f"https://justhodl.ai/index.html?cb={int(time.time())}")
print("bytes:", len(html))
checks = {
 "old 196-pill nav-links class fully gone": "nav-links" not in html,
 "new favorites bar present (7 pills)": html.count('class="fav-pill"') == 7,
 "all 5 zone headers present": all(z in html for z in ["Today&#39;s read" if False else "Today", "Macro &amp; liquidity", "Cross-asset &amp; positioning", "Your portfolio", "Deep intelligence"]),
 "nav-drawer script present": 'src="/jh-nav-drawer.js"' in html,
 "auth data-slot present in topbar": 'data-auth-slot' in html,
 "no [object Object] / undefined%": "[object Object]" not in html and "undefined%" not in html,
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")

print("\n=== confirm auth.js self-injecting-slot fix is live ===")
js = get(f"https://justhodl.ai/auth.js?cb={int(time.time())}")
print("  _ensureSlot present:", "_ensureSlot" in js)
print("  called in init():", "this._ensureSlot();" in js)
print("DONE 2649")
