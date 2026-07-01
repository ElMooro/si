"""ops 2657 — confirm the redesign is actually live."""
import urllib.request, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 Chrome/124"}),timeout=25).read().decode()
html = get(f"https://justhodl.ai/master-rank.html?cb={int(time.time())}")
checks = {
 "new platform palette (#0a0e14) in place": "#0a0e14" in html,
 "old orphaned palette (#0a0a0a/#facc15 theme-color) gone": 'content="#0a0e14"' in html and 'content="#facc15"' not in html,
 "spotlight/hero present": "spotlight" in html,
 "conviction-meter class present": "meter-dot" in html,
 "card-based leaderboard (no <table>)": "<table>" not in html,
 "category pill system present": "cat-fund" in html,
 "macro z-bar visual present": "zbar-fill" in html,
 "nav-drawer still present": "jh-nav-drawer.js" in html,
 "watchlist wiring still present": "wlEnabled" in html,
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
print("bytes:", len(html))
print("DONE 2657")
