"""ops 2655 — final combined verification: live engine output + live page markup."""
import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 Chrome/124"}),timeout=25).read().decode("utf-8","ignore")

j = json.loads(get(f"https://justhodl.ai/data/master-ranker.json?cb={int(time.time())}")) if False else None
# use the same S3-direct URL the page itself uses
j = json.loads(get(f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/master-ranker.json?cb={int(time.time())}"))
tt = j.get("top_tickers") or []
new_fams = {"institutional_13f","estimate_revisions","forward_orders","squeeze_setup","earnings_quality_hi"}
touched = [t for t in tt if new_fams & set(t.get("systems") or [])]
print(f"LIVE ENGINE: {len(tt)} top tickers, {len(touched)} touched by a new signal family")
print(f"  sample: {touched[0]['ticker']} -> {touched[0]['rationale'][:140]}")

html = get(f"https://justhodl.ai/master-rank.html?cb={int(time.time())}")
checks = {
 "no truncated rationale (.slice(0,130) gone)": ".slice(0,130)" not in html,
 "expand-toggle rows present": "expand-toggle" in html,
 "contribution bar rendering present": "contrib-bar-fill" in html,
 "multiplier badge logic present": "mult-badge" in html,
 "flag chip logic present": "flag-chip" in html,
 "data-health strip present": "healthStrip" in html,
 "new pill colors present": "pill.forward_orders" in html,
 "old nav dump gone": "🌪 Stress" not in html and "AUCTIONS" not in html,
 "nav-drawer present": 'jh-nav-drawer.js' in html,
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
print("DONE 2655")
