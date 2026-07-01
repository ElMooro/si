"""ops 2665 — final combined verification: live engine output + live page markup, for the
generalized all-sectors re-rating radar."""
import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 Chrome/124"}),timeout=25).read().decode()

j = json.loads(get(f"https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/ai-rerating-radar.json?cb={int(time.time())}"))
print("=== LIVE ENGINE ===")
print("version:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
cov = j.get("coverage", {})
print(f"universe={cov.get('n_universe')} ai_cohort={cov.get('n_ai_cohort')} sectors={cov.get('n_sectors')}")
print("by_sector:", cov.get("by_sector"))
s = j.get("summary", {})
print(f"n_priced={s.get('n_priced')} n_candidates={s.get('n_candidates')} n_contagion={s.get('n_contagion')}")
print(f"red_flagged: {len(s.get('red_flagged') or [])}")

html = get(f"https://justhodl.ai/ai-rerating.html?cb={int(time.time())}")
checks = {
 "new platform palette (#0a0e14)": "#0a0e14" in html,
 "old palette (#07090f) gone": "#07090f" not in html,
 "sector grid present": "sector-grid" in html and "sectorgrid" in html,
 "sector filter JS present": "SECTOR_FILTER" in html,
 "AI-infra cohort tag present": "tag ai" in html,
 "quality-flagged section present": "Quality-Flagged" in html,
 "peer-group-relative labeling present": "peer_group_kind" in html,
 "nav-drawer present": "jh-nav-drawer.js" in html,
 "old '~94-name cohort' text gone": "~94-name" not in html,
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
print("bytes:", len(html))
print("DONE 2665")
