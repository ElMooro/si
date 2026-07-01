import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
print(f"feed v{j.get('version')}")
sl=j.get("swap_lines") or {}; ta=j.get("treasury_auctions") or {}; sf=j.get("stablecoin_full") or {}; comp=j.get("composite") or {}
print(f"  SRF: ${sl.get('srf_bn')}bn (active {sl.get('srf_active_days')}d)")
print(f"  Treasury auctions: {ta.get('regime')} {ta.get('composite_score')}/100 | calendar: ${(ta.get('near_term_calendar') or {}).get('scheduled_bill_issuance_bn')}bn/14d")
print(f"  Stablecoin: {sf.get('state')} strength {sf.get('signal_strength')} | 30d {sf.get('delta_30d_pct')}%")
print(f"  Composite: {comp.get('n_components')} components (was 10)")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
for k,n in {"auctions section":"Treasury auction health","stablecoin section":"Stablecoin flow","SRF in backstops":"SRF (repo backstop)","renderAuctions":"renderAuctions","renderStablecoin":"renderStablecoin"}.items():
    print(f"  page [{'OK' if n in html else 'MISS'}] {k}")
print("DONE 2642")
