import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
print("page bytes:", len(html))
for k,v in {
 "regime-state section":"Forward returns by liquidity state" in html,
 "renderRegimeReturns fn":"renderRegimeReturns" in html and "excess vs unconditional baseline" in html,
 "significance ★ logic":"|t|≥2" in html or "significant peak" in html,
 "flip-log section":"Actual impulse flips" in html and "renderFlipLog" in html,
 "old n=2 aggregate removed":"after_UP_flip" not in html,
}.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
rr=j.get("regime_returns") or {}; sp=(rr.get("SPX_proxy") or {})
print(f"  feed v{j.get('version')} · SPX n_total={sp.get('n_total')} states={list((sp.get('states') or {}).keys())}")
lc=(j.get('lead_curves') or {}).get('HYG') or {}
print(f"  HYG lead best: {lc.get('best')}")
print("DONE 2621")
