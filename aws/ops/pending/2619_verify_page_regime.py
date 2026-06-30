import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
print("page bytes:", len(html))
print("  [%s] leverage-stress card" % ("OK" if "Leverage stress" in html and "margin debt" in html else "MISS"))
print("  [%s] dealer-survey card" % ("OK" if "dealer survey" in html.lower() else "MISS"))
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
ls=j.get("leverage_stress") or {}
print(f"  feed v{j.get('version')} · leverage {ls.get('score')} {ls.get('regime')} margin {ls.get('margin_debt_bn')}bn")
# confirm regime engines carry the new inputs
rr=json.loads(get(f"{proxy}/data/risk-regime.json?cb={int(time.time())}"))
lrm=(rr.get("components") or {}).get("liquidity_regime") or {}
print(f"  risk-regime: liq traj={lrm.get('trajectory')} ds={lrm.get('dollar_shortage')} score={lrm.get('score')}")
mr=json.loads(get(f"{proxy}/data/master-ranker.json?cb={int(time.time())}"))
rc=mr.get("regime_context") or {}
tl=[t for t in (mr.get('top_tickers') or []) if (t.get('liquidity_regime_mult') or 1.0)!=1.0]
print(f"  master-ranker: regime_context traj={rc.get('liquidity_trajectory')} ds={rc.get('dollar_shortage')} · {len(tl)} tickers forward-tilted")
print("DONE 2619")
