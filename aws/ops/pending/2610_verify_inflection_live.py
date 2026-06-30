import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
print("page bytes:", len(html))
checks={
 "Composite liquidity regime section":"Composite liquidity regime" in html,
 "Reserve & buffer mechanics section":"Reserve &amp; buffer mechanics" in html or "Reserve & buffer mechanics" in html,
 "Funding & plumbing stress section":"Funding &amp; plumbing stress" in html or "Funding & plumbing stress" in html,
 "Global liquidity section":"Global liquidity" in html,
 "SOFR-IORB wiring":"SOFR" in html and "IORB" in html,
 "LCLoR/reserve language":"LCLoR" in html or "comfort floor" in html or "reserve" in html.lower(),
 "gauge render fn":"compBar" in html and "gauge" in html,
 "global CB breakdown":"central-bank" in html.lower() or "components_usd_bn" in html,
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
# confirm the feed it reads is v1.2.0 with composite
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
co=j.get("composite") or {}
print(f"  feed version {j.get('version')} · composite score {co.get('liquidity_score')} {co.get('regime')} · n {co.get('n_components')}")
print(f"  reserves ${ (j.get('reserves') or {}).get('level_usd_bn') }bn · rrp ${ (j.get('rrp') or {}).get('level_usd_bn') }bn · global ${ ((j.get('global_liquidity') or {}).get('index') or {}).get('total_usd_trillions') }T")
print("DONE 2610")
