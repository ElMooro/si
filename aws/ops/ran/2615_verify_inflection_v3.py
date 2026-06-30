import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
print("page bytes:", len(html))
checks={
 "SVG speedometer (speedo fn)":"function speedo" in html and "A${R},${R}" in html,
 "ghost projection needle":"projection" in html and "showGhost" in html,
 "trajectory panel":"Where it's headed" in html and "headbadge" in html,
 "dollar shortage section":"Dollar shortage" in html,
 "fails-to-deliver/receive":"fails-to-deliver" in html and "fails-to-receive" in html,
 "swap lines / backstops":"swap lines" in html.lower() or "backstops" in html.lower(),
 "flow divergence bars (flowBars)":"function flowBars" in html and "Treasuries" in html,
 "dash-for-cash wording present":"dash-for-cash" in html.lower() or "DASH_FOR" in html,
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
print(f"  feed v{j.get('version')} · composite {(j.get('composite') or {}).get('liquidity_score')} {(j.get('composite') or {}).get('regime')}")
tr=j.get('trajectory') or {}; print(f"  trajectory: {tr.get('heading')} (vote {tr.get('vote')})")
ds=j.get('dollar_shortage') or {}; sf=j.get('settlement_fails') or {}; fd=j.get('flow_divergence') or {}
print(f"  dollar_shortage {ds.get('status')} · fails {sf.get('regime')} (FtD {sf.get('ust_ftd_bn')}/FtR {sf.get('ust_ftr_bn')}) · flow {fd.get('regime')}")
print("DONE 2615")
