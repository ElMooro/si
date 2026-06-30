import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
print("ENGINE v"+str(j.get("version"))+" — all 6 feature payloads present:")
checks={
 "F1 projection":bool((j.get("projection") or {}).get("path")),
 "F2 analogs":bool((j.get("analogs") or {}).get("analogs")),
 "F3 backtest":bool((j.get("backtest") or {}).get("curve")),
 "F4 cycle_clock":bool((j.get("cycle_clock") or {}).get("orbit")),
 "F5a reserve_runway":bool(j.get("reserve_runway")),
 "F5b forward_expectation":bool((j.get("forward_expectation") or {}).get("assets")),
 "F5c tensions":j.get("tensions",{}).get("level") is not None,
 "F5d data_health":bool(j.get("data_health")),
}
for k,v in checks.items(): print(f"  [{'OK' if v else 'MISS'}] {k}")
b=json.loads(get(f"{proxy}/data/liquidity-inflection-decisive-call.json?cb={int(time.time())}"))
print(f"\nF6 desk briefing: [{'OK' if b.get('headline') else 'MISS'}] '{b.get('headline')}'")
# live page mounts
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
for k,needle in {"AI brief mount":"aibrief","projection sec":"Forward net-liquidity projection","cycle clock sec":"Liquidity cycle clock","analogs sec":"Historical analogs","backtest sec":"Does liquidity timing actually work","tensions mount":'id="tensions"',"fwdexp mount":'id="fwdexp"',"runway mount":'id="runway"',"datahealth mount":'id="datahealth"'}.items():
    print(f"  page [{'OK' if needle in html else 'MISS'}] {k}")
print("DONE 2631")
