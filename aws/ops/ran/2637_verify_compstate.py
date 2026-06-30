import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
fec=j.get("forward_expectation_composite") or {}; an=j.get("analogs") or {}
print(f"feed v{j.get('version')}")
print(f"  composite fwd-exp: state {fec.get('state')} (z {fec.get('composite_z')}), src {fec.get('source')}, {len(fec.get('assets') or {})} assets")
print(f"  net-liq fwd-exp:   state {(j.get('forward_expectation') or {}).get('state')}")
print(f"  analogs composite_aware: {an.get('composite_aware')} | features {an.get('features')}")
print(f"  composite regime study assets: {list((j.get('regime_returns_composite') or {}).keys())}")
print(f"  snapshots accumulating: {(j.get('composite_snapshots') or {}).get('count')}")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
for k,n in {"dual fwd-exp (net-liq label)":"\u25b8 Net-liquidity impulse","dual fwd-exp (composite label)":"Full composite state (all components)","composite z handling":"composite z","FRED-backed provenance":"FRED-backed","composite analog label":"Composite state"}.items():
    print(f"  page [{'OK' if n in html else 'MISS'}] {k}")
print("DONE 2637")
