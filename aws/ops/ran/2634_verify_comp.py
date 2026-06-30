import urllib.request, json, time
def get(u): return urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"M","Cache-Control":"no-cache"}),timeout=25).read().decode("utf-8","ignore")
proxy="https://justhodl-data-proxy.raafouis.workers.dev"
j=json.loads(get(f"{proxy}/data/liquidity-inflection.json?cb={int(time.time())}"))
cc=j.get("composite_clock") or {}; cp=j.get("composite_projection") or {}
print(f"feed v{j.get('version')}")
print(f"  composite_clock: {cc.get('phase')} (impulse {cc.get('impulse')}, accel {cc.get('acceleration')}, {len(cc.get('orbit') or [])} orbit pts)")
print(f"  composite_projection: {cp.get('current_score')}→{cp.get('projected_score')}/100, led by {cp.get('primary_driver')}, {len(cp.get('component_z') or cp.get('components_used') or [])} components")
html=get(f"https://justhodl.ai/inflection.html?cb={int(time.time())}")
for k,n in {"composite projection section":"Composite forward projection","compproj mount":'id="compproj"',"renderCompProjection":"renderCompProjection","composite clock section":"Composite cycle clock","compclock mount":'id="compclock"',"net-liq projection (kept)":"Forward net-liquidity projection","net-liq clock (kept)":"Liquidity cycle clock"}.items():
    print(f"  page [{'OK' if n in html else 'MISS'}] {k}")
print("DONE 2634")
