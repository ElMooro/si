import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
def show(nm,key,fn):
    print(f"===== {nm} ({key}) =====")
    try: fn(g(key))
    except Exception as e: print(" ERR",str(e)[:90])
def f_rot(d):
    s=(d.get("sectors") or d.get("rankings") or [])
    print(" n:",len(s)); 
    if s: print(" fields:",list(s[0].keys())); print(" ex:",json.dumps(s[0])[:260])
show("sector-rotation","data/sector-rotation.json",f_rot)
def f_mfs(d):
    print(" sectors[0]:",json.dumps((d.get('sectors') or [{}])[0])[:240])
    print(" inst_tilt[0]:",json.dumps((d.get('institutional_sector_tilt') or [{}])[0])[:200])
show("money-flow-state","data/money-flow-state.json",f_mfs)
def f_cfr(d):
    cx=d.get("complexes") or []
    print(" n_complexes:",len(cx))
    if cx: print(" fields:",list(cx[0].keys())); 
    for c in cx[:6]: print("  -",c.get("complex"),"| sector=",c.get("sector"),"| pump=",c.get("pump_probability"),"| regime=",c.get("regime"),"| div=",c.get("flow_price_divergence"))
show("capital-flow-radar","data/capital-flow-radar.json",f_cfr)
def f_fin(d):
    ind=d.get("industries") or []
    print(" n_ind:",len(ind))
    if ind: print(" fields:",list(ind[0].keys())); print(" ex:",json.dumps(ind[0])[:220])
show("finviz-groups","data/finviz-groups.json",f_fin)
def f_dp(d):
    bd=d.get("board") or []
    print(" board n:",len(bd))
    if bd: print(" fields:",list(bd[0].keys())); print(" ex:",json.dumps(bd[0])[:200])
show("dark-pool","data/dark-pool.json",f_dp)
def f_liq(d): print(" regime:",d.get("regime"),"| keys:",list(d.keys())[:10])
show("liquidity-flow","data/liquidity-flow.json",f_liq)
# universe map availability
try:
    u=g("data/universe.json"); us=u if isinstance(u,list) else (u.get("stocks") or u.get("universe") or [])
    print("universe n:",len(us),"| ex:",json.dumps(us[0])[:160] if us else "n/a")
except Exception as e: print("universe ERR",str(e)[:60])
print("DONE 2518")
