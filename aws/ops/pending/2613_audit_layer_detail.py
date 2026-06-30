"""ops 2613 — detailed shapes for eurodollar-plumbing layers (fx/settlement/backstops), fails, flows."""
import urllib.request, json, time
PX="https://justhodl-data-proxy.raafouis.workers.dev"
def gp(p):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(f"{PX}/{p}?t={int(time.time())}",headers={"User-Agent":"M"}),timeout=18).read())
    except Exception as e: return {"__err__":str(e)[:40]}
edp=gp("data/eurodollar-plumbing.json")
lay=edp.get("layers") if isinstance(edp,dict) else {}
for k in ["fx","settlement","backstops","bank_funding"]:
    v=lay.get(k)
    print(f"\n### eurodollar-plumbing.layers.{k} ###")
    if isinstance(v,dict):
        for kk,vv in v.items():
            if isinstance(vv,(dict,list)): print(f"   {kk}: {json.dumps(vv)[:240]}")
            else: print(f"   {kk}: {vv}")
    else: print("  ", v)
print("\n### settlement-fails (full) ###")
sf=gp("data/settlement-fails.json")
for k in ["signal","headline","totals","classes","as_of"]:
    print(f"   {k}: {json.dumps(sf.get(k))[:300] if isinstance(sf,dict) else sf}")
print("\n### etf-flows.by_category + heavy lists ###")
ef=gp("data/etf-flows.json")
if isinstance(ef,dict):
    bc=ef.get("by_category")
    print("   by_category type:", type(bc).__name__)
    if isinstance(bc,dict): 
        for k,v in list(bc.items())[:14]: print(f"     {k}: {json.dumps(v)[:160]}")
    elif isinstance(bc,list):
        for v in bc[:14]: print(f"     {json.dumps(v)[:160]}")
    print("   heavy_inflow:", json.dumps(ef.get("heavy_inflow"))[:200])
    print("   heavy_outflow:", json.dumps(ef.get("heavy_outflow"))[:200])
print("\n### capital-flow.category_rotation ###")
cf=gp("data/capital-flow.json")
if isinstance(cf,dict):
    print("   category_rotation:", json.dumps(cf.get("category_rotation"))[:300])
    print("   etf_flows_in:", json.dumps(cf.get("etf_flows_in"))[:200])
    print("   etf_flows_out:", json.dumps(cf.get("etf_flows_out"))[:200])
print("DONE 2613")
