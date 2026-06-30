import urllib.request, json, time
PX="https://justhodl-data-proxy.raafouis.workers.dev"
def gp(p):
    try: return json.loads(urllib.request.urlopen(urllib.request.Request(f"{PX}/{p}?t={int(time.time())}",headers={"User-Agent":"M"}),timeout=18).read())
    except Exception as e: return {"__err__":str(e)[:40]}
rl=gp("data/repo-lending.json")
print("### repo-lending ###")
print("  composite_leverage_stress:", rl.get("composite_leverage_stress"), "regime:", rl.get("regime"))
for k in ["margin_debt","repo","securities_lending"]:
    v=rl.get(k)
    print(f"  {k}:", json.dumps(v)[:260] if v is not None else None)
ds=gp("data/dealer-survey.json")
print("\n### dealer-survey ###")
print("  top keys:", list(ds.keys()) if isinstance(ds,dict) else type(ds).__name__)
ls=ds.get("latest_survey") if isinstance(ds,dict) else None
print("  latest_survey type:", type(ls).__name__)
if isinstance(ls,dict):
    for k,v in list(ls.items())[:18]:
        print(f"    {k}: {json.dumps(v)[:160] if isinstance(v,(dict,list)) else v}")
elif isinstance(ls,list):
    print("    [list len", len(ls),"] sample:", json.dumps(ls[0])[:240] if ls else None)
print("  last_check_status:", ds.get("last_check_status") if isinstance(ds,dict) else None)
print("DONE 2616")
