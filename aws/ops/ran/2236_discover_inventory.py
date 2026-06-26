import boto3, json, urllib.request, urllib.parse
lam=boto3.client("lambda","us-east-1")
fk=None
for fn in ["justhodl-global-liquidity","justhodl-sovereign-fiscal","justhodl-capital-inflows"]:
    try:
        env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
        if env.get("FRED_API_KEY"): fk=env["FRED_API_KEY"]; break
    except Exception: pass
B="https://api.stlouisfed.org/fred"
def search(q,n=6):
    u=f"{B}/series/search?search_text={urllib.parse.quote(q)}&api_key={fk}&file_type=json&limit={n}&order_by=popularity&sort_order=desc"
    try:
        j=json.loads(urllib.request.urlopen(u,timeout=20).read())
        return [(s["id"],s["title"][:52],s.get("frequency_short"),s.get("observation_end")) for s in j.get("seriess",[])]
    except Exception as e: return [("ERR",str(e)[:40])]
def latest(sid):
    try:
        u=f"{B}/series/observations?series_id={sid}&api_key={fk}&file_type=json&sort_order=desc&limit=2"
        j=json.loads(urllib.request.urlopen(u,timeout=15).read())
        o=[(x["date"],x["value"]) for x in j["observations"] if x["value"]!="."]
        return o[0] if o else "EMPTY"
    except Exception as e: return f"ERR{str(e)[:18]}"
print("=== SEARCHES (inventory-to-sales by sector) ===")
for q in ["inventories to sales ratio","manufacturers inventories sales ratio","retailers inventories sales ratio",
          "merchant wholesalers inventories sales","auto inventory sales ratio","total business inventories",
          "manufacturing inventories durable","semiconductor inventories"]:
    print(f"\n'{q}':")
    for r in search(q): print("   ",r)
print("\n=== TEST likely inventory/sales ratio ids ===")
for sid in ["ISRATIO","RETAILIRSA","MNFCTRIRSA","WHLSLRIRSA","AISRSA","BUSINV","TOTBUSIMNSA",
            "MRTSIR44000USS","RETAILIMSA","AISRSANT","DGORDER","MNFCTRIMSA","WHLSLRIMSA"]:
    print(f"  {sid}: {latest(sid)}")
print("DONE 2236")
