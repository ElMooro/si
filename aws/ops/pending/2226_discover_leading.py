import boto3, json, urllib.request, urllib.parse
lam=boto3.client("lambda","us-east-1")
fk=None
for fn in ["justhodl-global-liquidity","justhodl-sovereign-fiscal"]:
    env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
    if env.get("FRED_API_KEY"): fk=env["FRED_API_KEY"]; break
B="https://api.stlouisfed.org/fred"
def search(q,n=5):
    u=f"{B}/series/search?search_text={urllib.parse.quote(q)}&api_key={fk}&file_type=json&limit={n}&order_by=popularity&sort_order=desc"
    j=json.loads(urllib.request.urlopen(u,timeout=20).read())
    return [(s["id"],s["title"][:50],s.get("frequency_short"),s.get("observation_end")) for s in j.get("seriess",[])]
def latest(sid):
    try:
        u=f"{B}/series/observations?series_id={sid}&api_key={fk}&file_type=json&sort_order=desc&limit=2"
        j=json.loads(urllib.request.urlopen(u,timeout=15).read())
        o=[(x["date"],x["value"]) for x in j["observations"] if x["value"]!="."]
        return o[0] if o else "EMPTY"
    except Exception as e: return f"ERR{str(e)[:20]}"
print("=== SEARCHES (true leading inputs) ===")
for q in ["PPI semiconductors","global price uranium","global price lithium","global price copper",
          "ISM manufacturing prices","ISM supplier deliveries","PPI iron steel","memory chips price",
          "global price natural gas","producer price index computer storage"]:
    print(f"\n'{q}':")
    for r in search(q): print("   ",r)
print("\n=== TEST likely IMF/PPI ids ===")
for sid in ["PCOPPUSDM","PALUMUSDM","PNICKUSDM","PURANUSDM","PIORECRUSDM","PLEAD","PNGASEUUSDM",
            "WPU117","WPU101","PCU334413334413","WPS1178","NAPMPRI","NAPMSDI","WPU10"]:
    print(f"  {sid}: {latest(sid)}")
print("DONE 2226")
