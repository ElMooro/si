import boto3, json, urllib.request
lam=boto3.client("lambda","us-east-1")
fk=None
for fn in ["justhodl-global-liquidity","justhodl-sovereign-fiscal"]:
    env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
    if env.get("FRED_API_KEY"): fk=env["FRED_API_KEY"]; break
def gj(u): return json.loads(urllib.request.urlopen(u,timeout=25).read())
B="https://api.stlouisfed.org/fred"
# release id for the TIC net-transaction series
rel=gj(f"{B}/series/release?series_id=FORTREASNET99996&api_key={fk}&file_type=json")
rid=rel["releases"][0]["id"]; print("TIC release id:", rid, rel["releases"][0]["name"][:50])
# enumerate net-transaction series in that release (paginate)
nets=[]
off=0
while off<4000:
    j=gj(f"{B}/release/series?release_id={rid}&api_key={fk}&file_type=json&limit=1000&offset={off}")
    ss=j.get("seriess",[])
    for s in ss:
        t=s["title"]
        if "Net Transactions" in t and "99996" in s["id"]:   # 99996 = grand-total grouping
            nets.append((s["id"], t[:75], s.get("observation_end")))
    if len(ss)<1000: break
    off+=1000
print(f"\nNet-transaction grand-total series ({len(nets)}):")
for r in nets: print("   ", r)
# latest values for the treasury one + any 'long-term securities' total
def latest(sid,n=14):
    j=gj(f"{B}/series/observations?series_id={sid}&api_key={fk}&file_type=json&sort_order=desc&limit={n}")
    return [(o["date"],float(o["value"])) for o in j["observations"] if o["value"]!="."]
lt=latest("FORTREASNET99996")
print("\nFORTREASNET99996 latest (Treasury net, $M):", lt[:4])
if lt: print("  12mo rolling sum ($B):", round(sum(v for _,v in lt[:12])/1000,1))
print("DONE 2220")
