import boto3, json, urllib.request, urllib.parse
lam=boto3.client("lambda","us-east-1")
fk=None
for fn in ["justhodl-global-liquidity","justhodl-sovereign-fiscal"]:
    env=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
    if env.get("FRED_API_KEY"): fk=env["FRED_API_KEY"]; break
B="https://api.stlouisfed.org/fred"
def search(q):
    u=f"{B}/series/search?search_text={urllib.parse.quote(q)}&api_key={fk}&file_type=json&limit=5&order_by=popularity&sort_order=desc"
    try:
        j=json.loads(urllib.request.urlopen(u,timeout=20).read())
        return [(s["id"],s["title"][:58],s.get("frequency_short"),s.get("observation_end")) for s in j.get("seriess",[])]
    except Exception as e: return [("ERR",str(e)[:40],"","")]
# true LEADING indicators: spot prices, PPI, supplier deliveries / prices paid
QUERIES = {
 "copper_spot":"global price copper", "uranium_spot":"global price uranium",
 "aluminum_spot":"global price aluminum","nickel_spot":"global price nickel",
 "iron_ore_spot":"global price iron ore","cobalt_spot":"global price cobalt",
 "ppi_semis":"PPI semiconductor","ppi_steel":"PPI iron steel","ppi_electrical":"PPI power distribution transformer",
 "supplier_deliveries":"ISM supplier deliveries manufacturing","prices_paid_philly":"Philadelphia Fed prices paid",
 "delivery_time_ny":"Empire State delivery time","prices_paid_ny":"Empire State prices paid",
 "lead_times_richmond":"Richmond Fed vendor lead time","natgas_spot":"Henry Hub natural gas spot",
}
for label,q in QUERIES.items():
    print(f"\n{label} ('{q}'):")
    for r in search(q): print("   ", r)
print("\nDONE 2228")
