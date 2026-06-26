import boto3, json, urllib.request, urllib.parse
lam=boto3.client("lambda","us-east-1")
fk=None
for fn in ["justhodl-global-liquidity","justhodl-sovereign-fiscal"]:
    e=lam.get_function_configuration(FunctionName=fn).get("Environment",{}).get("Variables",{})
    if e.get("FRED_API_KEY"): fk=e["FRED_API_KEY"]; break
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(u):
    return json.loads(urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh/1"}),timeout=25).read())

print("===== SECTOR LAYER: FRED inventories-to-sales ratios =====")
B="https://api.stlouisfed.org/fred"
for q in ["inventories to sales ratio","manufacturers inventories sales","retailers inventories sales"]:
    u=f"{B}/series/search?search_text={urllib.parse.quote(q)}&api_key={fk}&file_type=json&limit=6&order_by=popularity&sort_order=desc"
    print(f"\n'{q}':")
    for s in get(u).get("seriess",[]):
        print("   ",s["id"],"|",s["title"][:46],"|",s.get("frequency_short"),s.get("observation_end"))
def fred_trend(sid):
    try:
        u=f"{B}/series/observations?series_id={sid}&api_key={fk}&file_type=json&sort_order=desc&limit=14"
        o=[(x['date'],float(x['value'])) for x in get(u)['observations'] if x['value']!='.']
        if len(o)>=7:
            now=o[0][1]; q3=o[2][1]; yr=o[min(12,len(o)-1)][1]
            return f"latest {o[0][0]}={now:.3f} | 3mo={ (now/q3-1)*100:+.1f}% | 12mo={(now/yr-1)*100:+.1f}%"
        return "thin"
    except Exception as e: return f"ERR{str(e)[:25]}"
print("\n-- test key ratios (falling = inventory drawdown = bullish shortage) --")
for sid in ["ISRATIO","RETAILIRSA","MNFCTRIRSA","WHLSLRIRSA","MRTSIR4400XUSS","AISRSA"]:
    print(f"   {sid}: {fred_trend(sid)}")

print("\n===== STOCK LAYER: FMP /stable/ inventory + COGS (DIO) =====")
for ep in ["balance-sheet-statement","income-statement"]:
    u=f"https://financialmodelingprep.com/stable/{ep}?symbol=MU&period=quarter&limit=6&apikey={FMP}"
    try:
        d=get(u)
        if isinstance(d,list) and d:
            r0=d[0]
            keys=[k for k in r0 if any(w in k.lower() for w in ("inventory","cost","revenue","grossprofit","gross_profit","date","period","calendar"))]
            print(f"\n{ep}: {len(d)} quarters | relevant fields:",keys[:12])
            print("   latest:",{k:r0.get(k) for k in keys[:8]})
        else:
            print(f"\n{ep}: unexpected resp:",str(d)[:120])
    except Exception as e: print(f"\n{ep} ERR:",str(e)[:90])
print("DONE 2236")
