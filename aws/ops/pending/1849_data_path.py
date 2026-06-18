import boto3, urllib.request, json
lam=boto3.client("lambda",region_name="us-east-1")
def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl/1.0"}),timeout=25) as r:
            return 200,r.read().decode()
    except urllib.error.HTTPError as e: return e.code,e.read().decode()[:160]
    except Exception as e: return -1,"%s %s"%(type(e).__name__,e)

# 1) live EIA key from eia-energy-agent env (masked)
env=lam.get_function_configuration(FunctionName="eia-energy-agent").get("Environment",{}).get("Variables",{})
ek=env.get("EIA_API_KEY","")
print("eia-energy-agent live EIA_API_KEY: len=%d head=%s (dead-fallback head=trvQ)"%(len(ek), ek[:4] if ek else "NONE"))
if ek:
    c,b=get("https://api.eia.gov/v2/petroleum/pri/spt/data/?api_key=%s&frequency=daily&data[0]=value&facets[series][]=RWTC&sort[0][column]=period&sort[0][direction]=desc&length=2"%ek)
    print("  EIA test with live key [%s]: %s"%(c, b[:160].replace("\n"," ")))

# 2) FMP commodities (the entitlement we KNOW works)
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
for sym in ["CLUSD","BZUSD","RBUSD","HOUSD","NGUSD"]:
    c,b=get("https://financialmodelingprep.com/stable/quote?symbol=%s&apikey=%s"%(sym,FMP))
    px=None
    if c==200:
        try: px=json.loads(b)[0].get("price")
        except Exception: px=b[:80]
    print("  FMP %s [%s] price=%s"%(sym,c,px))

# 3) FRED Cushing candidates
FRED="2f057499936072679d8843d7fce99989"
for sid in ["WCESTUS1","W_EPC0_SAX_YCUOK_MBBL","DCOILWTICO"]:
    c,b=get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=1"%(sid,FRED))
    val=None
    if c==200:
        try: val=json.loads(b)["observations"][0]
        except Exception: val=b[:60]
    print("  FRED %s [%s] -> %s"%(sid,c,val))
