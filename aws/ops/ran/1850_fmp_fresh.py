import urllib.request, json, datetime
FMP="wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl/1.0"}),timeout=25) as r:
            return 200,r.read().decode()
    except urllib.error.HTTPError as e: return e.code,e.read().decode()[:160]
    except Exception as e: return -1,"%s %s"%(type(e).__name__,e)

now=int(datetime.datetime.utcnow().timestamp())
for sym in ["CLUSD","WTIUSD","BZUSD","RBUSD","HOUSD"]:
    c,b=get("https://financialmodelingprep.com/stable/quote?symbol=%s&apikey=%s"%(sym,FMP))
    if c==200:
        try:
            q=json.loads(b)[0]
            ts=q.get("timestamp")
            age_h=round((now-ts)/3600,1) if isinstance(ts,(int,float)) else "?"
            print("  %s price=%s chg=%s ts=%s age_h=%s"%(sym,q.get("price"),q.get("change"),ts,age_h))
        except Exception as e: print("  %s parse err %s :: %s"%(sym,e,b[:80]))
    else: print("  %s [%s] %s"%(sym,c,b[:80]))

# FRED Brent for the crude anchor pair
FRED="2f057499936072679d8843d7fce99989"
for sid in ["DCOILWTICO","DCOILBRENTEU"]:
    c,b=get("https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=1"%(sid,FRED))
    if c==200:
        o=json.loads(b)["observations"][0]; print("  FRED %s date=%s val=%s"%(sid,o["date"],o["value"]))
