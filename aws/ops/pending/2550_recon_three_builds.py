"""ops 2550 — recon for the three builds: ICE BofA YTW, Bahamas TIC, fiat-peg monitor."""
import urllib.request, json, boto3
FRED = "2f057499936072679d8843d7fce99989"
POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
s3 = boto3.client("s3", "us-east-1")

def jget(url):
    try:
        return json.loads(urllib.request.urlopen(url, timeout=20).read())
    except Exception as e:
        return {"_err": str(e)[:80]}

def fred_search(text):
    u = f"https://api.stlouisfed.org/fred/series/search?search_text={urllib.request.quote(text)}&api_key={FRED}&file_type=json&limit=8"
    d = jget(u)
    return [(s["id"], s["title"][:60]) for s in d.get("seriess", [])]

def fred_latest(sid):
    u = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&sort_order=desc&limit=1"
    d = jget(u)
    o = (d.get("observations") or [{}])[0]
    return o.get("value"), o.get("date"), d.get("_err")

print("=== (1) Bahamas / Bermuda / Caribbean TIC series in FRED ===")
for q in ["Bahamas Treasury Securities holdings", "Bermuda Treasury Securities holdings", "Caribbean Banking Centers Treasury"]:
    print(f"-- {q}")
    for sid, title in fred_search(q):
        if "Treasury" in title or "Holding" in title or "Foreign" in title:
            print(f"     {sid}: {title}")

print("\n=== (2) validate ICE BofA effective-yield (semi-annual YTW) series ===")
for sid in ["BAMLH0A0HYM2EY","BAMLC0A0CMEY","BAMLC0A1CAAAEY","BAMLC0A4CBBBEY","BAMLH0A2HYBEY","BAMLH0A3HYCEY","BAMLEMHYHYCEY","BAMLC0A2CAAEY","BAMLC0A3CAEY","BAMLH0A1HYBBEY","BAMLEMCBPIEY"]:
    v, dt, err = fred_latest(sid)
    print(f"   {sid:18} {'OK '+str(v)+' @'+str(dt) if v else 'MISSING '+str(err)}")

print("\n=== (3) dedicated ICE BofA engine outputs (do they write S3 feeds?) ===")
for k in ["data/ice-bofa.json","data/bond-indices.json","data/credit-indices.json","data/fred-ice-bofa.json","data/bond-index.json"]:
    try:
        d = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=k)["Body"].read())
        print(f"   ✓ {k} topkeys: {list(d)[:12]}")
    except Exception:
        pass

print("\n=== (4) fiat-peg FX sources ===")
print("FRED FX (pegged currencies):")
for sid in ["DEXHKUS","DEXCHUS","DEXDNUS","DEXUSEU","DEXSIUS"]:
    v, dt, err = fred_latest(sid)
    print(f"   {sid:10} {'OK '+str(v)+' @'+str(dt) if v else 'MISSING'}")
print("Polygon FX (Gulf pegs) — prev close:")
for pair in ["USDSAR","USDAED","USDHKD","USDCNH","USDQAR"]:
    d = jget(f"https://api.polygon.io/v2/aggs/ticker/C:{pair}/prev?apiKey={POLY}")
    res = (d.get("results") or [{}])
    print(f"   {pair}: {'c='+str(res[0].get('c')) if res and res[0].get('c') else 'no data '+str(d.get('_err') or d.get('status') or '')}")
print("\nDONE 2550")
