import urllib.request, json
UA={"User-Agent":"JustHodl Research raafouis@gmail.com"}
def get(url,t=45):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers=UA),timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return None
NY="https://markets.newyorkfed.org/api/pd"
print("=== PD seriesbreaks ===")
sb=get(NY+"/list/seriesbreaks.json")
try:
    j=json.loads(sb); arr=j.get("pd",{}).get("seriesbreaks") or j.get("seriesbreaks") or []
    for x in arr: print("  ",x)
except Exception as e: print("  parse:",e, str(sb)[:120])
print("\n=== fails keyids grouped by seriesbreak (headline UST/corp) ===")
lst=json.loads(get(NY+"/list/timeseries.json"))["pd"]["timeseries"]
for x in lst:
    if x["keyid"] in ("PDFTD-USTET","PDFTR-USTET","PDFTD-CS","PDFTR-CS") or (x["keyid"].startswith("PDFTD-UST") and len(x["keyid"])<14):
        print("  ",x["keyid"],"break=",x.get("seriesbreak"))
# try older-break stitch: does /get accept all history? check full range + try a date param
print("\n=== PDFTD-USTET full range probe ===")
for url in [NY+"/get/PDFTD-USTET.json", NY+"/get/PDFTD-USTET.json?startPeriod=1996-01-01"]:
    b=get(url)
    try:
        ts=json.loads(b)["pd"]["timeseries"]; print(f"  {url.split('?')[-1] if '?' in url else 'default'}: n={len(ts)} {ts[0]['asofdate']}..{ts[-1]['asofdate']}")
    except Exception as e: print("  err",e)
print("\n=== FRED earliest date for representative plumbing indicators ===")
F="2f057499936072679d8843d7fce99989"
for sid in ["SWPT","DTWEXBGS","RRPONTSYD","WALCL","BAMLH0A0HYM2","DRISCFLM","TEMPHELPS","STLFSI4","SOFR","WLODLL"]:
    b=get(f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={F}&file_type=json&observation_start=1990-01-01&limit=1&sort_order=asc")
    try:
        o=json.loads(b)["observations"]; print(f"  {sid:14} earliest {o[0]['date']}")
    except: print(f"  {sid:14} FAIL/none")
