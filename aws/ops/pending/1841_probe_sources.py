import urllib.request, json
def get(url, t=25):
    req=urllib.request.Request(url, headers={"User-Agent":"Mozilla/5.0 justhodl-probe"})
    return urllib.request.urlopen(req, timeout=t).read().decode("utf-8","ignore")
FRED="2f057499936072679d8843d7fce99989"

print("=== 1) FRED Net Due to Related Foreign Offices NDFACBW027SBOG ===")
try:
    d=json.loads(get("https://api.stlouisfed.org/fred/series/observations?series_id=NDFACBW027SBOG&api_key=%s&file_type=json&sort_order=desc&limit=3"%FRED))
    print("  latest:", [(o["date"],o["value"]) for o in d["observations"]])
    u=json.loads(get("https://api.stlouisfed.org/fred/series?series_id=NDFACBW027SBOG&api_key=%s&file_type=json"%FRED))
    print("  units:", u["seriess"][0]["units"], "| freq:", u["seriess"][0]["frequency"])
except Exception as e: print("  ERR",e)

print("=== 2) FRED search: H.4.1 repo (FIMA foreign-official + Fed repo asset) ===")
for q in ["repurchase agreements foreign official","assets repurchase agreements wednesday"]:
    try:
        d=json.loads(get("https://api.stlouisfed.org/fred/series/search?search_text=%s&api_key=%s&file_type=json&limit=6&order_by=popularity"%(urllib.parse.quote(q),FRED)))
        print("  q=%r:"%q)
        for s in d.get("seriess",[]): print("    %s | %s | %s"%(s["id"],s["frequency"],s["title"][:70]))
    except Exception as e: print("  ERR",e)
import urllib.parse

print("=== 3) OFR FSI endpoint discovery ===")
for url in ["https://www.financialresearch.gov/financial-stress-index/data/fsi.csv",
            "https://www.financialresearch.gov/financial-stress-index/data/fsi.json"]:
    try:
        r=get(url); print("  OK %s -> first120: %s"%(url, r[:120].replace(chr(10)," ")))
    except Exception as e: print("  MISS %s -> %s"%(url, str(e)[:60]))
try:
    r=get("https://data.financialresearch.gov/v1/metadata/search?query=*tress*")
    print("  OFR API stress search first200:", r[:200])
except Exception as e: print("  OFR API search ERR", str(e)[:80])
