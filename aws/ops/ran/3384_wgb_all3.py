"""ops 3384 — confirm the reverse-engineered WGB endpoint returns LIVE data for all three
target sovereigns (SG/HK/TW) before wiring into production. This is the exact fetch logic
the engine will use."""
import urllib.request, re, json
from ops_report import report

UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
ENDPOINT="https://www.worldgovernmentbonds.com/wp-json/country/v1/main"

def get(url):
    req=urllib.request.Request(url,headers={"User-Agent":UA})
    with urllib.request.urlopen(req,timeout=25) as r: return r.read().decode("utf-8","ignore")

def extract_gv(slug):
    page=get(f"https://www.worldgovernmentbonds.com/country/{slug}/")
    m=re.search(r"var\s+jsGlobalVars\s*=\s*(\{.*?\});",page,re.S)
    if not m: return None
    raw=m.group(1); depth=0
    for i,ch in enumerate(raw):
        if ch=="{":depth+=1
        elif ch=="}":
            depth-=1
            if depth==0:
                try: return json.loads(raw[:i+1])
                except: return None
    return None

def fetch_country(slug):
    gv=extract_gv(slug)
    if not gv: return {"error":"no globalvars"}
    body=json.dumps({"GLOBALVAR":gv}).encode()
    req=urllib.request.Request(ENDPOINT,data=body,headers={
        "User-Agent":UA,"Content-Type":"application/json",
        "Accept":"application/json, text/plain, */*",
        "Referer":f"https://www.worldgovernmentbonds.com/country/{slug}/",
        "Origin":"https://www.worldgovernmentbonds.com","X-Requested-With":"XMLHttpRequest"})
    with urllib.request.urlopen(req,timeout=25) as r:
        d=json.loads(r.read().decode("utf-8","ignore"))
    def f(k,cast=float):
        v=d.get(k)
        try: return cast(v) if v not in (None,"","----") else None
        except: return None
    return {
        "bond10y_pct": f("bond10y"),
        "cds_bp": f("lastCds"),
        "cds_default_prob": d.get("lastCdsDefaultProb"),
        "spread_vs_bund_bp": f("mainSpreadValue"),
        "rating": d.get("lastRatingValue"),
        "cb_rate_pct": f("cbRateNumber"),
        "as_of": d.get("lastDataValDesc"),
        "ok": bool(d.get("success")),
    }

with report("3384_wgb_all3") as r:
    r.section("Live WGB data — all target sovereigns")
    for name,slug in [("Singapore","singapore"),("Hong Kong","hong-kong"),
                      ("Taiwan","taiwan"),("South Korea","south-korea"),
                      ("Germany","germany")]:
        try:
            d=fetch_country(slug)
            r.log(f"  {name}: 10Y={d.get('bond10y_pct')}% CDS={d.get('cds_bp')}bp spread={d.get('spread_vs_bund_bp')}bp rating={d.get('rating')} cb={d.get('cb_rate_pct')}% asof={d.get('as_of')}")
        except Exception as e:
            r.log(f"  {name}: ERROR {type(e).__name__} {str(e)[:60]}")
    r.section("Verdict")
    r.log("If SG/HK/TW all return live 10Y+CDS → wire into sovereign-stress as REAL data, replacing data_unavailable.")
