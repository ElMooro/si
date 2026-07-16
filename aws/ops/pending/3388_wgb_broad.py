"""ops 3388 — confirm the WGB endpoint works across a BROAD set of world sovereigns (the
dedicated engine's universe). Test ~40 major countries; report which return live data so we
finalize the engine's country list."""
import urllib.request, re, json
from ops_report import report
UA="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
ENDPOINT="https://www.worldgovernmentbonds.com/wp-json/country/v1/main"
def get(url,t=20):
    req=urllib.request.Request(url,headers={"User-Agent":UA})
    with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
def fetch(slug):
    try:
        page=get(f"https://www.worldgovernmentbonds.com/country/{slug}/")
    except Exception: return None
    m=re.search(r"var\s+jsGlobalVars\s*=\s*(\{.*?\});",page,re.S)
    if not m: return None
    raw=m.group(1); depth=0; gv=None
    for i,ch in enumerate(raw):
        if ch=="{":depth+=1
        elif ch=="}":
            depth-=1
            if depth==0:
                try: gv=json.loads(raw[:i+1])
                except: return None
                break
    if not gv: return None
    body=json.dumps({"GLOBALVAR":gv}).encode()
    req=urllib.request.Request(ENDPOINT,data=body,headers={"User-Agent":UA,"Content-Type":"application/json",
        "Accept":"application/json","Referer":f"https://www.worldgovernmentbonds.com/country/{slug}/",
        "Origin":"https://www.worldgovernmentbonds.com","X-Requested-With":"XMLHttpRequest"})
    try:
        with urllib.request.urlopen(req,timeout=20) as r: d=json.loads(r.read().decode("utf-8","ignore"))
    except Exception: return None
    if not d.get("success"): return None
    def n(k):
        v=d.get(k)
        try: return float(v) if v not in (None,"","----") else None
        except: return None
    return {"y":n("bond10y"),"cds":n("lastCds"),"spr":n("mainSpreadValue"),"rat":d.get("lastRatingValue")}

SLUGS=["united-states","germany","france","italy","spain","united-kingdom","japan","china",
"canada","australia","switzerland","netherlands","belgium","austria","portugal","greece",
"ireland","finland","sweden","norway","denmark","poland","czech-republic","hungary","russia",
"brazil","mexico","chile","colombia","peru","india","indonesia","malaysia","thailand",
"philippines","vietnam","south-korea","singapore","hong-kong","taiwan","south-africa",
"turkey","israel","saudi-arabia","new-zealand"]
with report("3388_wgb_broad") as r:
    ok=[]; fail=[]
    for s in SLUGS:
        d=fetch(s)
        if d and d["y"] is not None:
            ok.append(s)
            r.log(f"  ✓ {s}: 10Y={d['y']}% CDS={d['cds']} spread={d['spr']} {d['rat']}")
        else:
            fail.append(s)
    r.section(f"WORKS: {len(ok)}/{len(SLUGS)}")
    r.log(f"  failed: {fail}")
