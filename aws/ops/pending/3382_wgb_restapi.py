"""ops 3382 — THE reverse-engineered endpoint. WGB serves data via WP REST POST to
/wp-json/country/v1/main with body {"GLOBALVAR": jsGlobalVars}. Extract the full jsGlobalVars
from each country page and POST it to get bond10y/lastCds/rating. Test SG/HK/TW/KR."""
import urllib.request, re, json
from ops_report import report

def get_text(url,t=25):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

def post_json(url,payload,t=25):
    try:
        body=json.dumps(payload).encode()
        req=urllib.request.Request(url,data=body,headers={
            "User-Agent":"Mozilla/5.0 (compatible; justhodl/1.0)",
            "Content-Type":"application/json","Accept":"application/json"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.read().decode("utf-8","ignore")
    except Exception as e: return f"__ERR__ {e}"

def extract_globalvars(country_slug):
    txt=get_text(f"https://www.worldgovernmentbonds.com/country/{country_slug}/")
    m=re.search(r"var\s+jsGlobalVars\s*=\s*(\{.*?\});",txt,re.S)
    if not m: return None
    raw=m.group(1)
    # trim to valid JSON (it ends at the matching brace before ;)
    try:
        return json.loads(raw)
    except Exception:
        # try progressively — find balanced braces
        depth=0
        for i,ch in enumerate(raw):
            if ch=="{":depth+=1
            elif ch=="}":
                depth-=1
                if depth==0:
                    try: return json.loads(raw[:i+1])
                    except: return None
    return None

with report("3382_wgb_restapi") as r:
    ENDPOINT="https://www.worldgovernmentbonds.com/wp-json/country/v1/main"
    for name,slug in [("Singapore","singapore"),("Hong Kong","hong-kong"),
                      ("Taiwan","taiwan"),("South Korea","south-korea")]:
        gv=extract_globalvars(slug)
        if not gv:
            r.log(f"  {name}: could not extract jsGlobalVars"); continue
        c1=gv.get("COUNTRY1",{})
        r.log(f"  {name}: symbol={c1.get('SYMBOL')} flag={c1.get('BANDIERA')} endpoint={gv.get('ENDPOINT','')[:50]}")
        resp=post_json(ENDPOINT,{"GLOBALVAR":gv})
        if resp.startswith("__ERR__"):
            r.log(f"    POST: {resp[:70]}"); continue
        # parse the returned JSON for the values
        try:
            d=json.loads(resp)
            # dig for bond10y / lastCds anywhere
            flat=json.dumps(d)
            b=re.search(r'"bond10y"\s*:\s*"?([\d.]+)',flat)
            cds=re.search(r'"lastCds"\s*:\s*"?([\d.]+)',flat)
            spr=re.search(r'"mainSpreadValue"\s*:\s*"?(-?[\d.]+)',flat)
            rat=re.search(r'"lastRatingValue"\s*:\s*"?([A-D][A-Za-z+\-0-9]*)',flat)
            cb=re.search(r'"cbRateNumber"\s*:\s*"?([\d.]+)',flat)
            r.log(f"    ✓ RESULT: 10Y={b.group(1) if b else '?'}% CDS={cds.group(1) if cds else '?'} spread={spr.group(1) if spr else '?'} rating={rat.group(1) if rat else '?'} cbrate={cb.group(1) if cb else '?'}")
            r.log(f"    (resp keys: {list(d.keys())[:10] if isinstance(d,dict) else type(d).__name__}, len={len(resp)})")
        except Exception as e:
            r.log(f"    parse: {type(e).__name__}; raw head: {resp[:150]}")
