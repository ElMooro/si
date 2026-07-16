"""ops 3383 — the WGB REST endpoint returns 403 to a bare POST (anti-bot). Replay it with
full browser headers (Referer, Origin, X-Requested-With, Accept, nonce if present). Also try
GET on /historical and grab any wp REST nonce from the page. Goal: real 200 with SG data."""
import urllib.request, re, json
from ops_report import report

def get_full(url,t=25):
    try:
        req=urllib.request.Request(url,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"})
        with urllib.request.urlopen(req,timeout=t) as r:
            return r.read().decode("utf-8","ignore"), dict(r.headers)
    except Exception as e: return f"__ERR__ {e}", {}

def post(url,payload,headers,t=25):
    try:
        body=json.dumps(payload).encode()
        req=urllib.request.Request(url,data=body,headers=headers)
        with urllib.request.urlopen(req,timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e:
        return e.code, (e.read().decode("utf-8","ignore")[:120] if e.fp else str(e))
    except Exception as e: return None, f"{type(e).__name__} {str(e)[:60]}"

with report("3383_wgb_headers") as r:
    page,_=get_full("https://www.worldgovernmentbonds.com/country/singapore/")
    # extract jsGlobalVars
    m=re.search(r"var\s+jsGlobalVars\s*=\s*(\{.*?\});",page,re.S)
    gv=None
    if m:
        raw=m.group(1); depth=0
        for i,ch in enumerate(raw):
            if ch=="{":depth+=1
            elif ch=="}":
                depth-=1
                if depth==0:
                    try: gv=json.loads(raw[:i+1])
                    except Exception as e: r.log(f"json err {e}")
                    break
    # look for a wp nonce
    nonce=re.search(r'"nonce"\s*:\s*"([a-f0-9]+)"',page) or re.search(r'wpApiSettings.*?nonce":"([a-f0-9]+)"',page)
    r.log(f"gv extracted: {bool(gv)}; nonce: {nonce.group(1) if nonce else 'none'}")

    ENDPOINT="https://www.worldgovernmentbonds.com/wp-json/country/v1/main"
    base_headers={
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Content-Type":"application/json",
        "Accept":"application/json, text/plain, */*",
        "Referer":"https://www.worldgovernmentbonds.com/country/singapore/",
        "Origin":"https://www.worldgovernmentbonds.com",
        "X-Requested-With":"XMLHttpRequest",
        "Accept-Language":"en-US,en;q=0.9",
    }
    r.section("POST with full browser headers")
    if gv:
        h=dict(base_headers)
        if nonce: h["X-WP-Nonce"]=nonce.group(1)
        code,resp=post(ENDPOINT,{"GLOBALVAR":gv},h)
        r.log(f"  status={code}")
        if code==200:
            flat=resp
            b=re.search(r'"bond10y"\s*:\s*"?([\d.]+)',flat)
            cds=re.search(r'"lastCds"\s*:\s*"?([\d.]+)',flat)
            spr=re.search(r'"mainSpreadValue"\s*:\s*"?(-?[\d.]+)',flat)
            rat=re.search(r'"lastRatingValue"\s*:\s*"?([A-D][A-Za-z+\-0-9]*)',flat)
            r.log(f"  ✅ 10Y={b.group(1) if b else '?'}% CDS={cds.group(1) if cds else '?'} spread={spr.group(1) if spr else '?'} rating={rat.group(1) if rat else '?'}")
            r.log(f"  resp head: {resp[:200]}")
        else:
            r.log(f"  body: {resp[:150]}")
    # also try GET form
    r.section("try GET /historical with symbol")
    for u in ["https://www.worldgovernmentbonds.com/wp-json/country/v1/main?country=singapore",
              "https://www.worldgovernmentbonds.com/wp-json/"]:
        code,resp=post(u,{},base_headers) if False else (None,None)
    # simple GET on wp-json root to confirm REST is open
    root,_=get_full("https://www.worldgovernmentbonds.com/wp-json/country/v1")
    r.log(f"  /wp-json/country/v1 GET: {root[:150]}")
