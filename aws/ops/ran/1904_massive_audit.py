import urllib.request, json, time, subprocess, os
K="zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
def g(u):
    try:
        r=urllib.request.urlopen(urllib.request.Request(u,headers={"User-Agent":"jh-audit"}),timeout=15)
        return r.getcode(), r.read().decode("utf-8","ignore")
    except urllib.error.HTTPError as e: return e.code, e.read().decode("utf-8","ignore")[:160]
    except Exception as e: return 0,str(e)[:120]
def probe(name, path):
    c,b=g("https://api.polygon.io"+path+("&" if "?" in path else "?")+"apiKey="+K)
    ok = c==200 and ('"results"' in b or '"ticker"' in b or '"value"' in b) and '"NOT_AUTHORIZED"' not in b
    note=""
    try:
        j=json.loads(b); 
        if isinstance(j,dict):
            note=j.get("status","") or ""
            if "results" in j: note+=" results=%s"%(len(j["results"]) if isinstance(j["results"],list) else "obj")
            if j.get("message"): note+=" | "+str(j["message"])[:60]
    except: note=b[:70]
    print("  [%s] %-22s HTTP %s  %s"%("UNLOCKED" if ok else "NO/LIMITED",name,c,note.replace(chr(10)," ")[:80]))
    return ok
print("=== MASSIVE (Polygon) KEY — WHAT'S UNLOCKED ===")
print("-- Stocks (paid: Stocks Starter) --")
probe("stock prev","/v2/aggs/ticker/AAPL/prev")
probe("stock 1m bars","/v2/aggs/ticker/AAPL/range/1/day/2026-05-01/2026-06-18?adjusted=true&limit=5")
print("-- Options (paid: Options Starter $29) --")
probe("opt contracts","/v3/reference/options/contracts?underlying_ticker=AAPL&limit=3")
probe("opt chain snapshot","/v3/snapshot/options/AAPL?limit=3")
# get a real contract then aggregate it
c,b=g("https://api.polygon.io/v3/reference/options/contracts?underlying_ticker=AAPL&limit=1&apiKey="+K)
ct=None
try: ct=json.loads(b)["results"][0]["ticker"]
except: pass
if ct: probe("opt contract bars","/v2/aggs/ticker/%s/range/1/day/2026-05-01/2026-06-18?limit=3"%ct)
print("-- Currencies / FX (paid: Currencies Starter $49) --")
probe("fx EURUSD bars","/v2/aggs/ticker/C:EURUSD/range/1/day/2026-05-01/2026-06-18?limit=5")
probe("fx prev USDJPY","/v2/aggs/ticker/C:USDJPY/prev")
probe("fx last quote","/v1/last_quote/currencies/EUR/USD")
print("-- Indices (paid: Indices Basic $0) --")
probe("idx SPX bars","/v2/aggs/ticker/I:SPX/range/1/day/2026-05-01/2026-06-18?limit=5")
probe("idx snapshot","/v3/snapshot/indices?ticker.any_of=I:SPX")
print("-- Futures (paid: Futures Starter $29) --")
probe("fut contracts","/futures/vX/contracts?limit=3")
probe("fut products","/futures/vX/products?limit=3")
probe("fut aggs ES","/v2/aggs/ticker/ES/range/1/day/2026-05-01/2026-06-18?limit=3")
print("\n=== CURRENT POLYGON USAGE IN CODEBASE (what we actually call) ===")
os.chdir(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))  # repo root-ish
try:
    out=subprocess.run("grep -rhoE 'api\\.(polygon|massive)\\.io/[a-zA-Z0-9/:_{}.-]+' aws/lambdas --include=*.py | sed -E 's/[0-9]{4}-[0-9]{2}-[0-9]{2}/{date}/g' | sort | uniq -c | sort -rn | head -25",shell=True,capture_output=True,text=True)
    print(out.stdout or "(none)")
    n=subprocess.run("grep -rlE 'api\\.(polygon|massive)\\.io' aws/lambdas --include=*.py | wc -l",shell=True,capture_output=True,text=True)
    print("lambdas referencing polygon/massive:",n.stdout.strip())
    print("\n-- endpoint CLASSES used --")
    for cls,pat in [("stock aggs","/v2/aggs/ticker/[A-Z]"),("FX (C:)","C:"),("options (O:)","O:|/snapshot/options|/reference/options"),("indices (I:)","I:|/snapshot/indices"),("futures","/futures/")]:
        r=subprocess.run("grep -rlE '%s' aws/lambdas --include=*.py | wc -l"%pat,shell=True,capture_output=True,text=True)
        print("   %-16s used in %s lambdas"%(cls,r.stdout.strip()))
except Exception as e: print("grep err",e)
print("\n=== EURODOLLAR / DOLLAR-SHORTAGE ENGINES — do they use FX? ===")
r=subprocess.run("for d in justhodl-eurodollar-plumbing justhodl-eurodollar-stress justhodl-sovereign-fiscal; do echo -n \"$d: \"; grep -lE 'C:|polygon|massive|EURUSD|DXY|USDJPY' aws/lambdas/$d/source/*.py 2>/dev/null && echo 'uses-fx' || echo 'NO direct FX (FRED proxies only)'; done",shell=True,capture_output=True,text=True)
print(r.stdout)
