import urllib.request, json, time
LAMBDA_URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
CDN="https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
COLD="GGG"  # Graco — real mid-cap, unlikely pre-cached
# confirm it's cold first
def get(u,timeout):
    t=time.time()
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
        with urllib.request.urlopen(req,timeout=timeout) as r:
            return r.status, time.time()-t, r.read().decode("utf-8","replace")
    except urllib.error.HTTPError as e:
        return e.code, time.time()-t, e.read(300).decode("utf-8","replace")
    except Exception as e:
        return None, time.time()-t, f"{type(e).__name__}: {str(e)[:80]}"
s,dt,b=get(f"{CDN}/{COLD}.json?v={int(time.time())}",20)
print(f"cold CDN check {COLD}: status={s} {dt:.1f}s cached={'from_cache' in b}")
try:
    pre=json.loads(b); print("   (already cached, from_cache=",pre.get("from_cache"),"gen=",pre.get("generated_at"),")")
    cold=False
except: cold=True
# now FRESH generate via Lambda URL, measure time
print(f"FRESH generate {COLD} via Lambda URL (cold path the user hit)...")
s,dt,b=get(f"{LAMBDA_URL}?ticker={COLD}",170)
print(f"   status={s} time={dt:.1f}s len={len(b)}")
if s==200:
    try:
        d=json.loads(b)
        secs={k:(("populated" if d.get(k) else "EMPTY")) for k in
              ["financials","valuation","bull_case","bear_case","ai_verdict","verdict","summary","thesis","analysis","scorecard","price"]
              if k in d}
        print("   top keys:",list(d.keys())[:20])
        print("   section fill:",json.dumps(secs))
        print("   from_cache:",d.get("from_cache"),"| has error field:",d.get("error"))
    except Exception as e: print("   parse err:",str(e)[:80],"| body[:200]:",b[:200])
print("DONE 2245")
