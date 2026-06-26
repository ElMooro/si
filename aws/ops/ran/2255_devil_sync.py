import json, time, urllib.request
URL="https://6nkrwmk2ntjx54okqvtzokosb40whvfb.lambda-url.us-east-1.on.aws/"
t=time.time()
req=urllib.request.Request(f"{URL}?ticker=LDOS&refresh=1",headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
try:
    with urllib.request.urlopen(req,timeout=260) as r: b=r.read().decode()
    print(f"generated in {time.time()-t:.0f}s, {len(b)} bytes")
    d=json.loads(b)
    # handle possible API-GW wrapper
    if "body" in d and isinstance(d["body"],str):
        try: d=json.loads(d["body"])
        except: pass
    da=d.get("devils_advocate"); v=d.get("verdict") or {}
    print("verdict:", v.get("rating"), v.get("conviction_grade"))
    print("devils_advocate present:", da is not None)
    if da:
        print("  title:", da.get("title"))
        print("  short_thesis:", str(da.get("short_thesis"))[:400])
        print("  kill_points:")
        for k in (da.get("kill_points") or []): print("     -", k.get("point"), "::", k.get("evidence"))
        print("  what_bulls_underestimate:", str(da.get("what_bulls_underestimate"))[:200])
    else:
        print("  ABSENT — model not emitting; will hoist into prompt")
except Exception as e:
    print(f"sync invoke err after {time.time()-t:.0f}s: {type(e).__name__} {str(e)[:60]}")
print("DONE 2255")
