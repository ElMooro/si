import urllib.request, json, time
BASE="https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
def get(u,t=20):
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"Mozilla/5.0 jh","Origin":"https://justhodl.ai"})
        with urllib.request.urlopen(req,timeout=t) as r: return r.status, r.read().decode("utf-8","replace")
    except urllib.error.HTTPError as e: return e.code, e.read(200).decode("utf-8","replace")
    except Exception as e: return None, f"{type(e).__name__}:{str(e)[:60]}"

print("1) nogen read of WARM ticker (BMNR) — expect 200 + doc, fast")
t=time.time(); s,b=get(f"{BASE}/BMNR.json?nogen=1&v={int(time.time())}")
ok = s==200 and b.strip().startswith("{") and '"generated_at"' in b
print(f"   status={s} {time.time()-t:.1f}s hasDoc={ok}")

print("2) nogen read of MISSING ticker (ZZTOP9) — expect fast 404/403, NO generation")
t=time.time(); s,b=get(f"{BASE}/ZZTOP9.json?nogen=1&v={int(time.time())}")
print(f"   status={s} {time.time()-t:.1f}s body={b[:80]}")

print("3) async trigger of a cold-ish ticker (PLTR) — expect FAST 202 'generating'")
t=time.time(); s,b=get(f"{BASE}/PLTR.json?async=1&v={int(time.time())}")
print(f"   status={s} {time.time()-t:.1f}s body={b[:100]}")

print("4) immediate nogen poll of PLTR right after trigger — likely 404 first, will fill in")
t=time.time(); s,b=get(f"{BASE}/PLTR.json?nogen=1&v={int(time.time())}")
print(f"   status={s} {time.time()-t:.1f}s {'has doc' if '\"generated_at\"' in b else 'not yet'}")
print("DONE 2284")
