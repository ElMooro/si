import urllib.request, json, time
CDN="https://justhodl-data-proxy.raafouis.workers.dev/equity-research"
def get(u,timeout):
    t=time.time()
    try:
        req=urllib.request.Request(u,headers={"User-Agent":"jh","Origin":"https://justhodl.ai"})
        with urllib.request.urlopen(req,timeout=timeout) as r:
            return r.status, time.time()-t, r.read().decode("utf-8","replace"), r.headers.get("access-control-allow-origin")
    except urllib.error.HTTPError as e:
        return e.code, time.time()-t, e.read(200).decode("utf-8","replace"), e.headers.get("access-control-allow-origin")
    except Exception as e:
        return None, time.time()-t, f"{type(e).__name__}: {str(e)[:70]}", None
# fresh cold tickers that should NOT be pre-cached — now must succeed via proxy (worker->Lambda)
for tk in ["WMS","RPM","AOS"]:
    s,dt,b,acao=get(f"{CDN}/{tk}.json?v={int(time.time())}",60)
    ok=False; gen=None; verdict=None
    if s==200:
        try:
            d=json.loads(b); ok=True; gen=d.get("generated_at"); 
            v=d.get("verdict") or {}; verdict=v.get("rating") if isinstance(v,dict) else None
        except: pass
    print(f"proxy {tk}.json -> status={s} {dt:.1f}s ACAO={acao!r} json_ok={ok} verdict={verdict} gen={gen}")
print("DONE 2246")
