import urllib.request
def get(url,t=40):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"JustHodl raafouis@gmail.com"}),timeout=t) as r:
            return r.status, r.read().decode("utf-8","ignore")
    except Exception as e: return getattr(e,'code',type(e).__name__), str(e)[:60]
print("=== TASK 2: every euro-area (U2) CISS series + its LATEST date (find fresh sub-indices) ===")
st,body=get("https://data-api.ecb.europa.eu/service/data/CISS?format=csvdata&lastNObservations=1")
rows={}
if st==200:
    lines=body.splitlines(); hdr=lines[0].split(",")
    ki=hdr.index("KEY"); ti=hdr.index("TIME_PERIOD"); vi=hdr.index("OBS_VALUE")
    for ln in lines[1:]:
        c=ln.split(",")
        if len(c)>max(ki,ti,vi) and ".U2." in c[ki]:   # euro-area only
            rows[c[ki]]=(c[ti],c[vi])
    # group: composite (.IDX) vs sub-index contribution (.CON) vs others, show freshness
    fresh=sorted([(k,d,v) for k,(d,v) in rows.items() if d>="2026-01-01"])
    stale=sorted([(k,d,v) for k,(d,v) in rows.items() if d<"2026-01-01"])
    print(f"\nFRESH U2 series (latest >= 2026-01-01) — {len(fresh)}:")
    for k,d,v in fresh: print(f"   {k:42} {d}  {v}")
    print(f"\nSTALE U2 series (latest < 2026-01-01) — {len(stale)}:")
    for k,d,v in stale: print(f"   {k:42} {d}  {v}")
else:
    print("  flow fetch failed:", st, body[:80])
