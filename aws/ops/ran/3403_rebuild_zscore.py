"""ops 3403 — rebuild long-history with PER-HUB Z-SCORING. Raw spread over-weighted chronic
euro-periphery levels (2017 looked like a crisis) and missed dollar crises (2008 barely
registered). Fix: z-score each hub's yield-spread-vs-Bund on its OWN full history, so the
signal is 'how stressed is this hub vs its own normal'. Danger-first composite of z-scores
mapped to 0-100. This makes 2008/2011/2020 spike and calm periods stay calm."""
import json, urllib.request, statistics
from datetime import datetime, timezone
from ops_report import report
import boto3
FRED="2f057499936072679d8843d7fce99989"
s3=boto3.client("s3",region_name="us-east-1")
HUBS={
 "United States":"IRLTLT01USM156N","United Kingdom":"IRLTLT01GBM156N","Germany":"IRLTLT01DEM156N",
 "France":"IRLTLT01FRM156N","Italy":"IRLTLT01ITM156N","Spain":"IRLTLT01ESM156N",
 "Switzerland":"IRLTLT01CHM156N","Netherlands":"IRLTLT01NLM156N","Belgium":"IRLTLT01BEM156N",
 "Ireland":"IRLTLT01IEM156N","Finland":"IRLTLT01FIM156N","Portugal":"IRLTLT01PTM156N",
 "Sweden":"IRLTLT01SEM156N","Japan":"IRLTLT01JPM156N","Canada":"IRLTLT01CAM156N",
 "Australia":"IRLTLT01AUM156N","Greece":"IRLTLT01GRM156N",
}
ANCHOR="Germany"
def fred_monthly(sid):
    url=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED}&file_type=json&observation_start=1985-01-01&frequency=m"
    req=urllib.request.Request(url,headers={"User-Agent":"jh/1.0"})
    with urllib.request.urlopen(req,timeout=30) as r: d=json.loads(r.read().decode())
    return {o["date"][:7]:float(o["value"]) for o in d.get("observations",[]) if o.get("value") not in (".","",None)}
def clamp(v,lo,hi): return max(lo,min(hi,v))
import math
def logistic(z): return 100.0/(1.0+math.exp(-1.0*z))

with report("3403_rebuild_zscore") as r:
    yields={n:fred_monthly(s) for n,s in HUBS.items()}
    anchor=yields[ANCHOR]
    # build each hub's spread-vs-Bund time-series, then z-score on OWN history
    spreads={}   # name -> {month: spread_pp}
    for name,ys in yields.items():
        if name==ANCHOR: continue
        spreads[name]={m:(v-anchor[m]) for m,v in ys.items() if m in anchor}
    # per-hub mean/std for z
    stats={n:(statistics.mean(s.values()),statistics.pstdev(s.values()) or 1e-9) for n,s in spreads.items()}
    months=sorted({m for s in spreads.values() for m in s})
    months=[m for m in months if m>="1990-01"]
    series=[]
    for m in months:
        zs=[]
        for name,s in spreads.items():
            if m in s:
                mu,sd=stats[name]
                zs.append((name,(s[m]-mu)/sd))
        if len(zs)<5: continue
        # z -> 0-100 stress via logistic; danger-first (0.55 avg + 0.45 worst)
        st=[(name,logistic(z)) for name,z in zs]
        avg=sum(v for _,v in st)/len(st)
        wname,wz=max(zs,key=lambda x:x[1])
        worst=logistic(wz)
        comp=round(0.55*avg+0.45*worst,1)
        series.append({"date":m+"-01","stress":comp,"worst_country":wname,"worst_z":round(wz,2),"n":len(zs)})
    r.section("Crisis validation (z-scored)")
    h={p["date"][:7]:p for p in series}
    for label,m in [("2008 Lehman","2008-10"),("2011 eurozone","2011-11"),("2012 Draghi","2012-07"),
                    ("2020 COVID","2020-03"),("2022 gilt","2022-10"),("calm 2005","2005-06"),
                    ("calm 2017","2017-06"),("recent","2026-05")]:
        p=h.get(m); r.log(f"  {label} ({m}): {p['stress'] if p else '—'}"+(f" worst {p['worst_country']} z={p['worst_z']}" if p else ""))
    vals=[p["stress"] for p in series]
    peaks=sorted(series,key=lambda x:-x["stress"])[:6]
    r.log(f"  range {min(vals):.1f}-{max(vals):.1f} median {statistics.median(vals):.1f}")
    r.log("  top peaks (should be real crises):")
    for p in peaks: r.log(f"    {p['date']}: {p['stress']} ({p['worst_country']} z={p['worst_z']})")
    payload={"version":"2.0.0","generated_at":datetime.now(timezone.utc).isoformat(),
             "basis":"Pre-CDS sovereign stress from 10Y yield spreads vs Bund, PER-HUB z-scored on own history (1990→now, monthly). Measures stress-vs-own-normal, danger-first (0.55 pack-avg + 0.45 worst-hub). CDS didn't exist pre-2003.",
             "anchor":ANCHOR,"n_points":len(series),"start":series[0]["date"],"end":series[-1]["date"],"history":series}
    s3.put_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign-longhistory.json",
                  Body=json.dumps(payload,default=str).encode(),ContentType="application/json",CacheControl="max-age=3600, public")
    r.ok(f"z-scored long history written — {len(series)} points {series[0]['date']}→{series[-1]['date']}")
