"""ops 3401 — build the LONG-HISTORY eurodollar-hub stress series back to 1990 from FRED 10Y
govt bond yields. Pre-CDS stress = each hub's yield SPREAD vs the German Bund anchor (the
market's pre-CDS sovereign-risk pricing), z-scored/mapped to 0-100 danger-first (pack avg +
worst hub), monthly. Writes data/global-sovereign-longhistory.json for the chart's spine.
This runs the construction ONCE here to seed it; a scheduled engine will maintain it."""
import json, urllib.request, statistics
from datetime import datetime, timezone
from ops_report import report
import boto3

FRED="2f057499936072679d8843d7fce99989"
s3=boto3.client("s3",region_name="us-east-1")

# DM hubs with long FRED yield history (slug -> series). Anchor = Germany (Bund).
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
    with urllib.request.urlopen(req,timeout=30) as r:
        d=json.loads(r.read().decode())
    out={}
    for o in d.get("observations",[]):
        v=o.get("value")
        if v not in (".","",None):
            out[o["date"][:7]]=float(v)   # YYYY-MM -> yield
    return out

def clamp(v,lo,hi): return max(lo,min(hi,v))

with report("3401_build_hub_history") as r:
    r.section("Fetch FRED yields for all long-history hubs")
    yields={}
    for name,sid in HUBS.items():
        try:
            yields[name]=fred_monthly(sid)
            r.log(f"  {name}: {len(yields[name])} monthly obs")
        except Exception as e:
            r.log(f"  {name}: ERR {str(e)[:40]}")
    anchor=yields.get(ANCHOR,{})
    if not anchor:
        r.fail("no anchor (Germany) data"); raise SystemExit(0)

    # build monthly spread-stress series from 1990
    r.section("Construct danger-first spread-stress series 1990→now")
    months=sorted({m for y in yields.values() for m in y} )
    months=[m for m in months if m>="1990-01"]
    series=[]
    for m in months:
        anc=anchor.get(m)
        if anc is None: continue
        spreads=[]
        for name,ys in yields.items():
            if name==ANCHOR: continue
            v=ys.get(m)
            if v is not None:
                spreads.append((name, v-anc))   # yield spread over Bund (pp)
        if len(spreads)<5: continue
        # spread(pp) -> stress 0-100. 0bp≈15, +150bp≈55, +400bp≈90 (danger-first: worst hub)
        def spr_to_stress(pp):
            bp=pp*100.0
            return clamp(15.0 + bp/400.0*75.0, 0, 100)
        st=[spr_to_stress(pp) for _,pp in spreads]
        avg=sum(st)/len(st)
        wname,wpp=max(spreads,key=lambda x:x[1])
        worst=spr_to_stress(wpp)
        comp=round(0.6*avg+0.4*worst,1)
        series.append({"date":m+"-01","stress":comp,"worst_country":wname,
                       "worst_spread_bp":round(wpp*100,1),"n":len(spreads),"basis":"yield-spread"})
    r.log(f"  built {len(series)} monthly points, {series[0]['date']} → {series[-1]['date']}")
    # sanity: crisis peaks
    peaks=sorted(series,key=lambda x:-x["stress"])[:6]
    r.log("  highest-stress months (should be crises):")
    for p in peaks:
        r.log(f"    {p['date']}: {p['stress']} (worst {p['worst_country']} +{p['worst_spread_bp']}bp)")
    # percentile of the current live reading vs this history
    vals=[p["stress"] for p in series]
    r.log(f"  history stress range: {min(vals):.1f} – {max(vals):.1f}, median {statistics.median(vals):.1f}")

    payload={"version":"1.0.0","generated_at":datetime.now(timezone.utc).isoformat(),
             "basis":"Pre-CDS sovereign stress from 10Y govt bond yield spreads vs German Bund (FRED, monthly). CDS didn't exist before ~2003; this is the market's pre-CDS sovereign-risk pricing, danger-first (0.6 pack-avg + 0.4 worst hub).",
             "anchor":ANCHOR,"n_points":len(series),"start":series[0]["date"],"end":series[-1]["date"],
             "history":series}
    s3.put_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign-longhistory.json",
                  Body=json.dumps(payload,default=str).encode(),ContentType="application/json",
                  CacheControl="max-age=3600, public")
    r.ok(f"LONG HISTORY written — {len(series)} monthly points back to {series[0]['date']}.")
