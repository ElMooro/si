"""ops 3407 — add YoY %-change (now / 12mo-ago − 1)×100 to every point in the long-history
file, so the chart can plot the YoY-percent trend (clearly shows reversals via zero-crossing).
Reads the existing longhistory, computes yoy_pct per month, writes back."""
import json, boto3
from ops_report import report
s3=boto3.client("s3",region_name="us-east-1")
KEY="data/global-sovereign-longhistory.json"
with report("3407_add_yoy_to_longhistory") as r:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=KEY)["Body"].read())
    h=d["history"]
    # index by YYYY-MM for 12-month lookback
    by_month={p["date"][:7]:p["stress"] for p in h}
    def minus12(ym):
        y,m=int(ym[:4]),int(ym[5:7]); y-=1
        return f"{y:04d}-{m:02d}"
    n_yoy=0
    for p in h:
        ym=p["date"][:7]; prev=by_month.get(minus12(ym))
        if prev not in (None,0):
            p["yoy_pct"]=round(100.0*(p["stress"]/prev-1.0),1)
            n_yoy+=1
        else:
            p["yoy_pct"]=None
    d["has_yoy_pct"]=True
    d["yoy_note"]="yoy_pct = (stress / stress 12 months ago − 1) × 100. Zero-crossing = trend reversal."
    s3.put_object(Bucket="justhodl-dashboard-live",Key=KEY,
                  Body=json.dumps(d,default=str).encode(),ContentType="application/json",
                  CacheControl="max-age=3600, public")
    # spot-check the crisis moments
    idx={p["date"][:7]:p for p in h}
    r.section("YoY% at key moments")
    for lbl,m in [("2008-10 Lehman","2008-10"),("2009-06 recovery","2009-06"),
                  ("2011-11 euro","2011-11"),("2013-06 post","2013-06"),("2026-05 now","2026-05")]:
        p=idx.get(m); r.log(f"  {lbl}: level={p['stress'] if p else '—'} yoy_pct={p.get('yoy_pct') if p else '—'}")
    vals=[p["yoy_pct"] for p in h if p.get("yoy_pct") is not None]
    r.ok(f"yoy_pct added to {n_yoy}/{len(h)} points. range {min(vals):.0f}% to {max(vals):.0f}%")
