"""ops 3402 — does the long-history series show the REAL crises (2008, 2011-12 eurozone,
2020, 2022) or is it drowned by pre-euro convergence spreads? Inspect specific crisis months.
If pre-1999 convergence dominates, recalibrate (z-score per hub on own history instead of raw
spread) so the signal measures STRESS not structural pre-euro yield gaps."""
import json, boto3
from ops_report import report
s3=boto3.client("s3",region_name="us-east-1")
with report("3402_check_crises") as r:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign-longhistory.json")["Body"].read())
    h={p["date"][:7]:p for p in d["history"]}
    r.section("Known crisis months — does stress spike?")
    for label,m in [("1992 ERM","1992-09"),("1998 LTCM","1998-10"),("2008 Lehman","2008-10"),
                    ("2011 eurozone","2011-11"),("2012 Draghi","2012-07"),("2020 COVID","2020-03"),
                    ("2022 gilt/UK","2022-10"),("2023 SVB/CS","2023-03"),("calm 2005","2005-06"),
                    ("calm 2017","2017-06"),("recent","2026-05")]:
        p=h.get(m)
        r.log(f"  {label} ({m}): {p['stress'] if p else 'no data'}"+(f" worst {p['worst_country']} +{p['worst_spread_bp']}bp" if p else ""))
    r.section("Verdict")
    r.log("If 2008/2011/2020 DON'T stand out above pre-euro 1990s → recalibrate to per-hub z-score.")
