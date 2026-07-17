"""ops 3422 — quality census: stale engine-feeds (real ones, categorized) +
scorecard bottom alpha families + top pages fetching STALE feeds (broken
sections users see)."""
import json, re, sys
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1")
def rj(k):
    try: return json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception: return {}
with report("3422_quality_census") as rep:
    rep.heading("ops 3422 — quality census")
    fr=rj("data/feed-registry.json")
    stale=[x for x in (fr.get("stale") or fr.get("feeds") or []) if x.get("stale")]
    junk_pat=re.compile(r"user|feedback|history-api|_test|tmp|secretary")
    eng_stale=sorted([x for x in stale if not junk_pat.search(x["key"])],key=lambda x:-x["age_h"])
    line1=f"stale engine feeds: {len(eng_stale)} of {len(stale)} stale · worst: "+", ".join(f"{x['key'].replace('data/','')}({int(x['age_h']//24)}d)" for x in eng_stale[:14])
    print(line1); rep.log(line1)
    sc=rj("data/signal-scorecard.json")
    rows=sc.get("scorecard") or sc.get("rows") or []
    if isinstance(rows,dict): rows=[dict(v,signal_type=k) for k,v in rows.items()]
    bad=[r for r in rows if isinstance(r.get("info_ratio"),(int,float)) and (r.get("n_scored") or r.get("n") or 0)>=20 and r["info_ratio"]<0]
    bad.sort(key=lambda r:r["info_ratio"])
    line2=f"negative-alpha families (n>=20): {len(bad)} · worst: "+", ".join(f"{r['signal_type']}(IR {r['info_ratio']:.2f},n{r.get('n_scored') or r.get('n')})" for r in bad[:12])
    print(line2); rep.log(line2)
    Path("aws/ops/reports/3422.json").write_text(json.dumps({
        "stale_engine_feeds":[{k:v for k,v in x.items() if k in ("key","age_h")} for x in eng_stale[:40]],
        "negative_alpha":[{"t":r["signal_type"],"ir":r["info_ratio"],"n":r.get("n_scored") or r.get("n"),"hit":r.get("alpha_hit_rate") or r.get("hit_rate")} for r in bad[:25]]},indent=2))
    sys.exit(0)
