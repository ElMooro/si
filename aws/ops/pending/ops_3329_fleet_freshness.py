"""ops 3329 — FAST read-only freshness check of the 6 FMP-migrated engines'
outputs (no invokes — just S3 head + a data-presence probe). Confirms the
fix by whether each feed is fresh + non-trivial. Some engines run on
schedules, so also report age so we can tell 'fixed & ran' from 'awaiting
next scheduled run'."""
import json, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report
S3=boto3.client("s3","us-east-1")
LAM=boto3.client("lambda","us-east-1")
BUCKET="justhodl-dashboard-live"
FEEDS={
  "rating-change-cluster":"data/rating-change-cluster.json",
  "sellside-views":"data/sellside-views.json",
  "52wk-quality-breakout":"data/52wk-quality-breakout.json",
  "starmine":"data/starmine.json",
  "buyback-scanner":"data/buyback-scanner.json",
  "insider-sell-cluster":"data/insider-sell-cluster.json",
}
def probe(key):
    try:
        o=S3.get_object(Bucket=BUCKET,Key=key)
        raw=o["Body"].read()
        d=json.loads(raw)
        gen=d.get("generated_at") or d.get("timestamp") or d.get("as_of")
        # count any list payloads to gauge non-empty
        counts={k:len(v) for k,v in d.items() if isinstance(v,list)}
        top=sorted(counts.items(),key=lambda x:-x[1])[:3]
        age=None
        if gen:
            try: age=int((datetime.now(timezone.utc)-datetime.fromisoformat(str(gen).replace("Z","+00:00"))).total_seconds())
            except Exception: pass
        return {"size":len(raw),"generated_at":gen,"age_s":age,"top_lists":top}
    except Exception as e:
        return {"err":type(e).__name__}
with report("3329_fleet_freshness") as rep:
    for name,key in FEEDS.items():
        rep.kv(**{name:probe(key)})
    rep.ok("freshness snapshot complete — cross-ref age vs each engine's schedule")
    rep.kv(RESULT="DONE")
