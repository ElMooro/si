"""1985 — freshness audit of all earnings/analyst/revision/PEAD/recon feeds to
decide overlap reconciliation + where Benzinga/Constituents should be wired."""
import boto3, datetime
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
now=datetime.datetime.now(datetime.timezone.utc)
feeds=["data/benzinga-earnings.json","data/benzinga-earnings-calendar.json",
 "data/earnings-tracker.json","data/earnings-pead.json","data/pead-signals.json",
 "data/eps-revision-velocity.json","data/estimate-revisions.json",
 "data/analyst-consensus.json","data/analyst-consensus-history.json",
 "data/ai-rerating-radar.json","data/rating-change-cluster.json","data/analyst-actions.json",
 "data/starmine.json","data/earnings-whisper.json","data/flow-lookthrough.json",
 "data/index-recon.json","data/index-inclusion.json","data/russell-recon-frontrun.json",
 "data/catalyst-calendar.json","data/conviction.json","data/signal-board.json",
 "data/best-setups.json","data/master-ranker.json","data/dossier.json"]
print(f"{'feed':<42} {'age_h':>7}  {'KB':>6}  status")
for k in feeds:
    try:
        h=s3.head_object(Bucket=B,Key=k)
        age=(now-h["LastModified"]).total_seconds()/3600
        kb=h["ContentLength"]/1024
        tag="LIVE" if age<48 else "STALE" if age<24*14 else "DEAD"
        print(f"{k:<42} {age:>7.1f}  {kb:>6.0f}  {tag}")
    except Exception as e:
        print(f"{k:<42} {'—':>7}  {'—':>6}  MISSING ({type(e).__name__})")
print("DONE 1985")
