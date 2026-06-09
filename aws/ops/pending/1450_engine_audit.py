"""Audit (a): read the LIVE output of every opportunity/ranking/backtest engine
from S3 — see what's real, stale, or empty. From AWS."""
import json, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=120,retries={"max_attempts":1})
s3=boto3.client("s3",region_name="us-east-1",config=cfg)
B="justhodl-dashboard-live"
def peek(key):
    try:
        o=s3.get_object(Bucket=B,Key=key)
        d=json.loads(o["Body"].read())
        age_h=round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)
        # summarize shape
        info={"age_h":age_h}
        if isinstance(d,dict):
            info["keys"]=list(d.keys())[:12]
            for arrk in ["opportunities","top_setups","ranked","results","setups","names","candidates","items","engines","by_signal","by_signal_type"]:
                if arrk in d and isinstance(d[arrk],list): info[arrk+"_count"]=len(d[arrk])
            if "generated_at" in d: info["generated_at"]=str(d["generated_at"])[:19]
            if "hit_rate" in d: info["hit_rate"]=d["hit_rate"]
            if "n_predictions" in d: info["n_predictions"]=d["n_predictions"]
        elif isinstance(d,list): info["list_len"]=len(d)
        return info
    except s3.exceptions.NoSuchKey: return {"MISSING":True}
    except Exception as e: return {"err":str(e)[:60]}
keys={
 "opportunities":"data/opportunities.json",
 "best-setups":"data/best-setups.json",
 "master-ranker":"data/master-ranker.json",
 "backtest-summary":"data/backtest-summary.json",
 "alpha-score":"screener/alpha-score.json",
 "estimate-revisions":"data/estimate-revisions-latest.json",
 "deep-value-overlap":"data/deep-value-overlap.json",
 "backlog":"data/backlog.json",
 "compound-signals":"data/compound-signals.json",
 "asymmetric-scorer":"data/asymmetric-scorer.json",
 "signal-board":"data/signal-board.json",
 "fundamentals":"data/fundamentals.json",
}
out={k:peek(v) for k,v in keys.items()}
open("aws/ops/reports/1450_ea.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
