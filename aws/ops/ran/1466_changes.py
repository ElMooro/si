"""Find existing change/alert data for the Why-Now feed. From AWS."""
import json, boto3
from datetime import datetime, timezone
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
B="justhodl-dashboard-live"
def peek(k):
    try:
        o=s3.get_object(Bucket=B,Key=k); d=json.loads(o["Body"].read())
        info={"age_h":round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)}
        if isinstance(d,dict):
            info["keys"]=list(d.keys())[:8]
            for a in ["changes","new_alerts","alerts","new_setups","events","diffs"]:
                if a in d and isinstance(d[a],list): info[a+"_n"]=len(d[a]); info[a+"_sample"]=d[a][:2]
        return info
    except Exception as e: return {"MISSING":str(e)[:30]}
# engines that emit change/alert lists
keys={
 "best-setups":"data/best-setups.json",       # has 'changes'
 "opportunities":"data/opportunities.json",     # has 'changes'
 "compound-signals":"data/compound-signals.json", # has 'new_alerts'
 "master-ranker":"data/master-ranker.json",     # has 'alerts'
 "signal-board":"data/signal-board.json",
 "funding-plumbing":"data/funding-plumbing.json",
 "crypto-cycle-risk":"data/crypto-cycle-risk.json",
 "bond-vol":"data/bond-vol.json",
 "redflag-alerts":"data/redflag-alerts.json",
 "catalyst-calendar":"data/catalyst-calendar.json",
}
out={k:peek(v) for k,v in keys.items()}
open("aws/ops/reports/1466_ch.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
