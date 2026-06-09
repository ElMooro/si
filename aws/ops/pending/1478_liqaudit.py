"""Verify which of the audit's liquidity/plumbing/carry gaps ACTUALLY exist.
Read the live data each engine produces. From AWS."""
import json, boto3, re
from datetime import datetime, timezone
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
B="justhodl-dashboard-live"
def peek(k):
    try:
        o=s3.get_object(Bucket=B,Key=k); d=json.loads(o["Body"].read())
        age=round((datetime.now(timezone.utc)-o["LastModified"]).total_seconds()/3600,1)
        keys=list(d.keys())[:18] if isinstance(d,dict) else f"list[{len(d)}]"
        return {"age_h":age,"keys":keys}
    except Exception as e: return {"MISSING":str(e)[:30]}
# probe the engines the audit said are "missing" — find their output files
files={
 "global-liquidity":"data/global-liquidity.json",
 "china-liquidity":"data/china-liquidity.json",
 "eurodollar-stress":"data/eurodollar-stress.json",
 "funding-plumbing":"data/funding-plumbing.json",
 "plumbing-aggregator":"data/plumbing.json",
 "repo-monitor":"data/repo.json",
 "carry-surface":"data/carry-surface.json",
 "crisis-composite":"data/crisis-composite.json",
 "liquidity":"data/liquidity.json",
 "bond-vol":"data/bond-vol.json",
 "portfolio-risk":"data/portfolio-risk.json",
 "risk-monitor":"data/risk-monitor.json",
 "global-stress":"data/global-stress.json",
 "systemic-stress":"data/systemic-stress.json",
}
out={k:peek(v) for k,v in files.items()}
open("aws/ops/reports/1478_liq.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
