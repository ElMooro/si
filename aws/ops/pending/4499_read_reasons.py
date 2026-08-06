"""ops 4499 — read the four wings' self-written blockers (the design pays
off: causes live IN the feeds)."""
import json,os
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":4499,"as_of":datetime.now(timezone.utc).isoformat()}
for f in ("data/soma-holdings.json","data/treasury-fiscal.json",
          "data/bls-macro.json","data/bea-gdp.json"):
    try:
        d=json.loads(s3.get_object(Bucket=B,Key=f)["Body"].read())
        rs=[f"{k}: {v.get('reason')}" for k,v in d.items()
            if isinstance(v,dict) and v.get("data_unavailable")][:3]
        R[f]=rs or ["no missing markers?"]
    except Exception as e: R[f]=[f"read-err {str(e)[:60]}"]
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4499_reasons.md","w").write(
 "# ops 4499 — wing reasons\n"+ "\n".join(f"- {k}:\n  "+"\n  ".join(v) for k,v in R.items() if isinstance(v,list)))
json.dump(R,open("aws/ops/reports/4499_reasons.json","w"),indent=1,default=str)
print(json.dumps(R,default=str)[:700])
