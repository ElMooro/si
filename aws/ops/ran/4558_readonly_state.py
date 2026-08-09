"""ops 4558 — pure S3 read, zero Lambda invokes (avoids repeating the
4557 client-timeout mistake). Check whether the server-side run actually
completed despite my client connection dropping."""
import json,os
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":4558,"at":datetime.now(timezone.utc).isoformat()}
try:
    st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    R["state"]={"cats_done":st.get("cats_done"),"series_seen":st.get("series_seen"),
               "series_excluded_stale":st.get("series_excluded_stale"),
               "series_imported":st.get("series_imported"),
               "blocked_at":st.get("blocked_at"),"updated_at":st.get("updated_at"),
               "n_pages":st.get("n_pages")}
    R["sample_imported"]=st.get("imported_ids",[])[:8]
    R["sample_excluded"]=st.get("excluded_ids",[])[:4]
except Exception as e: R["state_err"]=str(e)[:100]
try:
    m=json.loads(s3.get_object(Bucket=B,Key="data/providers/fred-scoped/manifest.json")["Body"].read())
    R["manifest"]=m
except Exception as e: R["manifest_err"]=str(e)[:80]
# spot-check one actual imported series file if any exist
if R.get("sample_imported"):
    sid=R["sample_imported"][0]
    try:
        cat=(R.get("state") or {})
        for prefix in ("Interest_Rates","Exchange_Rates","Monetary_Data","Financial_Indicators","Banking","Business_Lending","Foreign_Exchange_Intervention"):
            try:
                obj=json.loads(s3.get_object(Bucket=B,Key=f"data/warm/fred-scoped/{prefix}/{sid}.json")["Body"].read())
                R["spot_check"]={"id":sid,"found_under":prefix,"n_obs":len(obj.get("observations",[])),
                                 "last_obs":obj.get("observations",[{}])[-1] if obj.get("observations") else None,
                                 "title":obj.get("meta",{}).get("title")}
                break
            except Exception: continue
    except Exception as e: R["spot_check_err"]=str(e)[:60]
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4558.md","w").write("# 4558 read-only state — "+json.dumps(R,indent=1,default=str)+"\n")
print(json.dumps(R,default=str)[:700])
