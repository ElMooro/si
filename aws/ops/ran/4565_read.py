"""ops 4565 — pure read of fred-scoped state to confirm the accounting
fix reconciles on a naturally-occurring cron round (no invoke)."""
import json,os
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":4565,"at":datetime.now(timezone.utc).isoformat()}
st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
R["state"]={"cats_done":len(st.get("cats_done") or []),"of":st.get("n_categories_expanded"),
            "seen":st.get("series_seen"),"imported_total":st.get("series_imported"),
            "baseline":st.get("imported_baseline"),
            "new_this_epoch":st.get("series_imported",0)-st.get("imported_baseline",0),
            "stale":st.get("series_excluded_stale"),
            "disc":st.get("series_excluded_discontinued"),
            "skipped":st.get("series_skipped_already"),
            "errors":len(st.get("errors") or {}),
            "accounting":st.get("accounting"),"status":st.get("status"),
            "updated_at":st.get("updated_at"),"blocked_at":st.get("blocked_at")}
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4565.md","w").write("# 4565 — "+json.dumps(R["state"],default=str)+"\n")
print(json.dumps(R["state"],default=str)[:400])
