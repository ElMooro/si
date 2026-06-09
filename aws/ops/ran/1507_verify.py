"""Verify the audit's specific claims against live data."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=60))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:40]}
out={}
# CLAIM: eurodollar_stress_score: null in ecb-detail.json (the smoking gun)
ed=gj("data/ecb-detail.json")
out["ecb_detail_has_eurodollar_stress_score_field"]="eurodollar_stress_score" in ed if isinstance(ed,dict) else None
out["ecb_detail_eurodollar_stress_score_value"]=ed.get("eurodollar_stress_score","FIELD_ABSENT") if isinstance(ed,dict) else None
# CLAIM: only 3 of 5 CISS subindices (FI/FX/MM), missing SS_BO + SS_EQ
m=gj("data/ecb-hist/_manifest.json")
ciss_subs=[s["id"] for s in m.get("series",[]) if "ciss" in s["id"]] if isinstance(m,dict) else []
out["ciss_series_present"]=ciss_subs
# CLAIM: subindices stale since 2025-05-02
out["ciss_sub_latest_dates"]={s["id"]:s.get("latest_date") for s in m.get("series",[]) if "ciss" in s["id"] and s["id"]!="ciss_ea"} if isinstance(m,dict) else {}
# what the dump radar actually has (the 5 derived)
der=gj("data/ecb-derived.json")
out["ecb_derived_indicators"]=list((der.get("indicators") or {}).keys()) if isinstance(der,dict) else None
out["ecb_derived_has_usd_funding_composite"]="usd_funding_stress_composite" in (der.get("indicators") or {}) if isinstance(der,dict) else None
out["ecb_derived_has_target2"]=any("target" in k for k in (der.get("indicators") or {})) if isinstance(der,dict) else None
out["ecb_derived_has_esi"]=any("esi" in k or "eurodollar_stress_index" in k for k in (der.get("indicators") or {})) if isinstance(der,dict) else None
open("aws/ops/reports/1507_v.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
