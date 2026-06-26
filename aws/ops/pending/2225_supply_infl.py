import boto3, json
from datetime import datetime, timezone
s3=boto3.client("s3","us-east-1")
now=datetime.now(timezone.utc)
def age(ts):
    try:
        t=datetime.fromisoformat(str(ts).replace("Z","+00:00"))
        return round((now-t.replace(tzinfo=t.tzinfo or timezone.utc)).total_seconds()/3600,1)
    except: return None
for key in ["supply-inflection","supply-inflection-scanner","themes-detected"]:
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"data/{key}.json")["Body"].read())
        print(f"\n{key}.json: age={age(d.get('generated_at'))}h keys={list(d.keys())[:12]}")
        # show theme tightness + top inflecting signals
        for k,v in d.items():
            if isinstance(v,list) and v and isinstance(v[0],dict):
                print(f"   '{k}' n={len(v)} sample={json.dumps(v[0])[:150]}")
            if isinstance(v,dict) and k.lower() in ("theme_scores","themes","per_theme","theme_inflection","signals"):
                items=list(v.items())[:6]
                print(f"   dict '{k}': {[(kk, (vv if not isinstance(vv,(dict,list)) else json.dumps(vv)[:60])) for kk,vv in items]}")
    except Exception as e: print(f"\n{key}.json: ERR {str(e)[:55]}")
print("DONE 2225")
