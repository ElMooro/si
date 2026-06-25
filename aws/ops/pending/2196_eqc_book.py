import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/equity-confluence.json")["Body"].read())
print("top keys:", list(d.keys()))
for k,v in d.items():
    if isinstance(v,list) and v and isinstance(v[0],dict):
        print(f"  book '{k}' n={len(v)} sample-keys={list(v[0].keys())[:8]}")
        # look for value/insider in a sample
        for r in v[:30]:
            fl=r.get("families") or r.get("families_lit") or r.get("supers") or {}
            s=json.dumps(fl)
            if "value" in s or "insider" in s:
                print(f"    {r.get('ticker')}: families={s[:120]}"); break
print("super_status:", json.dumps(d.get("super_status",{}))[:200])
print("DONE 2196")
