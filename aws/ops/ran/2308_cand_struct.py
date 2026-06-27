import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("top-level keys:", list(d.keys()))
# find the list of candidates
for k,v in d.items():
    if isinstance(v,list) and v and isinstance(v[0],dict) and ("ticker" in v[0]):
        print(f"candidate list under '{k}': {len(v)} items")
        from collections import Counter
        gc=Counter(c.get("pressure_group") for c in v)
        print("  group distribution:", dict(gc))
        print("  industrials remapped:")
        for c in v:
            g=c.get("pressure_group")
            if g in ("MACHINERY","ELECTRICAL_EQUIP","AEROSPACE_DEFENSE"):
                print(f"    {c.get('ticker')}: {g} ({c.get('industry')})")
        # show a few of whatever is there
        print("  sample:", [(c.get('ticker'),c.get('pressure_group'),c.get('industry')) for c in v[:8]])
print("DONE 2308")
