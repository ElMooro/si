import boto3, json
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
fm=d.get("forward_model") or {}
print("=== key_assumptions structure (the [object Object] bug) ===")
ka=fm.get("key_assumptions") or []
print("count:",len(ka),"| type of [0]:", type(ka[0]).__name__ if ka else None)
print("sample:", json.dumps(ka[:2])[:300])
print("\n=== price series in doc? (for chart) ===")
print("top-level keys with price/return/eod:", [k for k in d if any(w in k.lower() for w in ("price","return","eod","history","prices"))])
print("returns:", json.dumps(d.get("returns"))[:200])
print("\n=== margins in doc (the blank Margin Trend) ===")
print("margins:", json.dumps(d.get("margins"))[:300])
print("DONE 2261")
