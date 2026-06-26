import boto3, json
d=json.loads(boto3.client("s3","us-east-1").get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
print("generated_at:", d.get("generated_at"))
bm=d.get("business_mix") or {}
print("\nBUSINESS MIX segments:", bm.get("segments"))
print("BUSINESS MIX geography:", bm.get("geography"))
print("segment_trend periods:", len(bm.get("segment_trend") or []))
print("business_mix_assessment:", str(d.get("business_mix_assessment"))[:280])
ph=d.get("price_history") or []
print("\nPRICE HISTORY:", len(ph), "pts | first:", ph[0] if ph else None, "| last:", ph[-1] if ph else None)
m=d.get("margins") or {}
ot=[x for x in (m.get("operating_trend") or []) if x.get("value") is not None]
print("\nMARGINS operating non-null:", len(ot), "| latest:", (m.get("operating_trend") or [{}])[0], "| gross latest:", (m.get("gross_trend") or [{}])[0])
print("DONE 2263")
