import json, boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"__err__":type(e).__name__}
for sid in ["m1_growth","hicp_headline"]:
    d=gj(f"data/ecb-hist/{sid}.json")
    pts=d.get("points") if isinstance(d,dict) else None
    print(f"=== {sid} ===  freq={d.get('freq')} n={d.get('n_points')} range={d.get('first_date')}→{d.get('latest_date')}")
    if pts:
        print("  head:", pts[:3])
        print("  tail:", pts[-3:])
        print("  date sample formats:", [p[0] for p in pts[:2]], "...", [p[0] for p in pts[-2:]])
# Now simulate the YoY-of-hicp + alignment to see overlap
m1=gj("data/ecb-hist/m1_growth.json").get("points") or []
hp=gj("data/ecb-hist/hicp_headline.json").get("points") or []
def yoy(pts):
    out=[]
    for i in range(12,len(pts)):
        prev=pts[i-12][1]
        if prev: out.append([pts[i][0], round((pts[i][1]/prev-1)*100,2)])
    return out
hy={d:v for d,v in yoy(hp)}
m1dates=set(d for d,_ in m1); hydates=set(hy.keys())
overlap=m1dates & hydates
print("\nm1 dates ex:", sorted(m1dates)[:3], sorted(m1dates)[-3:])
print("hicp-yoy dates ex:", sorted(hydates)[:3], sorted(hydates)[-3:])
print("OVERLAP count:", len(overlap), "| sample:", sorted(overlap)[:3], sorted(overlap)[-3:] if overlap else "NONE")
