"""ops 2652 — exact field structure for the 3 ambiguous candidates + confirm the
straightforward ones' ticker-field names precisely."""
import boto3, json
s3 = boto3.client("s3", region_name="us-east-1")
def get(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=f"data/{k}.json")["Body"].read())
    except Exception as e: return {"__err__": str(e)[:100]}

for name, path in [("analyst-actions","upgrades"), ("catalyst-calendar","events"), ("activist-13d","summary")]:
    d = get(name)
    v = d.get(path)
    print(f"=== {name}.{path} ===")
    print("  type:", type(v).__name__, "| len:" , len(v) if hasattr(v,'__len__') else '?')
    if isinstance(v, list) and v:
        print("  sample[0]:", json.dumps(v[0], indent=1)[:400])
    elif isinstance(v, dict):
        print("  keys:", list(v.keys())[:10])
    print()

print("=== confirm exact ticker-field names on the straightforward ones ===")
for name, field in [("13f-positions","most_bought"),("estimate-revisions","estimate_strength_leaders"),
                     ("forward-orders","top_25_by_score"),("backlog","accelerating"),
                     ("finra-short","squeeze_candidates"),("short-interest","top_crowded_shorts"),
                     ("beneish","red_flags"),("earnings-quality","top_20_high_quality"),
                     ("sector-rotation","sectors")]:
    d = get(name); v = d.get(field)
    if isinstance(v, list) and v:
        print(f"  {name}.{field}: sample[0] = {json.dumps(v[0])[:250]}")
    else:
        print(f"  {name}.{field}: EMPTY or missing ({v})")
print("DONE 2652")
