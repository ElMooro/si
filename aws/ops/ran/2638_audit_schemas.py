import boto3, json
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
def peek(key, fields_only_lists=True):
    try:
        d = json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception as e:
        print(f"  [{key}] ERROR: {e}"); return
    print(f"\n=== {key} ===")
    print("  top-level keys:", list(d.keys())[:25])
    return d

print("########## auction-crisis.json ##########")
d = peek("data/auction-crisis.json")
if d:
    for k in ("upcoming_auctions","upcoming","auctions","next_auctions","calendar"):
        v = d.get(k)
        if isinstance(v, list) and v:
            print(f"  found list field '{k}' len={len(v)}; sample[0]:", json.dumps(v[0])[:500])
            print(f"  sample[1]:", json.dumps(v[1])[:500] if len(v)>1 else "")
    for k in ("generated_at","as_of","schema_version","method"):
        if k in d: print(f"  {k}:", d[k])
    # dump raw if nothing matched
    if not any(isinstance(d.get(k), list) for k in ("upcoming_auctions","upcoming","auctions","next_auctions","calendar")):
        print("  RAW (first 1500 chars):", json.dumps(d)[:1500])

print("\n\n########## stablecoin-flow.json ##########")
d2 = peek("data/stablecoin-flow.json")
if d2:
    print("  RAW (first 1200 chars):", json.dumps(d2)[:1200])

print("\n\n########## treasury-proxy / treasury-api candidate keys ##########")
for key in ("data/treasury-proxy.json","data/treasury-api.json","data/treasury.json","data/daily-treasury-statement.json"):
    try:
        d3 = json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
        print(f"  [{key}] keys:", list(d3.keys())[:20])
    except Exception as e:
        print(f"  [{key}] not found / {str(e)[:60]}")
print("DONE 2638")
