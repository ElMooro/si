"""Where did micro/nano go? Audit data/universe.json source composition +
how the >=$30M and peRatio/psRatio filters thin the smallest caps."""
import json, boto3, collections
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
def load(k):
    try: return json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
    except Exception as e: return {"_err": str(e)[:120]}

uni = load("data/universe.json")
if "_err" in uni:
    print("universe.json:", uni["_err"]); raise SystemExit
stocks = uni.get("stocks") or []
print(f"data/universe.json: {len(stocks)} names | generated/keys: {list(uni.keys())[:8]}")
if stocks:
    print("sample record fields:", list(stocks[0].keys())[:14])

def bucket(m):
    if not m: return "no_mcap"
    if m < 50e6: return "nano"
    if m < 300e6: return "micro"
    if m < 2e9: return "small"
    if m < 10e9: return "mid"
    if m < 200e9: return "large"
    return "mega"

def num(v):
    try: return float(v)
    except Exception: return None

dist = collections.Counter()
ge30 = collections.Counter()
for s in stocks:
    m = num(s.get("market_cap") or s.get("marketCap"))
    b = bucket(m)
    dist[b] += 1
    if m and m >= 30e6:
        ge30[b] += 1

print("\nfull source universe by cap bucket:")
for b in ["nano","micro","small","mid","large","mega","no_mcap"]:
    print(f"  {b:>8}: {dist.get(b,0):>5}   (>=\$30M tail-eligible: {ge30.get(b,0)})")
print(f"\nTOTAL >=\$30M tail-eligible: {sum(ge30.values())}")
