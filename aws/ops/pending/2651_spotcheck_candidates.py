"""ops 2651 — spot-check live data quality/freshness of the top new-engine candidates
before wiring any into master-ranker."""
import boto3, json, time
from datetime import datetime, timezone
s3 = boto3.client("s3", region_name="us-east-1")

CANDIDATES = ["analyst-actions","dealer-gex","activist-13d","13f-positions","13f-aggregate",
  "catalyst-calendar","forward-orders","backlog","estimate-revisions","beneish","earnings-quality",
  "short-interest","finra-short","rotation-radar","sector-rotation","gf-value","magic-formula",
  "smart-beta","index-recon","merger-arb","dividend-growth","patent-velocity"]

for c in CANDIDATES:
    key = f"data/{c}.json"
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
        body = json.loads(obj["Body"].read())
        mod = obj["LastModified"]
        age_h = round((datetime.now(timezone.utc) - mod).total_seconds() / 3600, 1)
        if isinstance(body, dict):
            top = list(body.keys())[:6]
            # try to find the main list-of-tickers field
            list_field = None
            for k, v in body.items():
                if isinstance(v, list) and v and isinstance(v[0], dict) and any(kk in v[0] for kk in ("ticker","symbol")):
                    list_field = (k, len(v)); break
            print(f"  {c:22s} age={age_h:>5}h  keys={top}  ticker_list={list_field}")
        elif isinstance(body, list):
            print(f"  {c:22s} age={age_h:>5}h  TOP-LEVEL LIST, len={len(body)}, sample_keys={list(body[0].keys())[:6] if body else None}")
        else:
            print(f"  {c:22s} age={age_h:>5}h  unexpected type: {type(body)}")
    except s3.exceptions.NoSuchKey:
        print(f"  {c:22s} MISSING (no such key)")
    except Exception as e:
        print(f"  {c:22s} ERROR: {str(e)[:80]}")
print("DONE 2651")
