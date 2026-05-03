"""Inspect the 13f-cache directory state to see which parser_versions are stored."""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("inspect_13f_cache_state") as r:
        r.heading("Inspect 13F cache state")

        r.section("List 13f-cache contents")
        try:
            response = s3.list_objects_v2(Bucket=BUCKET, Prefix="13f-cache/", MaxKeys=50)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                size = obj["Size"]
                try:
                    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                    data = json.loads(body)
                    n_pos = len(data.get("positions", []))
                    pv = data.get("parser_version", "v1?")
                    cached = data.get("cached_at", "?")[:19]
                    # Sample value
                    sample_val = "?"
                    if data.get("positions"):
                        p0 = data["positions"][0]
                        sample_val = f"{p0.get('name','?')[:20]}: ${p0.get('value_usd', 0)/1e9:.2f}B"
                    r.log(f"  {key} (v={pv}, size={size}) {n_pos} pos | {sample_val} | cached {cached}")
                except Exception as e:
                    r.log(f"  {key} ({size}b) — read error: {str(e)[:100]}")
        except Exception as e:
            r.fail(f"  list error: {e}")

        r.section("Sample one fund's positions for value sanity check")
        # Fetch BERKSHIRE's cached file, look at first 3 positions
        try:
            for obj in response.get("Contents", []):
                if "BERKSHIRE" in obj["Key"]:
                    body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
                    data = json.loads(body)
                    r.log(f"  BERKSHIRE cache: {obj['Key']}")
                    r.log(f"    parser_version: {data.get('parser_version')}")
                    r.log(f"    cached_at: {data.get('cached_at')}")
                    r.log(f"    accession: {data.get('accession')}")
                    r.log(f"\n  First 5 positions:")
                    for p in data.get("positions", [])[:5]:
                        v_b = p.get("value_usd", 0) / 1e9
                        r.log(f"    {p.get('name','?')[:30]:30s} ${v_b:>10,.2f}B  shares={p.get('shares', 0):>15,}  cusip={p.get('cusip')}")
                    break
        except Exception as e:
            r.log(f"  err: {e}")


if __name__ == "__main__":
    main()
