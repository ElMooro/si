"""
Reproduce 13F parse failure from inside the Lambda runtime.

Two questions:
  1. Does parse_infotable from the deployed code work standalone?
  2. What does parse_one_fund return when called for BERKSHIRE?

Plan:
  1. Invoke the Lambda with a special event {"debug_fund": "BERKSHIRE"}
     that asks it to do a SINGLE-fund run and return verbose info.
  2. If we don't have that hook, just invoke with empty event and read
     the cache file justhodl-dashboard-live/13f-cache/BERKSHIRE/*.json
     to see what got cached.
"""
import json
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("test_13f_parse_inside_lambda") as r:
        r.heading("Test parse_infotable inside Lambda runtime")

        r.section("1. Check 13f-cache contents for failing funds")
        try:
            response = s3.list_objects_v2(Bucket=BUCKET, Prefix="13f-cache/", MaxKeys=50)
            for obj in response.get("Contents", []):
                key = obj["Key"]
                size = obj["Size"]
                # Read it
                try:
                    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
                    data = json.loads(body)
                    n_pos = len(data.get("positions", []))
                    r.log(f"  {key} ({size}b) — {n_pos} positions")
                except Exception as e:
                    r.log(f"  {key} ({size}b) — read error: {e}")
        except Exception as e:
            r.log(f"  list error: {e}")

        r.section("2. Read the live institutional-positions.json sample")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/institutional-positions.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {data.get('generated_at')}")
            r.log(f"  tracked_funds: {data.get('tracked_funds')}")
            r.log(f"  by_fund keys: {list(data.get('by_fund', {}).keys())}")
            for k, v in (data.get('by_fund') or {}).items():
                latest = v.get('latest_filing', {})
                r.log(f"    {k}: accession={latest.get('accession')}, period={latest.get('period_of_report')}")
        except Exception as e:
            r.log(f"  read fail: {e}")

        r.section("3. Read the 13f-positions.json — what does the page see?")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {data.get('generated_at')}")
            funds_parsed = data.get("funds_parsed", "?")
            funds_failed = data.get("funds_failed", "?")
            r.log(f"  funds_parsed: {funds_parsed}, funds_failed: {funds_failed}")
            r.log(f"  tickers_aggregated: {data.get('tickers_aggregated', '?')}")
            r.log(f"\n  funds_status:")
            for k, v in (data.get('by_fund') or {}).items():
                if isinstance(v, dict):
                    err = v.get('error')
                    n = len(v.get('top_positions', []))
                    r.log(f"    {k}: err={err}, top_positions={n}")
            r.log(f"\n  most_bought (first 5):")
            for x in (data.get("most_bought") or [])[:5]:
                r.log(f"    {x}")
            r.log(f"\n  consensus_holds (first 5):")
            for x in (data.get("consensus_holds") or [])[:5]:
                r.log(f"    {x}")
        except Exception as e:
            r.log(f"  read fail: {e}")


if __name__ == "__main__":
    main()
