"""
Trigger the rewritten earnings tracker + verify it produces real data.
"""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/earnings-tracker.json"
LAMBDA_NAME = "justhodl-earnings-tracker"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("verify_earnings_tracker") as r:
        r.heading("Trigger and verify rewritten earnings tracker")

        r.section("1. Lambda config sanity")
        try:
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            r.log(f"  runtime: {cfg['Runtime']}")
            r.log(f"  memory: {cfg['MemorySize']} MB")
            r.log(f"  timeout: {cfg['Timeout']}s")
            r.log(f"  last_modified: {cfg['LastModified']}")
            env = cfg.get("Environment", {}).get("Variables", {})
            r.log(f"  env vars: {list(env.keys())}")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("2. Trigger Lambda")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']}")
            r.log(f"  duration: {time.time()-t0:.1f}s")
            r.log(f"  response: {payload[:500]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ Function error: {inv['FunctionError']}")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("3. Verify S3 output")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=KEY)
            data = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {data.get('generated_at')}")
            r.log(f"  duration_s: {data.get('duration_s')}")
            r.log(f"  watchlist_size: {data.get('watchlist_size')}")
            r.log(f"  n_upcoming: {data.get('n_upcoming')}")
            r.log(f"  n_recent: {data.get('n_recent')}")
            r.log(f"  n_pead: {data.get('n_pead')}")
            stats = data.get("aggregate_stats", {})
            r.log(f"  beat_rate: {stats.get('beat_rate_eps_yoy')}%")
            r.log(f"  median_1d_return: {stats.get('median_1d_return_pct')}%")
            r.log(f"  pct_positive_reactions: {stats.get('pct_positive_reactions')}%")

            r.section("4. Sample upcoming earnings")
            for u in (data.get("upcoming_14d") or [])[:8]:
                r.log(f"    {u['ticker']:6s} {u['earnings_date']} {u['time']:4s} EPS_est={u.get('eps_consensus')} — {u.get('name','')[:35]}")

            r.section("5. Sample recent results (PEAD)")
            for x in (data.get("pead_signals") or [])[:8]:
                rs = x.get("returns", {})
                r.log(f"    {x['ticker']:6s} filed:{x['filing_date']} eps_actual={x.get('eps_actual')} yoy={x.get('eps_yoy_pct')}% 1d={rs.get('1d')}% 5d={rs.get('5d')}% label={x.get('pead_label')}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
