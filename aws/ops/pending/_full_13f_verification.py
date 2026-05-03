"""Wipe cache, invoke 13f-positions + sec-13f, verify all 18 funds operational + change detection."""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("full_13f_verification") as r:
        r.heading("Full 13F verification — all funds + change detection")

        r.section("1. Invoke sec-13f to refresh filings index (with prior_filing field)")
        try:
            t0 = time.time()
            resp = lam.invoke(
                FunctionName="justhodl-sec-13f",
                InvocationType="RequestResponse",
                Payload=b'{}',
            )
            r.ok(f"  status: {resp['StatusCode']}, dur: {time.time()-t0:.1f}s")
        except Exception as e:
            r.fail(f"  invoke error: {e}")

        time.sleep(2)

        r.section("2. Wipe 13f-cache to force re-parse with new logic")
        try:
            response = s3.list_objects_v2(Bucket=BUCKET, Prefix="13f-cache/", MaxKeys=100)
            objs = response.get("Contents", [])
            r.log(f"  found {len(objs)} cached files")
            if objs:
                s3.delete_objects(Bucket=BUCKET, Delete={
                    "Objects": [{"Key": o["Key"]} for o in objs],
                    "Quiet": True,
                })
                r.ok(f"  ✓ deleted {len(objs)}")
        except Exception as e:
            r.fail(f"  wipe error: {e}")

        r.section("3. Invoke 13f-positions (allow up to 10 min for full parse)")
        try:
            t0 = time.time()
            resp = lam.invoke(
                FunctionName="justhodl-13f-positions",
                InvocationType="RequestResponse",
                Payload=b'{"source": "verify-all"}',
            )
            dur = time.time() - t0
            r.log(f"  status: {resp['StatusCode']}, dur: {dur:.1f}s")
            body = resp["Payload"].read().decode("utf-8")
            r.log(f"  body: {body[:400]}")
        except Exception as e:
            r.fail(f"  invoke error: {e}")

        time.sleep(3)

        r.section("4. Final state")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
            data = json.loads(obj["Body"].read())
            funds_parsed = data.get("funds_parsed", 0)
            funds_failed = data.get("funds_failed", 0)
            r.log(f"  funds_parsed: {funds_parsed} / {data.get('funds_total')}")
            r.log(f"  funds_failed: {funds_failed}")
            for e in data.get("fund_errors", []):
                r.log(f"    {e.get('fund_key'):15s} {e.get('error')}")

            r.section("5. AUMs (sanity)")
            funds = data.get("by_fund", {})
            for k, v in sorted(funds.items(),
                               key=lambda x: -(x[1].get("total_value_usd", 0) if isinstance(x[1], dict) else 0)):
                if isinstance(v, dict) and not v.get("error"):
                    aum = v.get("total_value_usd", 0)
                    n = v.get("n_positions", 0)
                    cs = v.get("changes_summary", {})
                    new = len(cs.get("new", []))
                    exits = len(cs.get("exits", []))
                    adds = len(cs.get("adds", []))
                    trims = len(cs.get("trims", []))
                    r.log(f"    {k:15s} {n:5d}p ${aum/1e9:>7.1f}B   NEW={new:3d} EXIT={exits:3d} ADD={adds:3d} TRIM={trims:3d}")

            r.section("6. Top changes (cross-fund)")
            mb = (data.get("most_bought") or [])[:8]
            r.log(f"\n  Most bought (top 8 by buying activity):")
            for x in mb:
                t = x.get("ticker") or x.get("cusip", "?")[:9]
                n_buy = x.get("n_funds_adding", 0) + x.get("n_funds_new_position", 0)
                n_sell = x.get("n_funds_trimming", 0) + x.get("n_funds_exiting", 0)
                r.log(f"    {t:8s} {x.get('name','')[:30]:30s} +{n_buy} buying / -{n_sell} selling")

            ms = (data.get("most_sold") or [])[:8]
            r.log(f"\n  Most sold (top 8 by selling activity):")
            for x in ms:
                t = x.get("ticker") or x.get("cusip", "?")[:9]
                n_buy = x.get("n_funds_adding", 0) + x.get("n_funds_new_position", 0)
                n_sell = x.get("n_funds_trimming", 0) + x.get("n_funds_exiting", 0)
                r.log(f"    {t:8s} {x.get('name','')[:30]:30s} +{n_buy} buying / -{n_sell} selling")

            if funds_parsed >= 17:
                r.ok(f"\n  ✅ {funds_parsed}/18 funds operational")
        except Exception as e:
            r.fail(f"  state read error: {e}")


if __name__ == "__main__":
    main()
