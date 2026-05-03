"""Verify 13f-positions after all fixes — invoke + check S3."""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("verify_13f_final") as r:
        r.heading("Verify 13F positions after all fixes")

        # Wait for any rate-limiting to clear
        time.sleep(5)

        # Invoke
        try:
            r.section("Invoke 13f-positions")
            resp = lam.invoke(
                FunctionName="justhodl-13f-positions",
                InvocationType="RequestResponse",
                Payload=b'{"source":"verify-final"}',
            )
            r.ok(f"  invoke status: {resp['StatusCode']}")
            if "FunctionError" in resp:
                err = resp["Payload"].read().decode()
                r.fail(f"  FunctionError: {err[:300]}")
                return
            body = resp["Payload"].read().decode()
            r.log(f"  response body: {body}")
        except Exception as e:
            r.fail(f"  invoke fail: {e}")
            # Try to read S3 anyway
            r.log(f"  (continuing to read S3 in case prior run is current)")

        time.sleep(2)

        # Read S3 output
        r.section("Read data/13f-positions.json")
        try:
            data = json.loads(s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")["Body"].read())
        except Exception as e:
            r.fail(f"  read fail: {e}")
            return

        r.log(f"  generated_at: {data.get('generated_at')}")
        r.log(f"  funds_parsed: {data.get('funds_parsed')} / {data.get('funds_total', 18)}")
        r.log(f"  funds_failed: {data.get('funds_failed')}")

        # Per-fund breakdown
        r.section("Per-fund breakdown")
        for fk, f in sorted(data.get("by_fund", {}).items(),
                            key=lambda x: -(x[1].get("total_value_usd", 0))):
            r.log(f"  {fk:14s} {f.get('n_positions', 0):4d} positions  AUM ${f.get('total_value_usd', 0)/1e9:8.1f}B")

        if data.get("fund_errors"):
            r.section("Remaining errors")
            for e in data["fund_errors"]:
                r.log(f"  {e.get('fund_key', '?')}: {e.get('error', '?')}")

        # Top buys/sells
        r.section("Top 5 most-bought (cross-fund)")
        for s in data.get("most_bought", [])[:5]:
            actions = s.get("n_funds_adding", 0) + s.get("n_funds_new_position", 0)
            r.log(f"  {s.get('ticker', '?'):8s} {s.get('name', '')[:40]:40s} {actions} funds buying")

        r.section("Top 5 most-sold (cross-fund)")
        for s in data.get("most_sold", [])[:5]:
            actions = s.get("n_funds_trimming", 0) + s.get("n_funds_exiting", 0)
            r.log(f"  {s.get('ticker', '?'):8s} {s.get('name', '')[:40]:40s} {actions} funds selling")

        r.section("Summary")
        if data.get("funds_parsed", 0) >= 15:
            r.ok(f"  ✅ {data.get('funds_parsed')}/18 funds parsed — system operational")
        elif data.get("funds_parsed", 0) >= 10:
            r.log(f"  ⚠ {data.get('funds_parsed')}/18 — partial success")
        else:
            r.fail(f"  ✗ only {data.get('funds_parsed')}/18 — needs more work")


if __name__ == "__main__":
    main()
