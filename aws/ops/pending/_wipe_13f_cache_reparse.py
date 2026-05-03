"""
Wipe the 13f-cache directory, then invoke the Lambda for a fresh parse.

The previous cache files were written before my value-detection fix
landed (they show parser_version: None and have ×1000-too-large values
because old Lambda code multiplied by 1000). The new code uses
parser_version="v2" but the existing files lack the field, so
get_s3_json returns the old data and the version check passes-through.

Wait — the version check is:
  if cached and cached.get("parser_version") == PARSER_VERSION:
This SHOULD reject cached files without a parser_version key. But the
cache key itself uses {ACCESSION}_{PARSER_VERSION}.json now, while
existing files are at {ACCESSION}.json (without the version suffix).
So the new code SHOULD be reading from a different cache key entirely.

Let me verify by listing the bucket — if there are both with-version
and without-version files, then the cache logic is fine but the report
came from before the new code deployed. If only without-version files,
then the cache key fix didn't ship.

Either way: delete all 13f-cache files and force a fresh parse to
verify current code works correctly.
"""
import json
import time
from ops_report import report
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("wipe_13f_cache_force_reparse") as r:
        r.heading("Wipe 13f-cache and force fresh parse")

        r.section("1. List existing cache files")
        try:
            response = s3.list_objects_v2(Bucket=BUCKET, Prefix="13f-cache/", MaxKeys=100)
            objects = response.get("Contents", [])
            r.log(f"  found {len(objects)} cached files")
            v_with_version = sum(1 for o in objects if "_v2.json" in o["Key"] or "_v3.json" in o["Key"])
            v_unversioned = len(objects) - v_with_version
            r.log(f"    versioned cache files: {v_with_version}")
            r.log(f"    unversioned cache files: {v_unversioned}")
        except Exception as e:
            r.fail(f"  list error: {e}")
            return

        r.section("2. Delete all 13f-cache files")
        if not objects:
            r.log("  no files to delete")
        else:
            try:
                # Batch delete (1000 max per call)
                delete_request = {
                    "Objects": [{"Key": o["Key"]} for o in objects],
                    "Quiet": True,
                }
                resp = s3.delete_objects(Bucket=BUCKET, Delete=delete_request)
                errors = resp.get("Errors", [])
                if errors:
                    r.log(f"  ⚠ {len(errors)} errors during delete:")
                    for e in errors[:5]:
                        r.log(f"    {e}")
                else:
                    r.ok(f"  ✓ deleted {len(objects)} cache files")
            except Exception as e:
                r.fail(f"  delete error: {e}")
                return

        r.section("3. Invoke 13f-positions Lambda for fresh parse")
        try:
            t0 = time.time()
            resp = lam.invoke(
                FunctionName="justhodl-13f-positions",
                InvocationType="RequestResponse",
                Payload=b'{"source": "wipe-and-reparse"}',
            )
            dur = time.time() - t0
            r.log(f"  invocation status: {resp['StatusCode']}, duration: {dur:.1f}s")
            body = resp["Payload"].read().decode("utf-8")
            r.log(f"  response (first 400): {body[:400]}")
        except Exception as e:
            r.fail(f"  invoke error: {e}")
            return

        r.section("4. Read fresh data/13f-positions.json")
        try:
            time.sleep(2)
            obj = s3.get_object(Bucket=BUCKET, Key="data/13f-positions.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {data.get('generated_at')}")
            r.log(f"  funds_parsed: {data.get('funds_parsed')} / {data.get('funds_total')}")
            r.log(f"  funds_failed: {data.get('funds_failed')}")
            errs = data.get("fund_errors", [])
            for e in errs:
                r.log(f"    {e.get('fund_key'):15s} {e.get('error')}")

            r.log(f"\n  Per-fund AUM (sanity check):")
            for k, v in (data.get("by_fund") or {}).items():
                if isinstance(v, dict) and not v.get("error"):
                    aum = v.get("portfolio_summary", {}).get("total_value_usd", 0)
                    n = v.get("portfolio_summary", {}).get("n_positions", 0)
                    r.log(f"    {k:15s} {n:5d} pos  AUM ${aum/1e9:>9.1f}B")

            mb = (data.get("most_bought") or [])[:5]
            r.log(f"\n  Top 5 most-bought (cross-fund):")
            for x in mb:
                r.log(f"    {x.get('ticker', '?'):8s} {x.get('name', '')[:30]:30s} "
                      f"{x.get('n_funds_holding')} funds, ${x.get('total_value', 0)/1e9:.1f}B total")
        except Exception as e:
            r.fail(f"  read error: {e}")


if __name__ == "__main__":
    main()
