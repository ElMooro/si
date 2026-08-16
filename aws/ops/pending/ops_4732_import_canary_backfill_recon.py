"""
ops/4732 -- import-canary backfill recon (read-only).

Two questions that decide how the backfill actually gets built:

  (1) data/import-canary-history.json is a self-building ledger keyed
      {"lines": {"HS6:854231": {"2026-07": 12.4, ...}}} -- YYYY-MM keys,
      not YYYY-MM-DD, which is exactly why ops 4731's date-regex scan
      (and I, reading that report) wrongly called this "zero history."
      I never even looked at the right file. Read it for real: how many
      months does each line actually have banked right now?

  (2) The live lambda calls Census's timeseries API once PER MONTH
      (time=<single YYYY-MM>) per line. If Census's API accepts a real
      range (time=from+YYYY-MM+to+YYYY-MM), one call gets years of
      history instead of 300+ serial single-month calls. Test both the
      range syntax and how far back real (non-null) monthly values
      actually exist, on the exact endpoint/params the live code uses,
      so the backfill script is built on measured reality instead of
      assumption.

Read-only: one S3 GET, a handful of Census API GETs. No writes.
"""
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
HIST_KEY = "data/import-canary-history.json"
BASE_HS = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-backfill-recon/1.0)"}

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def census_get(params, timeout=20):
    qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
    url = f"{BASE_HS}?{qs}"
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read()
            return {"ok": True, "status": r.status, "elapsed_ms": round((time.time() - t0) * 1000),
                     "body": body.decode("utf-8", "replace")}
    except urllib.error.HTTPError as e:
        try:
            sample = e.read()[:300].decode("utf-8", "replace")
        except Exception:
            sample = ""
        return {"ok": False, "status": e.code, "elapsed_ms": round((time.time() - t0) * 1000),
                 "body": sample}
    except Exception as e:
        return {"ok": False, "status": None, "error": f"{type(e).__name__}: {str(e)[:120]}",
                 "elapsed_ms": round((time.time() - t0) * 1000)}


def main():
    with report("4732_import_canary_backfill_recon") as rep:
        rep.heading("ops 4732 -- import-canary backfill recon (read-only)")

        rep.section("1. Real current depth of the self-building ledger (data/import-canary-history.json)")
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read()
            hist = json.loads(raw)
            lines = hist.get("lines", {})
            rep.kv(check="hist_key_size_bytes", value=len(raw))
            rep.kv(check="n_lines_tracked", value=len(lines))
            depths = sorted(((k, len(v), min(v) if v else None, max(v) if v else None)
                              for k, v in lines.items()), key=lambda x: -x[1])
            if depths:
                rep.kv(check="deepest_line_months", value=depths[0][1])
                rep.kv(check="shallowest_line_months", value=depths[-1][1])
                rep.log(f"deepest line: {depths[0][0]} -- {depths[0][1]} months, "
                        f"{depths[0][2]} -> {depths[0][3]}")
                rep.log(f"shallowest line: {depths[-1][0]} -- {depths[-1][1]} months, "
                        f"{depths[-1][2]} -> {depths[-1][3]}")
                for k, n, earliest, latest in depths[:8]:
                    rep.log(f"  {k}: {n} months banked, {earliest} -> {latest}")
            else:
                rep.warn("ledger exists but 'lines' dict is empty")
        except s3.exceptions.NoSuchKey:
            rep.fail(f"{HIST_KEY} does not exist at all -- ledger has never been written")
        except Exception as e:
            rep.warn(f"couldn't read/parse {HIST_KEY}: {type(e).__name__}: {str(e)[:120]}")

        rep.section("2. Census API key -- is one configured, or running keyless?")
        try:
            p = ssm.get_parameter(Name="/justhodl/census_api_key", WithDecryption=True)
            key = p["Parameter"]["Value"]
            rep.ok(f"found /justhodl/census_api_key in SSM ({len(key)} chars)")
        except Exception as e:
            key = ""
            rep.warn(f"no /justhodl/census_api_key in SSM ({type(e).__name__}) -- "
                     "live lambda's CENSUS_API_KEY env default is empty, so it's "
                     "running keyless right now (Census allows limited keyless access)")

        rep.section("3. Does Census's timeseries API accept a real date RANGE (one call vs. hundreds)?")
        test_params_range = {"get": "GEN_VAL_MO", "COMM_LVL": "HS6", "I_COMMODITY": "854231",
                              "time": "from 2013-01 to 2013-06"}
        if key:
            test_params_range["key"] = key
        r_range = census_get(test_params_range)
        rep.kv(check="census_range_query_works", value=r_range["ok"])
        rep.log(f"range probe (2013-01..2013-06, HS6 854231): {json.dumps({k: v for k, v in r_range.items() if k != 'body'})}")
        rep.log(f"  body sample: {r_range.get('body', '')[:400]}")

        rep.section("4. How far back does real (non-null) monthly data actually exist for this line?")
        for probe_year in ["1995-01", "2002-01", "2008-01", "2013-01"]:
            params = {"get": "GEN_VAL_MO", "COMM_LVL": "HS6", "I_COMMODITY": "854231", "time": probe_year}
            if key:
                params["key"] = key
            r = census_get(params)
            rep.kv(check=f"census_data_exists_{probe_year}", value=r["ok"])
            rep.log(f"  {probe_year}: {json.dumps({k: v for k, v in r.items() if k != 'body'})} "
                    f"body: {r.get('body', '')[:200]}")

        rep.section("Summary")
        rep.log("Read-only recon only. If the range query in section 3 works, the backfill "
                 "script becomes one call per line for the whole available span instead of "
                 "hundreds of serial monthly calls. Section 1's real depths replace the wrong "
                 "'zero history' conclusion from checking the wrong file in ops 4731.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("RECON ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
