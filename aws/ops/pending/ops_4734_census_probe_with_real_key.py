"""
ops/4734 -- proper Census depth probe, this time with the real key.

ops 4732's probes all hit "Missing Key" because it only checked SSM.
ops 4733 confirmed the real key is a direct Lambda env var on
justhodl-import-canary (40 chars). Pull that value in-memory only --
never print or log it, this repo is public -- and redo the exact same
two questions properly: does Census's range syntax work, and how far
back does real (non-null) monthly HS-level data actually exist. This
is the last unknown before writing the real backfill.
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
FUNCTION_NAME = "justhodl-import-canary"
BASE_HS = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-backfill-recon/1.0)"}

lam = boto3.client("lambda", region_name=REGION)


def census_get(params, timeout=20):
    qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
    url = f"{BASE_HS}?{qs}"
    t0 = time.time()
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8", "replace")
            is_json = body.strip().startswith("[") or body.strip().startswith("{")
            return {"ok": True, "status": r.status, "is_json": is_json,
                     "elapsed_ms": round((time.time() - t0) * 1000), "body": body}
    except urllib.error.HTTPError as e:
        try:
            sample = e.read()[:300].decode("utf-8", "replace")
        except Exception:
            sample = ""
        return {"ok": False, "status": e.code, "is_json": False,
                 "elapsed_ms": round((time.time() - t0) * 1000), "body": sample}
    except Exception as e:
        return {"ok": False, "status": None, "is_json": False,
                 "error": f"{type(e).__name__}: {str(e)[:120]}",
                 "elapsed_ms": round((time.time() - t0) * 1000)}


def main():
    with report("4734_census_probe_with_real_key") as rep:
        rep.heading("ops 4734 -- Census depth probe with the real key (value never printed)")

        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        key = ((cfg.get("Environment") or {}).get("Variables") or {}).get("CENSUS_API_KEY", "")
        rep.kv(check="key_retrieved", value=bool(key))
        if not key:
            rep.fail("could not retrieve the key -- stopping, nothing else to test")
            return

        rep.section("1. Range-query syntax -- does 'time=from X to Y' work with a real key?")
        r_range = census_get({"get": "GEN_VAL_MO", "COMM_LVL": "HS6", "I_COMMODITY": "854231",
                                "time": "from 2013-01 to 2013-06", "key": key})
        rep.kv(check="range_query_returns_json", value=r_range["is_json"])
        body_preview = r_range.get("body", "")[:500]
        rep.log(f"range probe result: status={r_range.get('status')} is_json={r_range['is_json']} "
                f"elapsed_ms={r_range.get('elapsed_ms')}")
        rep.log(f"  body preview: {body_preview}")

        rep.section("2. How far back does real (non-null) monthly data exist -- single-month probes")
        for probe_ym in ["1992-01", "2000-01", "2005-01", "2010-01", "2013-01", "2020-01"]:
            r = census_get({"get": "GEN_VAL_MO", "COMM_LVL": "HS6", "I_COMMODITY": "854231",
                              "time": probe_ym, "key": key})
            has_row = False
            if r["is_json"]:
                try:
                    parsed = json.loads(r["body"])
                    has_row = len(parsed) > 1  # header row + at least one data row
                except Exception:
                    has_row = False
            rep.kv(check=f"real_data_at_{probe_ym}", value=has_row)
            rep.log(f"  {probe_ym}: is_json={r['is_json']} has_data_row={has_row} "
                    f"status={r.get('status')} body[:150]={r.get('body','')[:150]}")

        rep.section("3. NAICS endpoint too -- same COMM_LVL/HS shape doesn't apply, confirm its own earliest")
        BASE_NAICS_LOCAL = "https://api.census.gov/data/timeseries/intltrade/imports/naics"
        for probe_ym in ["2005-01", "2013-01"]:
            qs = f"get=GEN_VAL_MO&time={urllib.request.quote(probe_ym)}&NAICS=334413&key={key}"
            url = f"{BASE_NAICS_LOCAL}?{qs}"
            t0 = time.time()
            try:
                req = urllib.request.Request(url, headers=UA)
                with urllib.request.urlopen(req, timeout=20) as r:
                    body = r.read().decode("utf-8", "replace")
                is_json = body.strip().startswith("[")
                rep.kv(check=f"naics_real_data_at_{probe_ym}", value=is_json)
                rep.log(f"  NAICS {probe_ym}: is_json={is_json} body[:150]={body[:150]}")
            except Exception as e:
                rep.log(f"  NAICS {probe_ym}: {type(e).__name__} {str(e)[:100]}")

        rep.section("Summary")
        rep.log("Key value never printed. This gives the real earliest usable month and "
                 "confirms whether range queries work, so the backfill script can be written "
                 "against measured behavior instead of assumption.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
