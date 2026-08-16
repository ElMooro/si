"""
ops/4736 -- patch the one gap left by ops 4735.

HS4:6403 (Leather footwear) is at 114 months instead of 150 because its
2013-01..2015-12 chunk hit a Census read timeout. The ledger stores only
YoY percentages, not raw values, so computing the missing YoY points
(2014-01..2016-12) needs raw data for 2013-01..2016-12 -- the 2016 raw
was fetched last run but never persisted. One range call covers all 48
months. Longer timeout (60s) + more retries than the main run since this
exact chunk is the one that timed out.
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
FUNCTION_NAME = "justhodl-import-canary"
HIST_KEY = "data/import-canary-history.json"
BASE_HS = "https://api.census.gov/data/timeseries/intltrade/imports/hs"
UA = {"User-Agent": "Mozilla/5.0 (justhodl-backfill/1.0)"}
LINE_KEY = "HS4:6403"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def months_back(ym, n):
    y, m = int(ym[:4]), int(ym[5:7])
    total = y * 12 + (m - 1) - n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def main():
    with report("4736_patch_6403_gap") as rep:
        rep.heading("ops 4736 -- patch HS4:6403's missing 2013-2016 span")

        cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        key = ((cfg.get("Environment") or {}).get("Variables") or {}).get("CENSUS_API_KEY", "")
        rep.kv(check="key_retrieved", value=bool(key))
        if not key:
            rep.fail("no key -- stopping")
            return

        params = {"get": "GEN_VAL_MO", "COMM_LVL": "HS4", "I_COMMODITY": "6403",
                   "time": "from 2013-01 to 2016-12", "key": key}
        qs = "&".join(f"{k}={urllib.request.quote(str(v))}" for k, v in params.items())
        url = f"{BASE_HS}?{qs}"

        raw = {}
        last_err = None
        for attempt in range(4):
            try:
                req = urllib.request.Request(url, headers=UA)
                with urllib.request.urlopen(req, timeout=60) as r:
                    body = r.read().decode("utf-8", "replace")
                if body.strip().startswith("["):
                    rows = json.loads(body)
                    header = rows[0]
                    val_i = header.index("GEN_VAL_MO")
                    time_i = header.index("time")
                    for row in rows[1:]:
                        try:
                            fv = float(row[val_i])
                            if fv > 0:
                                raw[row[time_i]] = fv
                        except (TypeError, ValueError):
                            pass
                break
            except Exception as e:
                last_err = e
                rep.warn(f"attempt {attempt + 1}: {type(e).__name__}: {str(e)[:100]}")
                time.sleep(3 * (attempt + 1))

        rep.kv(check="raw_months_fetched", value=len(raw))
        if not raw:
            rep.fail(f"all attempts failed: {last_err}")
            return

        yoy = {}
        for ym, v in raw.items():
            prev = months_back(ym, 12)
            if raw.get(prev):
                yoy[ym] = round((v / raw[prev] - 1) * 100, 2)
        rep.kv(check="yoy_points_computed", value=len(yoy))

        hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
        rec = hist["lines"].setdefault(LINE_KEY, {})
        before = len(rec)
        # only fill months not already present -- never touch what the
        # main backfill / live lambda already wrote
        added = 0
        for ym, v in yoy.items():
            if ym not in rec:
                rec[ym] = v
                added += 1
        s3.put_object(Bucket=BUCKET, Key=HIST_KEY, Body=json.dumps(hist),
                       ContentType="application/json")
        rep.kv(check="ledger_before", value=before)
        rep.kv(check="points_added", value=added)
        rep.kv(check="ledger_after", value=len(rec))
        rep.ok(f"{LINE_KEY}: {before} -> {len(rec)} months ({added} added)")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("PATCH ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
