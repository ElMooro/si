"""ops_4030 — lift the watchlist cap, settle, verify Khalid's upload landed."""
import io
import json
import sys
import time
import urllib.request
import zipfile as zf
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=90, retries={"max_attempts": 1}))
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-tv-notes-ingest"
MARK = "ops4030: watchlist cap"


def main():
    with report("4030_cap_and_proof") as rep:
        rep.heading("ops 4030 — cap lifted + upload proof")
        checks = []

        rep.section("A. proof his SYNC landed")
        h = s3.head_object(Bucket=BUCKET, Key="data/tv-watchlists.json")
        age_min = (datetime.now(timezone.utc) - h["LastModified"]).total_seconds() / 60
        wl = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-watchlists.json")["Body"].read())
        wls = wl.get("watchlists") or []
        rep.kv(modified=h["LastModified"].isoformat(), age_min=round(age_min, 1),
               n_watchlists=len(wls))
        checks.append(("watchlists artifact refreshed TODAY", age_min < 180))
        checks.append(("~200 lists landed (pre-fix cap)", len(wls) >= 190))
        try:
            ev = logs.filter_log_events(
                logGroupName=f"/aws/lambda/{FN}",
                startTime=int((datetime.now(timezone.utc) -
                               timedelta(minutes=90)).timestamp() * 1000),
                limit=30)
            starts = [e for e in ev.get("events") or []
                      if "START RequestId" in e.get("message", "")]
            rep.kv(ingest_invocations_90min=len(starts))
            checks.append(("ingest heard from the browser", len(starts) >= 1))
        except Exception as e:
            rep.log(f"  logs: {type(e).__name__}")

        rep.section("B. lift the cap (491 lists must fit)")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        assert MARK in src and "[:1200]" in src
        buf = io.BytesIO()
        with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", src)
        for _ in range(6):
            try:
                lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue(),
                                         Publish=True)
                break
            except lam.exceptions.ResourceConflictException:
                time.sleep(10)
        ok = False
        for _ in range(24):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") != "InProgress":
                dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
                    lam.get_function(FunctionName=FN)["Code"]["Location"],
                    timeout=60).read())).read("lambda_function.py").decode()
                if MARK in dep and "[:1200]" in dep:
                    ok = True
                    break
            time.sleep(8)
        checks.append(("cap 1200 settled in the deployed zip", ok))

        failed = [l for l, k in checks if not k]
        for l, k in checks:
            (rep.ok if k else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — upload proven ({len(wls)} lists, {age_min:.0f} min "
               f"ago); next upload carries all 491")


if __name__ == "__main__":
    main()
