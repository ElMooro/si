"""
ops_3906 — DEPLOY: signal-backtest's FMP-rate-limit retry fix. Root cause
confirmed via real production logs (ops 3905): 18/18 chunks hit HTTP 429
Too Many Requests, every single run, with 57,997 candidate observations
but 0 live prices resolved. Added exponential-backoff retry (1s/2s/4s) plus
a 0.3s inter-chunk delay; verified locally against a stub reproducing the
exact 429-then-recover pattern before shipping.

Gate: does n_observations move off the structural 0 this run. Given the
scale (potentially ~1,800 distinct tickers, 18 chunks, real network I/O),
this invoke may take a while — timeout bumped to 420s.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-signal-backtest"
BUCKET = "justhodl-dashboard-live"
KEY = "data/signal-backtest.json"
MARKER = "n_rate_limited"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=470, retries={"max_attempts": 0}))


def snapshot():
    o = s3.get_object(Bucket=BUCKET, Key=KEY)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3906_signal_backtest_retry_fix_deploy") as rep:
        rep.heading("ops 3906 — deploy FMP retry-with-backoff, hard-gate on n_observations > 0")

        rep.section("1. BEFORE")
        before, blm = snapshot()
        rep.kv(before_n_observations=before.get("n_observations"),
               before_maturity=before.get("maturity"), before_s3=blm.isoformat())

        rep.section("2. ZIP-SETTLE BY MARKER")
        settled = False
        for attempt in range(1, 31):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src:
                    rep.ok(f"  new artifact live on attempt {attempt} ({len(blob):,} zip bytes)")
                    settled = True
                    break
                rep.log(f"  attempt {attempt}: marker not yet in the deployed zip")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:90]}")
            time.sleep(15)
        if not settled:
            rep.fail("  deploy never landed")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.ok(f"  State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')} "
               f"Timeout={cfg.get('Timeout')}")

        rep.section("3. invoke (real network I/O across ~18 chunks with backoff — may take a while)")
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = json.loads(resp["Payload"].read())
        body = json.loads(raw["body"]) if isinstance(raw, dict) and "body" in raw else raw
        rep.log(f"  invoke body: {json.dumps(body, default=str)[:500]}")
        if resp.get("FunctionError"):
            rep.fail(f"  invoke raised FunctionError: {raw}")
            sys.exit(1)

        rep.section("4. THE REAL GATE")
        after, alm = snapshot()
        rep.kv(after_n_observations=after.get("n_observations"),
               after_maturity=after.get("maturity"), after_s3=alm.isoformat(),
               overall=json.dumps(after.get("overall"), default=str)[:300])

        by_verdict = after.get("by_verdict") or {}
        rep.log(f"  by_verdict (first 3): {json.dumps(dict(list(by_verdict.items())[:3]), default=str)[:600]}")

        checks = [
            ("artifact rewritten this invoke", alm > blm),
            ("n_observations is a real positive number (was structurally 0)",
             (after.get("n_observations") or 0) > 0),
            ("maturity moved off BOOTSTRAPPING", after.get("maturity") != "BOOTSTRAPPING"),
            ("by_verdict has real, non-empty entries", len(by_verdict) > 0),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {after.get('n_observations')} real observations, "
               f"maturity={after.get('maturity')}")


if __name__ == "__main__":
    main()
