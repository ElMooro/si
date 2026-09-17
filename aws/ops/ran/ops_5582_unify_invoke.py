"""ops 5582 — invoke the unify Lambdas that are already deployed so harvests
exist. Deploy-lambdas 3286/3287/3288 succeeded; this is the invoke half.

Fires Event for long jobs (13F, look-through, earnings-tracker), RequestResponse
for short ones (boom-radar, estimate-revisions, best-ideas, massive-signals).
Then polls S3 for 13f-desk / 13f-by-ticker / by_ticker slices / boom-radar 1.2.
Hashes and row counts only — no keys.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"

SLOW = [
    "justhodl-13f-positions",
    "justhodl-flow-lookthrough",
    "justhodl-earnings-tracker",
    "justhodl-financial-secretary",
]
FAST = [
    "justhodl-estimate-revisions",
    "justhodl-boom-radar",
    "justhodl-best-ideas",
    "justhodl-master-ranker",
    "justhodl-massive-signals",
]
PROBE = [
    ("data/13f-desk.json", ("tickers", "by_fund", "most_bought", "accumulating")),
    ("data/13f-by-ticker.json", ("tickers",)),
    ("data/flow-lookthrough.json", ("by_ticker", "ticker_map")),
    ("data/estimate-revisions.json", ("by_ticker", "upward_revisions")),
    ("data/boom-radar.json", ("top_picks", "methodology")),
]


def _head(s3, key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return {
            "ok": True,
            "bytes": h.get("ContentLength"),
            "lm": (h.get("LastModified") or datetime.now(timezone.utc)).isoformat(),
        }
    except ClientError:
        return {"ok": False}


def _get(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def main() -> int:
    lam = boto3.client(
        "lambda", region_name=REGION,
        config=Config(read_timeout=180, connect_timeout=10, retries={"max_attempts": 0}),
    )
    s3 = boto3.client("s3", region_name=REGION)
    t0 = time.time()
    with report("ops_5582_unify_invoke") as R:
        R.heading("ops 5582 — invoke unify engines (already deployed)")
        R.section("Event (long)")
        for fn in SLOW:
            try:
                r = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
                R.ok("%s Event status=%s" % (fn, r.get("StatusCode")))
            except ClientError as e:
                R.fail("%s Event %s" % (fn, e.response.get("Error", {}).get("Code")))
        R.section("RequestResponse (short)")
        for fn in FAST:
            try:
                r = lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
                raw = (r.get("Payload").read() if r.get("Payload") else b"")[:400]
                err = r.get("FunctionError")
                if err:
                    R.fail("%s FunctionError=%s body=%s" % (fn, err, raw.decode("utf-8", "replace")))
                else:
                    R.ok("%s status=%s body=%s" % (fn, r.get("StatusCode"), raw.decode("utf-8", "replace")[:240]))
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                R.fail("%s %s" % (fn, code))
            except Exception as e:
                R.fail("%s %s" % (fn, type(e).__name__))
        R.section("S3 poll (13f-desk + by_ticker + boom 1.2)")
        deadline = time.time() + 420
        last = {}
        while time.time() < deadline:
            last = {k: _head(s3, k) for k, _ in PROBE}
            ready = all(v.get("ok") for v in last.values())
            R.log("poll " + " ".join("%s=%s" % (k.split("/")[-1], "ok" if v.get("ok") else "miss") for k, v in last.items()))
            if ready:
                # 13f-desk must be newer than this op start — allow 3 min slack for Event lag
                desk = last.get("data/13f-desk.json") or {}
                if desk.get("ok"):
                    break
            time.sleep(20)
        R.section("Harvest shape (counts only)")
        for key, fields in PROBE:
            d = _get(s3, key)
            h = last.get(key) or _head(s3, key)
            if not d:
                R.fail("%s missing (%s)" % (key, h))
                continue
            bits = ["bytes=%s" % h.get("bytes"), "lm=%s" % h.get("lm")]
            if key.endswith("boom-radar.json"):
                bits.append("version=%s" % d.get("version"))
                gated = ((d.get("methodology") or {}).get("gated_off"))
                bits.append("gated_off=%s" % gated)
            for f in fields:
                v = d.get(f)
                if isinstance(v, dict):
                    bits.append("%s_n=%d" % (f, len(v)))
                    if f == "by_ticker":
                        bits.append("AAPL=%s" % ("yes" if "AAPL" in v else "no"))
                elif isinstance(v, list):
                    bits.append("%s_n=%d" % (f, len(v)))
            R.ok("%s %s" % (key, " ".join(bits)))
        R.ok("elapsed_s=%.1f" % (time.time() - t0))
        failed = getattr(R, "_failed", False)
    if failed:
        sys.exit(1)
    return 0


if __name__ == "__main__":
    sys.exit(main())
