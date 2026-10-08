"""ops 4678 — tv-bars rev-2 proof: does the socket open now?

4674 created the engine and proved everything except the socket: the
handshake 400'd because Sec-WebSocket-Key was the static RFC6455
example nonce. rev-2 sends a fresh random nonce and falls back across
data/prodata/widgetdata endpoints, reporting each rejection.

Deploys the code itself (deploy-lambdas only creates/updates reliably
on its own trigger; ops owns the outcome), then pulls 3 ICE symbols and
contracts on pre-2020 history actually landing.
"""
import gzip
import io
import json
import sys
import time
import zipfile

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
FN = "justhodl-tv-bars"
SRC = "aws/lambdas/justhodl-tv-bars/source/lambda_function.py"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
TEST = ["BAMLH0A0HYM2", "BAMLC0A2CAA", "BAMLH0A3HYC"]


def gj(key, gz=False):
    try:
        raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
        return json.loads(gzip.decompress(raw) if gz else raw)
    except Exception:
        return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4678_tv_bars_rev2") as r:
        r.heading("ops 4678 — tv-bars rev-2 (handshake nonce fix)")
        misses = 0

        r.section("1. Push rev-2 code")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr("lambda_function.py", open(SRC).read())
        lam.update_function_code(FunctionName=FN,
                                 ZipFile=buf.getvalue())
        t0 = time.time()
        ok = False
        while time.time() - t0 < 240:
            c = lam.get_function(FunctionName=FN)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" and \
                    c.get("State") == "Active":
                ok = True
                break
            time.sleep(8)
        misses += contract(r, "deploy", ok, "rev-2 code live")

        r.section("2. Pull 3 ICE symbols")
        resp = lam.invoke(FunctionName=FN,
                          InvocationType="RequestResponse",
                          Payload=json.dumps(
                              {"symbols": TEST}).encode())
        raw = resp["Payload"].read().decode("utf-8", "replace")
        r.log("  handler: %s" % raw[:700])

        r.section("3. What landed")
        deep = 0
        for sid in TEST:
            d = gj("data/warm/tv-bars/%s.json.gz" % sid, gz=True)
            if not d:
                r.log("  %s: nothing banked" % sid)
                continue
            r.log("  %s: n=%s %s -> %s"
                  % (sid, d.get("n"), d.get("first_date"),
                     d.get("last_date")))
            if str(d.get("first_date") or "9999") < "2020":
                deep += 1
        st = gj("data/warm/tv-bars/_state.json")
        fails = st.get("failures") or {}
        for k, v in list(fails.items())[:4]:
            r.log("  fail %s: %s" % (k, str(v)[:230]))
        misses += contract(r, "socket", not any(
            "handshake" in str(v) for v in fails.values()),
            "socket opened (no handshake rejection)")
        misses += contract(r, "depth", deep >= 1,
                           "%d/%d symbols carry pre-2020 history"
                           % (deep, len(TEST)))

        r.section("verdict")
        if misses:
            r.fail("rev-2: %d red — endpoint rejections above are the "
                   "next lead" % misses)
            sys.exit(1)
        r.ok("TV history rail LIVE — ICE gap closable without a "
             "browser; hourly schedule converges the rest")


if __name__ == "__main__":
    main()
