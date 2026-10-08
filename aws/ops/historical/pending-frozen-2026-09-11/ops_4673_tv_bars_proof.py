"""ops 4673 — prove the server-side TV bar rail on 3 ICE symbols.

Khalid: "you should deploy code autonomously" — correct, and the
extension route violated that (it needed a manual reload). The new
justhodl-tv-bars engine speaks TV's WebSocket protocol from Lambda
using the session already in SSM. This op proves it end-to-end on
three symbols before any convergence run: does the session still
authenticate, does the handshake work from Lambda's network, and does
TV actually serve pre-2023 ICE history as Khalid observes in his
account?

Contracts are deliberately harsh — a rail that returns 2023-only bars
is worth nothing here, so the op fails unless real pre-2020 history
lands.
"""
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

from ops_report import report

B = "justhodl-dashboard-live"
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
    with report("4673_tv_bars_proof") as r:
        r.heading("ops 4673 — server-side TV bar rail, 3-symbol proof")
        misses = 0

        r.section("1. Session present?")
        ssm = boto3.client("ssm", region_name="us-east-1")
        have = {}
        for p in ("sessionid", "sessionid_sign", "device_t",
                  "auth_token"):
            try:
                v = ssm.get_parameter(
                    Name="/justhodl/tradingview/%s" % p,
                    WithDecryption=True)["Parameter"]["Value"]
                have[p] = len(v or "")
            except Exception:
                have[p] = 0
        r.log("  SSM lengths (values never printed): %s" % have)
        misses += contract(r, "session", have.get("sessionid", 0) > 10,
                           "sessionid present (len=%d)"
                           % have.get("sessionid", 0))

        r.section("2. Settle deploy")
        t0 = time.time()
        ok = False
        while time.time() - t0 < 300:
            try:
                c = lam.get_function(
                    FunctionName="justhodl-tv-bars")["Configuration"]
                if c.get("State") == "Active" and \
                        c.get("LastUpdateStatus") == "Successful":
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(10)
        misses += contract(r, "deploy", ok,
                           "justhodl-tv-bars is Active")
        if not ok:
            r.fail("  engine not deployed — cannot prove the rail")
            sys.exit(1)

        r.section("3. Pull 3 ICE symbols (sync)")
        resp = lam.invoke(FunctionName="justhodl-tv-bars",
                          InvocationType="RequestResponse",
                          Payload=json.dumps(
                              {"symbols": TEST}).encode())
        raw = resp["Payload"].read().decode("utf-8", "replace")
        r.log("  handler: %s" % raw[:400])
        if resp.get("FunctionError"):
            misses += contract(r, "pull", False,
                               "handler error: %s" % raw[:250])

        r.section("4. What actually landed")
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
        r.log("  state: done=%d catalog=%s failures=%s"
              % (len(set(st.get("done") or [])),
                 len(st.get("catalog") or []),
                 dict(list((st.get("failures") or {}).items())[:4])))
        misses += contract(r, "depth", deep >= 1,
                           "%d/%d symbols carry pre-2020 history "
                           "(Khalid sees inception in his account — "
                           "this is the test of whether the rail "
                           "reaches it)" % (deep, len(TEST)))

        r.section("verdict")
        if misses:
            r.fail("tv bar rail: %d red — failures above are the "
                   "protocol evidence for the next revision" % misses)
            sys.exit(1)
        r.ok("server-side TV history rail PROVEN — no browser, no "
             "extension reload; convergence can run on the hourly "
             "schedule")


if __name__ == "__main__":
    main()
