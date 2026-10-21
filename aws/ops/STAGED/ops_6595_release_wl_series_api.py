"""ops 6595 - release the anomaly hold on justhodl-wl-series-api once its cause is gone from the served site.

Khalid's screenshot, 2026-10-21 08:07 CT: the anomaly sentinel held the watchlist-series Function URL after 4,894
invocations in an hour (twice in a row).  ops 6591/6592: every one of them was the nav-drawer `?diag=1` telemetry
beacon (ops 3276) fired by the Lightpanda/1.0 crawler looping over forensic / bottom / why / share-flows pages -
not a single series request.  The beacon was removed from jh-nav-drawer.js (f89778e9; Pages run 39607928969 green).

Here: (1) fetch the served /jh-nav-drawer.js and refuse to continue if the beacon is still in it; (2) release the
hold ({"action":"release_anomaly"} - the early release the governor's own e-mail describes); (3) read back the
function's reserved concurrency and the governor's status.  The sentinel keeps watching the API like any engine.
"""
from pathlib import Path
import json
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
API = "justhodl-wl-series-api"


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def gov(payload):
    lam = c("lambda")
    for _ in range(8):
        try:
            r = lam.invoke(FunctionName="justhodl-spend-governor", Payload=json.dumps(payload).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            if r.get("FunctionError"):
                raise RuntimeError("governor error: %s" % json.dumps(body)[:300])
            return body
        except lam.exceptions.TooManyRequestsException:
            time.sleep(15)
    raise RuntimeError("governor busy")


def main():
    lam = c("lambda")
    with report("ops_6595_release_wl_series_api") as r:
        r.heading("ops 6595 - release the wl-series-api anomaly hold (cause removed from the served site)")
        req = urllib.request.Request("https://justhodl.ai/jh-nav-drawer.js?v=%d" % int(time.time()), headers={"User-Agent": "justhodl-ops/6595", "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            js = resp.read().decode("utf-8", errors="replace")
        beacon = "?diag=1" in js or "lambda-url.us-east-1.on.aws" in js
        r.log("served jh-nav-drawer.js: %d bytes, beacon present: %s, ops 6592 note present: %s" % (len(js), beacon, "ops 6592" in js))
        if beacon:
            r.fail("the served nav drawer still carries the beacon - not releasing")
            raise SystemExit(1)
        before = gov({"action": "status"})
        r.log("before: anomaly_held %s; concurrency %s" % (json.dumps(before.get("anomaly_held")), lam.get_function_concurrency(FunctionName=API).get("ReservedConcurrentExecutions")))
        if not any(x[0] == API for x in (before.get("anomaly_held") or [])):
            r.ok("no anomaly hold on %s any more (already released)" % API)
            return
        res = gov({"action": "release_anomaly", "function": API})
        r.log("release_anomaly: %s" % json.dumps(res)[:600])
        time.sleep(5)
        after = gov({"action": "status"})
        conc = lam.get_function_concurrency(FunctionName=API).get("ReservedConcurrentExecutions")
        r.log("after: anomaly_held %s; concurrency %s" % (json.dumps(after.get("anomaly_held")), conc))
        held = any(x[0] == API for x in (after.get("anomaly_held") or []))
        r.kv(released=not held, concurrency_after=conc, beacon_gone=not beacon)
        if held or conc == 0:
            r.fail("still held (concurrency %s)" % conc)
            raise SystemExit(1)
        r.ok("%s released; reserved concurrency %s; the beacon is gone from the served site" % (API, conc))


if __name__ == "__main__":
    main()
