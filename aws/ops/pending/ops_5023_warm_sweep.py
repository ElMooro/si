"""ops 5023 — warm sweep: majors onto 2.9.3 now.

The nightly fleet rewrites the full universe, but until then any
first visitor pays the (few-second) live build. This op pre-warms
the most-searched names so every big ticker renders instantly from
the CDN. Threaded, refresh=1, asserts every doc lands on 2.9.3 with
gf_extras.
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config

from ops_report import report

FN = "justhodl-equity-research"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(connect_timeout=10, read_timeout=600,
                                 retries={"max_attempts": 0},
                                 max_pool_connections=12))

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
           "AVGO", "AMD", "ORCL", "KO", "PEP", "JPM", "V", "MA",
           "UNH", "XOM", "LLY", "WMT", "COST", "NFLX", "CRM", "PLTR",
           "INTC", "MU", "QCOM", "TSM", "BA", "DIS", "AAOI"]


def build(t):
    t0 = time.time()
    r = lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"queryStringParameters": {"ticker": t,
                                   "refresh": "1"}}).encode())
    body = json.loads(r["Payload"].read() or b"{}")
    doc = body
    if isinstance(body, dict) and isinstance(body.get("body"), str):
        doc = json.loads(body["body"])
    ok = isinstance(doc, dict) and \
        doc.get("schema_version") == "2.9.3" and \
        bool((doc.get("gf_extras") or {}).get("available"))
    return t, ok, round(time.time() - t0, 1), (
        doc.get("schema_version") if isinstance(doc, dict) else "err")


with report("ops_5023_warm_sweep") as rep:
    rep.heading("ops 5023 -- warm sweep to 2.9.3 (30 majors)")
    rep.section("P1 threaded builds")
    t0 = time.time()
    bad = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = [ex.submit(build, t) for t in TICKERS]
        for f in as_completed(futs):
            t, ok, dt, sch = f.result()
            (rep.ok if ok else rep.fail)("%s %ss %s" % (t, dt, sch))
            if not ok:
                bad.append(t)
    if bad:
        rep.section("P2 sequential retry of failures")
        still = []
        for t in bad:
            time.sleep(2)
            t2, ok, dt, sch = build(t)
            (rep.ok if ok else rep.fail)(
                "retry %s %ss %s" % (t2, dt, sch))
            if not ok:
                still.append(t2)
        bad = still
    rep.kv(total_s=round(time.time() - t0, 1), tickers=len(TICKERS),
           failed=len(bad))
    if bad:
        raise SystemExit("warm sweep failures after retry: %s"
                         % ",".join(bad))
    rep.ok("all 30 majors cached on 2.9.3 — instant CDN renders")
