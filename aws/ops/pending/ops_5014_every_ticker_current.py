"""ops_5014 -- every ticker, current engine: version-gated cache +
in-place auto-upgrade.

Why: statement flows (and the 5010/5011 layers) only showed for tickers
whose docs happened to be regenerated -- the cache gate accepted any
<24h doc regardless of schema, and the page told users to reload.

Server: the doc cache is now a MISS whenever the cached schema_version
differs from SCHEMA_CURRENT (single constant, also used at assembly) --
so any request for any ticker regenerates on the spot; every future
schema bump rolls out the same way with zero per-ticker ops.
Client: all three why.html layers (OPS5010/5011/5013) fire refresh=1,
poll the CDN, and draw in place when the current doc lands -- no manual
reload.

Proof below is behavioral, on real tickers never touched by prior ops
runs: MSFT is invoked PLAIN (no refresh, no cache bust) -- the exact
path that used to serve stale -- and must come back freshly generated
at the current schema with reconciling statement flows. GOOGL proves
the refresh=1 path.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-equity-research"
SRC = Path(__file__).resolve().parents[2] / "lambdas" / FN / "source"
ROOT = Path(__file__).resolve().parents[3]

lam = boto3.client("lambda", region_name=REGION, config=Config(
    connect_timeout=10, read_timeout=600, retries={"max_attempts": 0}))


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5014"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def invoke(t, refresh=False):
    qs = {"ticker": t}
    if refresh:
        qs["refresh"] = "1"
    rsp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps(
                         {"queryStringParameters": qs}).encode())
    raw = rsp["Payload"].read().decode("utf-8", "replace")
    if rsp.get("FunctionError"):
        raise RuntimeError("FunctionError: %s" % raw[:300])
    body = json.loads(raw)
    return json.loads(body["body"]) if isinstance(body, dict) \
        and "body" in body else body


def check_flows(rep, fails, t, doc):
    tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
    if doc.get("schema_version") != "2.6":
        fails.append(tag("schema %r != 2.6" % doc.get("schema_version")))
        return
    if doc.get("from_cache") is True:
        fails.append(tag("served from cache -- version gate did not fire"))
        return
    sf = doc.get("statement_flows") or {}
    if not sf.get("available"):
        fails.append(tag("statement_flows unavailable: %s" % sf.get("reason")))
        return
    p0 = (sf.get("periods") or [{}])[0]
    I, Bl, Cf = p0.get("income"), p0.get("balance"), p0.get("cashflow")
    if not (I and Bl and Cf):
        fails.append(tag("latest period missing a statement"))
        return
    rev, ta = I["revenue"], Bl["total_assets"]
    if abs(rev - I["gross_profit"] - I["cogs"]) > 0.01 * rev:
        fails.append(tag("income does not tie"))
    if abs(ta - Bl["liab"]["total"] - Bl["equity"]["total"]) > 0.01 * ta:
        fails.append(tag("balance does not tie"))
    scale = max(abs(Cf["cfo"]), abs(Cf["cfi"] or 0), abs(Cf["cff"] or 0), 1.0)
    if abs(Cf["cfo"] + (Cf["cfi"] or 0) + (Cf["cff"] or 0) + Cf["fx"]
           - (Cf["net_change"] or 0)) > 0.03 * scale:
        fails.append(tag("cashflow does not tie"))
    rep.kv(ticker=t, schema=doc.get("schema_version"),
           fy=p0.get("label"), revenue_m=round(rev / 1e6, 1),
           periods=len(sf.get("periods") or []))
    rep.ok(tag("fresh current-schema doc with reconciling flows"))


with report("ops_5014_every_ticker_current") as rep:
    rep.heading("ops 5014 -- version-gated cache + in-place auto-upgrade")
    fails = []

    rep.section("G0 preflight")
    src = (SRC / "lambda_function.py").read_text()
    for mark in ('SCHEMA_CURRENT = "2.6"',
                 'cached.get("schema_version") == SCHEMA_CURRENT',
                 '"schema_version": SCHEMA_CURRENT'):
        if mark not in src:
            fails.append("lambda missing %r" % mark)
    page = (ROOT / "why.html").read_text()
    if page.count("ops 5014:") != 3:
        fails.append("why.html ops-5014 markers != 3")
    if page.count("appear here automatically") != 3:
        fails.append("why.html auto-upgrade notes != 3")
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight failed")
    rep.ok("version gate, schema constant, and 3 auto-upgrade layers present")

    rep.section("G1 deploy (code only)")
    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    rep.section("P1 behavioral proof on untouched tickers")
    t0 = time.time()
    doc = invoke("MSFT", refresh=False)   # plain path -- the one that
    rep.kv(ticker="MSFT", gen_s=round(time.time() - t0, 1))  # used to be stale
    check_flows(rep, fails, "MSFT", doc)
    t0 = time.time()
    doc = invoke("GOOGL", refresh=True)
    rep.kv(ticker="GOOGL", gen_s=round(time.time() - t0, 1))
    check_flows(rep, fails, "GOOGL", doc)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("behavioral proof failed")

    rep.section("G2 live page carries the auto-upgrade layers")
    deadline = time.time() + 300
    ok = False
    while time.time() < deadline:
        try:
            pv = http("https://justhodl.ai/why.html?cb=%d" % int(time.time()))
            if pv.count("appear here automatically") == 3:
                ok = True
                break
            rep.log("waiting for site sync")
        except Exception as e:
            rep.log("live fetch: %s" % e)
        time.sleep(15)
    if not ok:
        rep.fail("site never served the 3 auto-upgrade layers")
        raise SystemExit("live check failed")
    rep.ok("served page carries all 3 auto-upgrade layers")
    rep.ok("OPS 5014 PASS -- every ticker now serves the current engine: "
           "stale-schema cache is a miss server-side, and the page "
           "upgrades itself in place")
