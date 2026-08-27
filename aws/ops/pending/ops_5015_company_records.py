"""ops_5015 -- company records: the last GuruFocus summary panels.

Server: equity-research v2.7 adds `company_records` -- SEC filings
(dual-endpoint fallback), stock events (next-earnings countdown + past
report-day closes and next-session reactions), press releases,
executives, the Peter Lynch price-vs-15xEPS series, ISIN
cross-listings, and index membership (S&P 500 / Nasdaq-100 / Dow via a
shared 24h-cached constituent list; Russell honestly unavailable).
SCHEMA_CURRENT bumps to 2.7, so the ops-5014 version gate rolls it to
every ticker automatically. Client: why.html <script id="OPS5015">
with self-heal + in-place auto-upgrade. Real data only.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops5015"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def invoke(t):
    rsp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps(
                         {"queryStringParameters": {"ticker": t}}).encode())
    raw = rsp["Payload"].read().decode("utf-8", "replace")
    if rsp.get("FunctionError"):
        raise RuntimeError("FunctionError: %s" % raw[:300])
    body = json.loads(raw)
    return json.loads(body["body"]) if isinstance(body, dict) \
        and "body" in body else body


with report("ops_5015_company_records") as rep:
    rep.heading("ops 5015 -- company records (equity-research v2.7)")
    fails, warns = [], []

    rep.section("G0 preflight")
    src = (SRC / "lambda_function.py").read_text()
    for mark in ("JH-5015", "build_company_records", '"sec_filings_a"',
                 '"press_rel"', '"key_execs"', '"company_records":',
                 'SCHEMA_CURRENT = "2.7"'):
        if mark not in src:
            fails.append("lambda missing %r" % mark)
    page = (ROOT / "why.html").read_text()
    for mark in ('<script id="OPS5015">', "jh5015-wrap",
                 "Peter Lynch Chart"):
        if mark not in page:
            fails.append("why.html missing %r" % mark)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight failed")
    rep.ok("v2.7 markers and OPS5015 block present")

    rep.section("G1 deploy (code only)")
    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    rep.section("P1 real-data asserts (version gate regenerates on its own)")
    for t in ("AAOI", "NVDA"):
        tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
        t0 = time.time()
        doc = invoke(t)
        rep.kv(ticker=t, gen_s=round(time.time() - t0, 1),
               schema=doc.get("schema_version"),
               from_cache=doc.get("from_cache"))
        if doc.get("schema_version") != "2.7":
            fails.append(tag("schema %r != 2.7" % doc.get("schema_version")))
            continue
        R = doc.get("company_records") or {}
        if not R.get("available"):
            fails.append(tag("company_records unavailable"))
            continue
        Ev = R.get("events") or {}
        if not Ev.get("available") or len(Ev.get("past") or []) < 4:
            fails.append(tag("events: past<4 or unavailable: %s"
                             % Ev.get("reason")))
        else:
            reacts = [r for r in Ev["past"]
                      if r.get("react_next_pct") is not None]
            if len(reacts) < 3:
                fails.append(tag("fewer than 3 computed reactions"))
            rep.kv(ticker=t,
                   next_earnings=(Ev.get("next") or {}).get("date"),
                   days=(Ev.get("next") or {}).get("days"),
                   past_events=len(Ev["past"]))
        Ly = R.get("lynch") or {}
        if not Ly.get("available"):
            fails.append(tag("lynch unavailable: %s" % Ly.get("reason")))
        else:
            rep.kv(ticker=t, lynch_years=len(Ly["years"]),
                   pos_eps_years=Ly["pos_eps_years"])
            if t == "AAOI" and Ly["pos_eps_years"] < 2:
                fails.append(tag("AAOI should have >=2 profitable years "
                                 "(2016/2017)"))
        for name, need in (("filings", 3), ("press", 1),
                           ("executives", 3)):
            blk = R.get(name) or {}
            n = len(blk.get("rows") or [])
            if not blk.get("available") or n < need:
                (warns if name == "press" else fails).append(
                    tag("%s: %d rows (need %d) -- %s"
                        % (name, n, need, blk.get("reason"))))
            else:
                rep.kv(ticker=t, **{name: n})
        Li = R.get("listings") or {}
        if Li.get("available"):
            rep.kv(ticker=t, listings=len(Li["rows"]))
        else:
            warns.append(tag("listings: %s" % Li.get("reason")))
        M = R.get("index_membership") or {}
        if not M.get("available") or len(M.get("checked") or []) < 3:
            fails.append(tag("index membership checks incomplete: %s"
                             % M.get("reason")))
        elif t == "NVDA" and "S&P 500" not in (M.get("member_of") or []):
            fails.append(tag("NVDA must be an S&P 500 member"))
        else:
            rep.kv(ticker=t, member_of=",".join(M.get("member_of") or [])
                   or "(none)")
        rep.ok(tag("company records checked"))
    for wmsg in warns:
        rep.warn(wmsg)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("real-data asserts failed")

    rep.section("G2 live page carries OPS5015")
    deadline = time.time() + 300
    ok = False
    while time.time() < deadline:
        try:
            pv = http("https://justhodl.ai/why.html?cb=%d" % int(time.time()))
            if '<script id="OPS5015">' in pv and "Peter Lynch Chart" in pv:
                ok = True
                break
            rep.log("waiting for site sync")
        except Exception as e:
            rep.log("live fetch: %s" % e)
        time.sleep(15)
    if not ok:
        rep.fail("site never served OPS5015")
        raise SystemExit("live check failed")
    rep.ok("served page carries the Peter Lynch chart + company records")
    rep.ok("OPS 5015 PASS -- every GuruFocus summary panel from the "
           "screenshots is now built; v2.7 rolls to all tickers via the "
           "version gate + in-place auto-upgrade")
# rerun: verify EDGAR-fallback filings on real data (15:16)
