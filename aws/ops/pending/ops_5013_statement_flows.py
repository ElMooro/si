"""ops_5013 -- statement flows: GuruFocus-style Sankey breakdowns of the
income statement, balance sheet and cash-flow statement.

Server: equity-research v2.6 adds an audited `statement_flows` block
(latest FY + prior FY + latest quarter; explicit residual lines; per-
period reconciliation flags) and two new quarterly FMP fetches.
Client: shared engine assets/jh-flows.js (JHFlows.render / .statements)
usable by every research page; why.html wires it via <script
id="OPS5013"> with the ops-5012 self-heal pattern. Real data only --
every assert below runs against the live FMP-backed payload and the
served site.
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
B = "justhodl-dashboard-live"
FN = "justhodl-equity-research"
SRC = Path(__file__).resolve().parents[2] / "lambdas" / FN / "source"
ROOT = Path(__file__).resolve().parents[3]
TICKERS = ["AAOI", "NVDA"]

lam = boto3.client("lambda", region_name=REGION, config=Config(
    connect_timeout=10, read_timeout=600, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name=REGION)


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5013"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def get_doc(rep, t):
    try:
        rsp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps(
                             {"queryStringParameters": {"ticker": t}}).encode())
        raw = rsp["Payload"].read().decode("utf-8", "replace")
        if rsp.get("FunctionError"):
            raise RuntimeError("FunctionError: %s" % raw[:300])
        body = json.loads(raw)
        return json.loads(body["body"]) if isinstance(body, dict) \
            and "body" in body else body
    except Exception as e:
        rep.warn("%s sync invoke failed (%s) -- async+S3 fallback"
                 % (t, str(e)[:140]))
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps(
                       {"_internal": "1", "ticker": t}).encode())
        key = "equity-research/%s.json" % t
        for _ in range(24):
            time.sleep(15)
            try:
                obj = s3c.get_object(Bucket=B, Key=key)
                return json.loads(obj["Body"].read())
            except Exception:
                continue
        raise RuntimeError("%s: no document after async fallback" % t)


with report("ops_5013_statement_flows") as rep:
    rep.heading("ops 5013 -- statement flows (equity-research v2.6 + "
                "assets/jh-flows.js)")
    fails = []

    rep.section("G0 preflight -- repo carries every piece")
    src_txt = (SRC / "lambda_function.py").read_text()
    for mark in ("JH-5013", "build_statement_flows",
                 '"balance_quarterly"', '"cashflow_quarterly"',
                 '"statement_flows":', '"schema_version": "2.6"'):
        if mark not in src_txt:
            fails.append("lambda source missing %r" % mark)
    lib = (ROOT / "assets" / "jh-flows.js").read_text()
    for mark in ("window.JHFlows", "v:'5013'", "mapIncome", "mapBalance",
                 "mapCash"):
        if mark not in lib:
            fails.append("jh-flows.js missing %r" % mark)
    page = (ROOT / "why.html").read_text()
    for mark in ('<script id="OPS5013">', "assets/jh-flows.js?v=5013",
                 "jh5013-wrap"):
        if mark not in page:
            fails.append("why.html missing %r" % mark)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight failed")
    rep.ok("lambda v2.6 markers, shared lib, and OPS5013 block present")

    rep.section("G1 deploy v2.6 (code only)")
    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    rep.section("P1 regenerate %s with real data" % "+".join(TICKERS))
    for t in TICKERS:
        try:
            s3c.delete_object(Bucket=B, Key="equity-research/%s.json" % t)
        except Exception:
            pass
        rep.log("doc cache busted for %s" % t)
    docs = {}
    for t in TICKERS:
        t0 = time.time()
        docs[t] = get_doc(rep, t)
        rep.kv(ticker=t, gen_s=round(time.time() - t0, 1),
               schema=docs[t].get("schema_version"))

    rep.section("P2 statement_flows reconciliation on real data")
    for t, doc in docs.items():
        tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
        if doc.get("schema_version") != "2.6":
            fails.append(tag("schema %r != 2.6" % doc.get("schema_version")))
            continue
        sf = doc.get("statement_flows") or {}
        if not sf.get("available"):
            fails.append(tag("statement_flows unavailable: %s"
                             % sf.get("reason")))
            continue
        ps = sf.get("periods") or []
        if len(ps) < 2:
            fails.append(tag("only %d periods" % len(ps)))
            continue
        kinds = {p.get("kind") for p in ps}
        if "annual" not in kinds or "quarter" not in kinds:
            fails.append(tag("period kinds %r" % kinds))
        p0 = ps[0]
        I, Bl, Cf = p0.get("income"), p0.get("balance"), p0.get("cashflow")
        if not (I and Bl and Cf):
            fails.append(tag("latest FY missing a statement: %s"
                             % [bool(I), bool(Bl), bool(Cf)]))
            continue
        rev = I["revenue"]
        if abs(rev - I["gross_profit"] - I["cogs"]) > 0.01 * rev:
            fails.append(tag("income does not tie: rev != gp + cogs"))
        if not I.get("recon_ok"):
            fails.append(tag("income recon_ok False"))
        ta = Bl["total_assets"]
        if abs(ta - Bl["cur"]["total"] - Bl["lt"]["total"]) > 0.01 * ta:
            fails.append(tag("balance: assets != current + LT"))
        if abs(ta - Bl["liab"]["total"] - Bl["equity"]["total"]) > 0.01 * ta:
            fails.append(tag("balance: assets != liabilities + equity"))
        scale = max(abs(Cf["cfo"]), abs(Cf["cfi"] or 0),
                    abs(Cf["cff"] or 0), 1.0)
        if abs(Cf["cfo"] + (Cf["cfi"] or 0) + (Cf["cff"] or 0) + Cf["fx"]
               - (Cf["net_change"] or 0)) > 0.03 * scale:
            fails.append(tag("cashflow: CFO+CFI+CFF+FX != net change"))
        if Cf.get("begin_cash") is not None and Cf.get("end_cash") is not None:
            if abs(Cf["begin_cash"] + Cf["net_change"] - Cf["end_cash"]) \
                    > 0.03 * max(abs(Cf["end_cash"]), scale):
                fails.append(tag("cashflow: begin + change != end"))
        segs = I.get("segments") or []
        rep.kv(ticker=t, periods=len(ps), fy=p0.get("label"),
               revenue_m=round(rev / 1e6, 1), segments=len(segs),
               end_cash_m=round((Cf.get("end_cash") or 0) / 1e6, 1))
        if t == "AAOI" and len(segs) < 2:
            fails.append(tag("expected >=2 revenue segments on latest FY"))
        rep.ok(tag("all three statements reconcile"))

    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("real-data asserts failed")

    rep.section("G2 live -- lib served + page wired")
    deadline = time.time() + 300
    lib_ok = page_ok = False
    while time.time() < deadline and not (lib_ok and page_ok):
        try:
            if not lib_ok:
                lv = http("https://justhodl.ai/assets/jh-flows.js?cb=%d"
                          % int(time.time()))
                lib_ok = "window.JHFlows" in lv and "v:'5013'" in lv
            if not page_ok:
                pv = http("https://justhodl.ai/why.html?cb=%d"
                          % int(time.time()))
                page_ok = '<script id="OPS5013">' in pv
        except Exception as e:
            rep.log("live fetch: %s" % e)
        if not (lib_ok and page_ok):
            rep.log("waiting for site sync (lib=%s page=%s)"
                    % (lib_ok, page_ok))
            time.sleep(15)
    if not (lib_ok and page_ok):
        rep.fail("site sync incomplete (lib=%s page=%s)" % (lib_ok, page_ok))
        raise SystemExit("live checks failed")
    rep.ok("assets/jh-flows.js live and OPS5013 on the served page")
    rep.ok("OPS 5013 PASS -- income / balance-sheet / cash-flow Sankeys "
           "live on real data; shared engine available to every research "
           "page via assets/jh-flows.js")
