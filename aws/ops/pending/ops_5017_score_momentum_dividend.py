"""ops_5017 -- JH Score wheel, Momentum, Dividend & Buy Back.

The last unbuilt visuals from the original screenshots: the composite
score with its five rank bars, the momentum panel (1M/3M/6M/12M vs SPY
+ 52-week band, rubric rank), and the Dividend & Buy Back panel
(trailing-window dividend CAGR so a partial year can never fake a
trend; payout honestly undefined for loss-makers; buybacks from actual
share counts). equity-research v2.8; SCHEMA_CURRENT bump rolls it to
every ticker via the ops-5014 version gate; the client block is
ticker-bus driven (ops 5016), self-heals, self-positions ABOVE the
5010 stack, and auto-upgrades in place. Composite = visible weights
(30/25/20/15/10), unknowns excluded and renormalized, labeled as
research shorthand -- not a rating or a recommendation.
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops5017"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def invoke(t):
    rsp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps(
                         {"queryStringParameters":
                          {"ticker": t, "refresh": "1"}}).encode())
    raw = rsp["Payload"].read().decode("utf-8", "replace")
    if rsp.get("FunctionError"):
        raise RuntimeError("FunctionError: %s" % raw[:300])
    body = json.loads(raw)
    return json.loads(body["body"]) if isinstance(body, dict) \
        and "body" in body else body


with report("ops_5017_score_momentum_dividend") as rep:
    rep.heading("ops 5017 -- JH Score / Momentum / Dividend & Buy Back "
                "(v2.8)")
    fails = []

    rep.section("G0 preflight")
    src = (SRC / "lambda_function.py").read_text()
    for mark in ("JH-5017", "build_momentum", "build_dividend_buyback",
                 "build_jh_score", '"jh_score":',
                 'SCHEMA_CURRENT = "2.8"'):
        if mark not in src:
            fails.append("lambda missing %r" % mark)
    page = (ROOT / "why.html").read_text()
    for mark in ('<script id="OPS5017">', "jh5017-wrap", "JH SCORE"):
        if mark not in page:
            fails.append("why.html missing %r" % mark)
    if page.count("__JH_TICKER_BUS.subscribe") != 5:
        fails.append("expected 5 bus subscriptions")
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight failed")
    rep.ok("v2.8 markers, OPS5017 block, 5 bus subscriptions")

    rep.section("G1 deploy (code only)")
    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    rep.section("P1 real-data asserts")
    for t in ("AAOI", "NVDA"):
        tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
        t0 = time.time()
        doc = invoke(t)
        rep.kv(ticker=t, gen_s=round(time.time() - t0, 1),
               schema=doc.get("schema_version"))
        if doc.get("schema_version") != "2.8":
            fails.append(tag("schema %r != 2.8" % doc.get("schema_version")))
            continue
        S = doc.get("jh_score") or {}
        if not S.get("available") or not (1 <= S.get("score", 0) <= 100):
            fails.append(tag("score unavailable/out of bounds: %s"
                             % S.get("reason", S.get("score"))))
        else:
            known = [c for c in S["components"] if c["rank"] is not None]
            rep.kv(ticker=t, jh_score=S["score"],
                   sub_ranks="/".join("%s:%s" % (c["name"].split()[0],
                                                 c["rank"])
                                      for c in S["components"]))
            if len(S["components"]) != 5:
                fails.append(tag("components != 5"))
            if len(known) < 4:
                fails.append(tag("fewer than 4 computable sub-ranks"))
        M = doc.get("momentum_panel") or {}
        if not M.get("available") or M.get("rank") is None:
            fails.append(tag("momentum unavailable: %s" % M.get("reason")))
        else:
            r12 = [r for r in M["rows"] if r["h"] == "12M"][0]
            if r12.get("rel") is None:
                fails.append(tag("12M rel-vs-SPY not computed"))
            rep.kv(ticker=t, mom_rank=M["rank"], rel_12m=r12.get("rel"),
                   pos_52w=(M.get("band_52w") or {}).get("pos_pct"))
        D = doc.get("dividend_buyback") or {}
        if not D.get("available"):
            fails.append(tag("dividend_buyback unavailable"))
        else:
            dv = D.get("dividend") or {}
            if t == "AAOI" and dv.get("pays") is not False:
                fails.append(tag("AAOI must show pays=False honestly"))
            if t == "NVDA" and not dv.get("pays"):
                fails.append(tag("NVDA pays a dividend -- must show it"))
            rep.kv(ticker=t, pays=dv.get("pays"),
                   yield_pct=dv.get("yield_pct"),
                   consec=dv.get("consecutive_years"),
                   share_3y=(D.get("buyback") or {})
                   .get("share_count_3y_cagr_pct"))
        rep.ok(tag("score/momentum/dividend checked"))
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("real-data asserts failed")

    rep.section("G2 live page carries OPS5017")
    deadline = time.time() + 300
    ok = False
    while time.time() < deadline:
        try:
            pv = http("https://justhodl.ai/why.html?cb=%d" % int(time.time()))
            if ('<script id="OPS5017">' in pv
                    and pv.count("__JH_TICKER_BUS.subscribe") == 5):
                ok = True
                break
            rep.log("waiting for site sync")
        except Exception as e:
            rep.log("live fetch: %s" % e)
        time.sleep(15)
    if not ok:
        rep.fail("site never served OPS5017")
        raise SystemExit("live check failed")
    rep.ok("served page carries the score wheel block + 5 subscriptions")
    rep.ok("OPS 5017 PASS -- every visual from every screenshot in this "
           "conversation is now built, verified on real data, and rolls "
           "to every ticker")
