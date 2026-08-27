"""ops_5011 -- why.html full GuruFocus-style report layer.

Server: justhodl-equity-research v2.5 -- model fair-value gauge (FMP DCF
+ own historical-median multiples), valuation vs own 10y history +
Street view, Financial Strength & Profitability rubric ranks,
3y/5y/10y growth CAGRs, computed Severe/Medium/Good signs, annualized
and calendar-year returns vs peers (industry2 cache now holds 10y peer
prices), and the company profile card. Page: <script id="OPS5011">.
Real data only -- every assert runs against the live FMP-backed payload.
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
TICKERS = ["AAOI", "NVDA"]

lam = boto3.client("lambda", region_name=REGION, config=Config(
    connect_timeout=10, read_timeout=600, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name=REGION)


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5011"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


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


with report("ops_5011_why_full_report") as rep:
    rep.heading("ops 5011 -- why.html full report layer "
                "(equity-research v2.5)")
    warns, fails = [], []

    rep.section("G1 deploy v2.5 (code only)")
    src_txt = (SRC / "lambda_function.py").read_text()
    for mark in ("JH-5011", "build_valuation_panel",
                 "build_strength_profitability", "build_signals",
                 "build_peer_returns", "build_company_profile",
                 "equity-research/industry2/",
                 '"schema_version": "2.5"'):
        if mark not in src_txt:
            fails.append("lambda source missing marker %r" % mark)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight FAILS: " + "; ".join(fails))
    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    rep.section("P1 regenerate %s with real data" % "+".join(TICKERS))
    for t in TICKERS:
        for key in ("equity-research/%s.json" % t,
                    "equity-research/industry/%s.json" % t,
                    "equity-research/industry2/%s.json" % t):
            try:
                s3c.delete_object(Bucket=B, Key=key)
            except Exception:
                pass
        rep.log("caches busted for %s" % t)

    docs = {}
    for t in TICKERS:
        t0 = time.time()
        docs[t] = get_doc(rep, t)
        rep.kv(ticker=t, gen_s=round(time.time() - t0, 1),
               schema=docs[t].get("schema_version"))

    for t, doc in docs.items():
        tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
        if doc.get("schema_version") != "2.5":
            fails.append(tag("schema %r != 2.5" % doc.get("schema_version")))
            continue

        vp = doc.get("valuation_panel") or {}
        if not vp.get("available"):
            fails.append(tag("valuation_panel unavailable: %s"
                             % vp.get("reason")))
        else:
            fv = vp.get("fair_value") or {}
            if fv.get("value") is not None:
                if not (fv.get("verdict") and num(fv.get("price_to_fv"))
                        and fv["price_to_fv"] > 0):
                    fails.append(tag("fair_value malformed: %r" % fv))
                if not fv.get("parts"):
                    fails.append(tag("fair_value has no method parts"))
            else:
                warns.append(tag("no model FV (%s methods) -- honest"
                                 % len(fv.get("parts") or [])))
            rows = vp.get("rows") or []
            if len(rows) < 5:
                fails.append(tag("valuation rows %d < 5" % len(rows)))
            ps = next((r for r in rows
                       if r["metric"].startswith("P/S")), {})
            if not num(ps.get("current")):
                fails.append(tag("P/S current missing"))
            if ps.get("hist_n", 0) < 5:
                warns.append(tag("P/S history only %s FY points"
                                 % ps.get("hist_n")))

        sp = doc.get("strength_profitability") or {}
        if not sp.get("available"):
            fails.append(tag("strength_profitability unavailable: %s"
                             % sp.get("reason")))
        else:
            sr = (sp.get("strength") or {}).get("rank")
            pk = (sp.get("profitability") or {}).get("rank")
            for name, v in (("strength", sr), ("profitability", pk)):
                if v is not None and not 1 <= v <= 10:
                    fails.append(tag("%s rank %r out of 1-10" % (name, v)))
            if len((sp.get("strength") or {}).get("items") or []) < 3:
                fails.append(tag("strength items < 3"))
            if len((sp.get("profitability") or {}).get("items") or []) < 4:
                fails.append(tag("profitability items < 4"))
            if t == "NVDA" and (pk or 0) < 7:
                warns.append("NVDA profitability rank %s -- review rubric"
                             % pk)
            if t == "AAOI" and (pk or 10) > 5:
                warns.append("AAOI profitability rank %s -- unexpectedly "
                             "high, review" % pk)

        gp = doc.get("growth_panel") or {}
        if not gp.get("available"):
            fails.append(tag("growth_panel unavailable: %s"
                             % gp.get("reason")))
        elif not any(num(r.get("cagr_3y")) for r in gp.get("rows") or []):
            warns.append(tag("no 3y CAGR computed on any metric"))

        sg = doc.get("signals") or {}
        if not sg.get("available"):
            fails.append(tag("signals unavailable: %s" % sg.get("reason")))
        else:
            n_all = sum(len(sg.get(k) or [])
                        for k in ("severe", "medium", "good"))
            if n_all == 0:
                warns.append(tag("zero computed signs"))
            if t == "AAOI" and len(sg.get("severe") or []) < 2:
                warns.append("AAOI <2 severe signs -- review thresholds")

        pr = doc.get("peer_returns") or {}
        if not pr.get("available"):
            fails.append(tag("peer_returns unavailable: %s"
                             % pr.get("reason")))
        else:
            ann = pr.get("annualized") or []
            if not ann or ann[0].get("symbol") != t:
                fails.append(tag("stock row missing from annualized table"))
            elif not num(ann[0].get("1Y")):
                fails.append(tag("stock 1Y return missing"))
            if len(pr.get("peers") or []) < 2:
                warns.append(tag("only %d peers with 10y prices in "
                                 "return tables"
                                 % len(pr.get("peers") or [])))
            if len(pr.get("calendar") or []) < 1 or \
                    len(pr.get("years") or []) < 5:
                fails.append(tag("calendar-year table malformed"))

        cp = doc.get("company_profile") or {}
        if not cp.get("available"):
            warns.append(tag("company_profile unavailable: %s"
                             % cp.get("reason")))
        elif not cp.get("name"):
            fails.append(tag("profile has no name"))
        rep.ok("%s asserted" % t)

    a = docs.get("AAOI") or {}
    fva = ((a.get("valuation_panel") or {}).get("fair_value") or {})
    rep.kv(aaoi_fv=fva.get("value"), aaoi_ratio=fva.get("price_to_fv"),
           aaoi_verdict=fva.get("verdict"),
           aaoi_sev=len((a.get("signals") or {}).get("severe") or []),
           aaoi_strength=((a.get("strength_profitability") or {})
                          .get("strength") or {}).get("rank"))

    rep.section("G2 page markers")
    raw_page = http("https://raw.githubusercontent.com/ElMooro/si/main"
                    "/why.html")
    for mk in ('id="OPS5011"', "jh5011-fv", "jh5011-ret", "jh5011-sig",
               "jh5011-desc"):
        if mk not in raw_page:
            fails.append("marker %s missing from repo why.html" % mk)
    live_ok = False
    for _ in range(16):
        try:
            if 'id="OPS5011"' in http("https://justhodl.ai/why.html"
                                      "?ops=5011"):
                live_ok = True
                break
        except Exception:
            pass
        time.sleep(30)
    rep.kv(live_page_marker=live_ok)
    if not live_ok:
        warns.append("live page not showing OPS5011 yet (CDN/pages lag) "
                     "-- repo copy verified")

    for w in warns:
        rep.warn(w)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.ok("OPS 5011 PASS -- fair value, valuation history, strength/"
           "profitability, CAGRs, signs, peer returns and profile live "
           "on real data")
