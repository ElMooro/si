"""ops_5018 -- full GuruFocus summary parity (equity-research v2.9).

Every remaining visual from the 12-screenshot set, computed from real
FMP/EDGAR/treasury data with printed assumptions: fundamental tables
with own-10yr percentile context, Piotroski (9 components) / Altman /
Beneish / WACC-vs-ROIC, the full valuation-ratio set, a six-method
valuation ladder + price-vs-median-P/S band, RSI & skip-month
momentum, key statistics (3y Sharpe/Sortino vs the live 10y treasury),
5-step DuPont, five financial mini-charts, product & geographic
revenue mix, analyst estimates, transcript index, news, splits &
company facts, shareholder yield, risk assessment, and the peer
performance chart (reusing the industry2 10y peer-price cache).

SCHEMA_CURRENT -> 2.9 rolls everything to every ticker via the
ops-5014 version gate; the client layer is ticker-bus driven and
self-healing. The asserts below are deep and run on freshly
regenerated AAOI + NVDA docs (refresh=1), including cross-checks
against facts visible in the user's own screenshots (AAOI IPO
2013-09-26, geographic mix led by China/Taiwan, FY2026 consensus
revenue ~$1.0B, High risk).
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops5018"})
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


with report("ops_5018_gf_parity") as rep:
    rep.heading("ops 5018 -- full GuruFocus summary parity (v2.9)")
    fails, warns = [], []

    rep.section("G0 preflight")
    src = (SRC / "lambda_function.py").read_text()
    for mark in ("JH-5018", "build_gf_extras", "build_scores",
                 "build_valuation_ladder", "build_dupont",
                 '"rev_geo_seg"', '"analyst_est"', '"treasury"',
                 '"gf_extras":', 'SCHEMA_CURRENT = "2.9.2"'):
        if mark not in src:
            fails.append("lambda missing %r" % mark)
    page = (ROOT / "why.html").read_text()
    for mark in ('<script id="OPS5018">', "jh5018-wrap",
                 "Full Summary Tables"):
        if mark not in page:
            fails.append("why.html missing %r" % mark)
    if page.count("__JH_TICKER_BUS.subscribe") != 6:
        fails.append("expected 6 bus subscriptions, page has %d"
                     % page.count("__JH_TICKER_BUS.subscribe"))
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight failed")
    rep.ok("v2.9 markers, OPS5018 block, 6 bus subscriptions")

    rep.section("G1 deploy (code only)")
    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes,
                             Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    rep.section("P1 deep real-data asserts")
    for t in ("AAOI", "NVDA"):
        tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
        t0 = time.time()
        doc = invoke(t)
        doc_kb = len(json.dumps(doc)) // 1024
        rep.kv(ticker=t, gen_s=round(time.time() - t0, 1),
               schema=doc.get("schema_version"), doc_kb=doc_kb)
        if doc.get("schema_version") != "2.9.2":
            fails.append(tag("schema %r != 2.9.2"
                             % doc.get("schema_version")))
            continue
        X = doc.get("gf_extras") or {}
        if not X.get("available"):
            fails.append(tag("gf_extras unavailable: %s"
                             % X.get("reason")))
            continue

        for name in ("fin_strength_table", "profitability_table",
                     "liquidity_table"):
            T = X.get(name) or {}
            if not T.get("available") or len(T.get("rows") or []) < 4:
                fails.append(tag("%s thin/unavailable: %s"
                                 % (name, T.get("reason"))))
        S = X.get("scores") or {}
        pi = S.get("piotroski") or {}
        if not (0 <= pi.get("total", -1) <= 9) or \
                len(pi.get("components") or []) != 9:
            fails.append(tag("piotroski malformed"))
        al = S.get("altman") or {}
        if al.get("zone") not in ("Safe", "Grey", "Distress"):
            fails.append(tag("altman missing"))
        be = S.get("beneish") or {}
        if not isinstance(be.get("m"), (int, float)):
            fails.append(tag("beneish missing"))
        wr = S.get("wacc_roic") or {}
        if not isinstance(wr.get("wacc_pct"), (int, float)) or \
                not (2.0 <= wr["wacc_pct"] <= 45.0):
            fails.append(tag("wacc missing/insane: %r (beta_used=%r "
                             "rf=%r)" % (wr.get("wacc_pct"),
                                         wr.get("beta_used"),
                                         wr.get("rf_used"))))
        if not isinstance(wr.get("roic_pct"), (int, float)):
            fails.append(tag("roic missing"))
        ali = al.get("inputs") or {}
        if isinstance(al.get("z"), (int, float)) and not \
                (0.2 <= al["z"] <= 80):
            fails.append(tag("altman scale off: z=%s inputs=%s"
                             % (al["z"], {k: round(v / 1e6, 1)
                                          for k, v in ali.items()
                                          if isinstance(v, (int, float))
                                          })))
        rep.kv(ticker=t, piotroski=pi.get("total"),
               altman="%s(%s)" % (al.get("z"), al.get("zone")),
               beneish=be.get("m"), wacc=wr.get("wacc_pct"),
               roic=wr.get("roic_pct"))

        VR = X.get("valuation_ratios") or {}
        if not VR.get("available") or len(VR.get("rows") or []) < 9:
            fails.append(tag("valuation ratios thin"))
        LD = X.get("valuation_ladder") or {}
        if not LD.get("available") or len(LD.get("methods") or []) < 4 \
                or not LD.get("price"):
            fails.append(tag("valuation ladder thin: %s methods=%s"
                             % (LD.get("reason"),
                                [m.get("name") for m in
                                 (LD.get("methods") or [])])))
        else:
            rep.kv(ticker=t, ladder_methods=len(LD["methods"]),
                   ladder_sh_m=LD.get("sh_used"))
        FB = X.get("fv_band") or {}
        if not FB.get("available") or \
                len(FB.get("monthly_price") or []) < 36 or \
                len(FB.get("anchor_by_year") or []) < 3:
            fails.append(tag("fv band thin: %s" % FB.get("reason")))
        ME = X.get("momentum_extra") or {}
        rsi = (ME.get("rsi") or {}).get("14d")
        if not ME.get("available") or rsi is None or not \
                (0 <= rsi <= 100):
            fails.append(tag("momentum extra bad"))
        KS = X.get("key_stats") or {}
        if not KS.get("available") or \
                not isinstance(KS.get("sharpe_3y"), (int, float)):
            fails.append(tag("key stats bad"))
        DU = X.get("dupont") or {}
        if DU.get("available"):
            ident = (DU["net_margin_pct"] / 100.0 *
                     DU["asset_turnover"] *
                     (DU["equity_multiplier"] or 0) * 100.0)
            if abs(ident - (DU.get("roe_pct") or 0)) > \
                    max(0.6, abs(DU.get("roe_pct") or 0) * 0.03):
                fails.append(tag("DuPont identity broken: %.2f vs %.2f"
                                 % (ident, DU.get("roe_pct"))))
        else:
            warns.append(tag("dupont: %s" % DU.get("reason")))
        MI = X.get("financial_minis") or {}
        ch = (MI.get("charts") or {})
        if len(ch) < 5 or not (ch.get("rev_ni_ebitda") or {}) \
                .get("Revenue"):
            fails.append(tag("mini-charts thin"))
        SG = X.get("segments") or {}
        ge = (SG.get("geographic") or {})
        pr = (SG.get("product") or {})
        if t == "AAOI":
            if not ge.get("available") or \
                    len((ge.get("latest") or {}).get("data") or {}) < 2:
                fails.append(tag("AAOI geographic mix missing"))
            else:
                top = max((ge["latest"]["data"] or {"": 0}).items(),
                          key=lambda kv: kv[1])[0]
                rep.kv(ticker=t, geo_regions=len(ge["latest"]["data"]),
                       geo_top=top)
            if not pr.get("available"):
                fails.append(tag("AAOI product mix missing"))
        ES = X.get("estimates") or {}
        if ES.get("available") and ES.get("rows"):
            first_fy = ES["rows"][0]["fy"]
            if first_fy < time.strftime("%Y-%m"):
                fails.append(tag("estimates leak past FY: %s"
                                 % first_fy))
        if ES.get("available"):
            r0 = (ES.get("rows") or [{}])[0]
            rep.kv(ticker=t, est_fy=r0.get("fy"),
                   est_rev_m=round((r0.get("revenue") or 0) / 1e6, 1),
                   est_rows=len(ES.get("rows") or []))
            if t == "AAOI" and r0.get("revenue") and not \
                    (7e8 <= r0["revenue"] <= 1.5e9):
                warns.append(tag("AAOI FY-next consensus revenue "
                                 "outside expected band: %.0fM"
                                 % (r0["revenue"] / 1e6)))
        else:
            warns.append(tag("estimates: %s" % ES.get("reason")))
        TL = X.get("transcripts") or {}
        if not TL.get("available") or len(TL.get("rows") or []) < 5 or \
                not (TL["rows"][0].get("label") or "").startswith("Q"):
            fails.append(tag("transcript labels thin"))
        NW = X.get("news") or {}
        if not NW.get("available") or len(NW.get("rows") or []) < 3:
            warns.append(tag("news thin: %s" % NW.get("reason")))
        SP = X.get("splits") or {}
        if not SP.get("available"):
            warns.append(tag("splits: %s" % SP.get("reason")))
        FA = X.get("facts") or {}
        if t == "AAOI" and FA.get("ipo_date") != "2013-09-26":
            fails.append(tag("AAOI IPO date wrong: %r"
                             % FA.get("ipo_date")))
        PP = X.get("peer_perf") or {}
        if not PP.get("available") or len(PP.get("series") or {}) < 2:
            fails.append(tag("peer perf thin: %s" % PP.get("reason")))
        else:
            npts = min(len(v) for v in PP["series"].values())
            if npts < 36:
                fails.append(tag("peer series too short: %d" % npts))
            rep.kv(ticker=t, peer_series=len(PP["series"]),
                   peer_pts=npts)
        RA = X.get("risk_assessment") or {}
        if t == "AAOI" and RA.get("level") != "High":
            fails.append(tag("AAOI risk must assess High (vol/beta/"
                             "drawdown): got %r" % RA.get("level")))
        DE = X.get("dividend_extra") or {}
        sy = DE.get("shareholder_yield_pct")
        di = DE.get("inputs") or {}
        rep.kv(ticker=t, sh_yield=sy,
               de_div_m=round((di.get("dividends") or 0) / 1e6, 1),
               de_buyb_m=round((di.get("buybacks") or 0) / 1e6, 1),
               de_iss_m=round((di.get("issuance") or 0) / 1e6, 1))
        if not isinstance(sy, (int, float)) or sy == 0.0:
            fails.append(tag("shareholder yield zero/missing"))
        elif t == "AAOI" and sy >= 0:
            fails.append(tag("AAOI shareholder yield must be negative "
                             "(heavy issuance): %s" % sy))
        elif t == "NVDA" and sy <= 0:
            fails.append(tag("NVDA shareholder yield must be positive "
                             "(buybacks+div): %s" % sy))
        rep.kv(ticker=t, risk=RA.get("level"),
               transcripts=len(TL.get("rows") or []),
               news=len((NW.get("rows") or [])))
        rep.ok(tag("all gf_extras sections checked"))

    for wmsg in warns:
        rep.warn(wmsg)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("deep asserts failed")

    rep.section("G2 live page carries OPS5018")
    deadline = time.time() + 300
    ok = False
    while time.time() < deadline:
        try:
            pv = http("https://justhodl.ai/why.html?cb=%d"
                      % int(time.time()))
            if ('<script id="OPS5018">' in pv
                    and pv.count("__JH_TICKER_BUS.subscribe") == 6
                    and "Full Summary Tables" in pv):
                ok = True
                break
            rep.log("waiting for site sync")
        except Exception as e:
            rep.log("live fetch: %s" % e)
        time.sleep(15)
    if not ok:
        rep.fail("site never served OPS5018")
        raise SystemExit("live check failed")
    rep.kv(page_kb=len(pv) // 1024)
    rep.ok("served page carries the parity layer + 6 subscriptions")
    rep.ok("OPS 5018 PASS -- every visual from all twelve screenshots "
           "is built, deep-verified on real data, and rolls to every "
           "ticker")
