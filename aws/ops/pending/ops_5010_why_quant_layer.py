"""ops_5010 -- why.html Quant Layer: beta/classification on top, realistic
return base rates, 200/250-day EMA distance & chart, stock-vs-industry
risk and 5y YoY/QoQ growth.

Server: justhodl-equity-research v2.4 (additive builders; income_quarterly
depth 8->22; peer-median industry aggregate cached 6d at
equity-research/industry/<T>.json). Page: why.html <script id="OPS5010">.

  G1 deploy: code-only update_function_code (env/config untouched)
  P1 regenerate AAOI+NVDA against live FMP data, hard-assert every block
  G2 repo + live page markers (CDN lag = warn, ops-3299 rule)
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

# Function timeout is 300s (config.json). Default botocore read timeout is
# 60s and would kill a fresh-generation sync invoke -- override, no retries
# (a retry would double-invoke a 3-minute generation).
lam = boto3.client("lambda", region_name=REGION, config=Config(
    connect_timeout=10, read_timeout=600, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name=REGION)


def http(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5010"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def get_doc(rep, t):
    """Sync invoke; on any failure fall back to async + S3 cache poll."""
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
        for _ in range(24):  # up to 6 min
            time.sleep(15)
            try:
                obj = s3c.get_object(Bucket=B, Key=key)
                return json.loads(obj["Body"].read())
            except Exception:
                continue
        raise RuntimeError("%s: no document after async fallback" % t)


with report("ops_5010_why_quant_layer") as rep:
    rep.heading("ops 5010 -- why.html Quant Layer (equity-research v2.4)")
    warns, fails = [], []

    # ── G1: code-only deploy (env untouched) ────────────────────────
    rep.section("G1 deploy justhodl-equity-research v2.4 (code only)")
    src_txt = (SRC / "lambda_function.py").read_text()
    for mark in ("JH-5010", "build_price_analytics", "build_classification",
                 "build_industry_growth_risk", '"limit": 22',
                 '"schema_version": "2.4"'):
        if mark not in src_txt:
            fails.append("repo lambda source missing marker %r" % mark)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("preflight FAILS: " + "; ".join(fails))

    zip_bytes = build_zip(SRC)
    rep.kv(zip_kb=len(zip_bytes) // 1024)
    lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes, Publish=True)
    lam.get_waiter("function_updated_v2").wait(FunctionName=FN)
    rep.ok("code updated; configuration/env untouched")

    # ── P1: regenerate + assert against real FMP data ───────────────
    rep.section("P1 regenerate %s with real data" % "+".join(TICKERS))
    for t in TICKERS:
        for key in ("equity-research/%s.json" % t,
                    "equity-research/industry/%s.json" % t):
            try:
                s3c.delete_object(Bucket=B, Key=key)
            except Exception:
                pass
        rep.log("cache busted for %s" % t)

    docs = {}
    for t in TICKERS:
        t0 = time.time()
        docs[t] = get_doc(rep, t)
        rep.kv(ticker=t, gen_s=round(time.time() - t0, 1),
               schema=docs[t].get("schema_version"))

    for t, doc in docs.items():
        tag = lambda m: "%s: %s" % (t, m)  # noqa: E731
        if doc.get("schema_version") != "2.4":
            fails.append(tag("schema_version %r != 2.4"
                             % doc.get("schema_version")))
            continue

        pa = doc.get("price_analytics") or {}
        if not pa.get("available"):
            fails.append(tag("price_analytics unavailable: %s"
                             % pa.get("reason")))
        else:
            for k in ("ema200", "ema250"):
                e = pa.get(k) or {}
                if not (num(e.get("value")) and e["value"] > 0
                        and num(e.get("dist_pct"))):
                    fails.append(tag("%s malformed: %r" % (k, e)))
            rk = pa.get("risk") or {}
            if not (num(rk.get("vol_1y_pct")) and 3 < rk["vol_1y_pct"] < 400):
                fails.append(tag("vol_1y_pct implausible: %r"
                                 % rk.get("vol_1y_pct")))
            if rk.get("beta_2y") is not None and not -5 < rk["beta_2y"] < 15:
                fails.append(tag("beta_2y implausible: %r" % rk["beta_2y"]))
            if not (num(rk.get("max_dd_1y_pct"))
                    and -100 <= rk["max_dd_1y_pct"] <= 0):
                fails.append(tag("max_dd_1y_pct implausible: %r"
                                 % rk.get("max_dd_1y_pct")))
            hz = (pa.get("expectations") or {}).get("horizons") or {}
            for h in ("1M", "3M", "1Y"):
                e = hz.get(h) or {}
                if not e.get("available"):
                    fails.append(tag("horizon %s unavailable: %s"
                                     % (h, e.get("reason"))))
                    continue
                if e.get("n_windows", 0) < 1500:
                    warns.append(tag("horizon %s only %s windows"
                                     % (h, e.get("n_windows"))))
                if not (e.get("p10_pct") < e.get("median_pct")
                        < e.get("p90_pct")):
                    fails.append(tag("horizon %s ordering broken: %r"
                                     % (h, e)))
            if len(pa.get("ema_chart") or []) < 60:
                fails.append(tag("ema_chart too short: %s"
                                 % len(pa.get("ema_chart") or [])))

        cls = doc.get("classification") or {}
        if not cls.get("available"):
            fails.append(tag("classification unavailable: %s"
                             % cls.get("reason")))
        else:
            mc = cls.get("market_cap") or 0
            tier = cls.get("cap_tier")
            want = ("MEGA-CAP" if mc >= 200e9 else "LARGE-CAP"
                    if mc >= 10e9 else "MID-CAP" if mc >= 2e9
                    else "SMALL-CAP" if mc >= 300e6 else "MICRO-CAP")
            if tier != want:
                fails.append(tag("cap_tier %s != %s for mcap %.1fB"
                                 % (tier, want, mc / 1e9)))
            if len(cls.get("checklist") or []) != 7:
                fails.append(tag("checklist must have 7 rows"))
            if t == "AAOI" and cls.get("is_blue_chip"):
                fails.append("AAOI flagged blue-chip -- checklist broken")
            if t == "NVDA" and not cls.get("is_blue_chip"):
                warns.append("NVDA not blue-chip (%s) -- review thresholds"
                             % cls.get("blue_chip_score"))

        ind = doc.get("industry_growth") or {}
        if ind.get("available"):
            if len(ind.get("peers") or []) < 3:
                fails.append(tag("industry available but <3 peers"))
            iq = [q for q in (ind.get("industry_quarters") or [])
                  if q.get("yoy") is not None]
            if len(iq) < 8:
                warns.append(tag("only %d industry quarters with YoY"
                                 % len(iq)))
            if not (ind.get("stock_quarters") or []):
                fails.append(tag("stock_quarters empty"))
        else:
            warns.append(tag("industry_growth honest-unavailable: %s"
                             % ind.get("reason")))

        stq = ((doc.get("industry_growth") or {})
               .get("stock_quarters") or [])
        if len(stq) < 16:
            warns.append(tag("stock quarterly series only %d points"
                             % len(stq)))
        rep.ok("%s asserted" % t)

    try:
        cdn = json.loads(http("https://justhodl-data-proxy.raafouis"
                              ".workers.dev/equity-research/AAOI.json"
                              "?ops=5010"))
        rep.kv(cdn_schema=cdn.get("schema_version"),
               cdn_has_quant=bool(cdn.get("price_analytics")))
        if not cdn.get("price_analytics"):
            warns.append("CDN not serving v2.4 yet (worker cache TTL) "
                         "-- clears on its own")
    except Exception as e:
        warns.append("CDN check failed: %s" % str(e)[:120])

    # ── G2: repo + live page markers ────────────────────────────────
    rep.section("G2 page markers")
    raw_page = http("https://raw.githubusercontent.com/ElMooro/si/main"
                    "/why.html")
    for mk in ('id="OPS5010"', "jh5010-expect", "jh5010-ema",
               "jh5010-growth"):
        if mk not in raw_page:
            fails.append("marker %s missing from repo why.html" % mk)
    live_ok = False
    for _ in range(16):
        try:
            if 'id="OPS5010"' in http("https://justhodl.ai/why.html"
                                      "?ops=5010"):
                live_ok = True
                break
        except Exception:
            pass
        time.sleep(30)
    rep.kv(live_page_marker=live_ok)
    if not live_ok:
        warns.append("live why.html not showing OPS5010 yet (CDN/pages "
                     "lag) -- repo copy verified")

    for w in warns:
        rep.warn(w)
    if fails:
        for f in fails:
            rep.fail(f)
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.ok("OPS 5010 PASS -- classification, base-rate expectations, "
           "EMA distance, industry risk & 5y growth live on real data")
