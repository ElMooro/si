"""ops/4921 -- floor-audit v2.0.0: whole market, durability, and a call.

Khalid's three asks, built the way a fund would build them:

1. ALL STOCKS. Auditing 6,000 filers forensically is impossible in a
   Lambda; screening them is nearly free. XBRL **frames** returns one
   tag for EVERY filer in a single call, and Polygon grouped-daily
   returns every last close in another -- ~11 HTTP calls covers the
   whole market, including foreign private issuers filing IFRS. The
   deep forensic audit (live crypto marks, per-leg provenance,
   decomposition) then runs only where a floor could plausibly exist.
   Screen wide, audit deep, and label which is which.

2. DEFINITIONS. Every metric on the page now carries its formula, its
   source, and what it means in plain English -- a retail reader
   should never have to guess what "residual" or "mNAV" is.

3. A CALL. Verdicts classify; they do not decide. v2.0 adds the
   decision layer: discount + asset quality + durability + unexplained
   dump -> BUY / ACCUMULATE / WATCH / PASS / AVOID / REDUCE with
   conviction, the reasons, the risks, and what would flip it. Vetoes
   fire BEFORE any score, because the classic way to lose money on a
   balance-sheet screen is to buy a real discount to a floor that is
   being burned or issued away. New legs: TTM operating cash flow,
   runway, YoY dilution, debt-to-floor, asset quality, and the SELL
   side this desk never had -- premium to NAV on treasury companies.

Harness GREEN 78/78 (27 new: tiers, quality, burn, vetoes, ladder).

Gates:
  G1 deploy v2.0.0 at 2048/900 (the sweep needs room)
  G2 config reset (market_sweep + durability thresholds)
  G3 fresh run, version 2.0.0
  G4 MARKET BREADTH: >=1,500 filers screened across >=4 cap tiers,
     with mega/large caps present -- proof this is no longer a
     crypto-only desk
  G5 DECISION LAYER: every OK ticker carries a recommendation with
     action/conviction/plain/invalidation; no BUY on a vetoed or
     structurally quarantined name; vetoes recorded where fired
  G6 PRECEDENCE + REGRESSION: BTBT still SENSELESS, quarantines still
     clean, order book still ranked below hard assets
  G7 edge: page marker floor-audit-v2.0.0 + feed v2.0.0 (real UA)
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-floor-audit"
                       / "source"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
import lambda_function as ENG  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-floor-audit"
OUT_KEY = "data/floor-audit.json"
CFG_KEY = "data/floor-audit/config.json"
MARKER = "floor-audit-v2.0.0"
UA = "JustHodl ops4921 edge-acceptance"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def edge(url, cap=200000):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read(cap).decode("utf-8",
                                                      "replace")
    except Exception as e:  # noqa: BLE001
        return "%s:%s" % (type(e).__name__, str(e)[:70]), ""


def main():
    with report("4921-floor-v200-market-sweep") as r:
        r.heading("ops 4921 -- floor-audit v2.0.0 whole market + call")
        t0 = datetime.now(timezone.utc)

        r.section("G1 deploy")
        if ENG.VERSION != "2.0.0":
            r.log("FAIL G1: source VERSION %s" % ENG.VERSION)
            sys.exit(1)
        poly = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        )["Environment"]["Variables"]["POLYGON_API_KEY"]
        deploy_lambda(
            report=r, function_name=FN,
            source_dir=ROOT / "aws" / "lambdas" / FN / "source",
            env_vars={"POLYGON_API_KEY": poly},
            timeout=900, memory=2048,
            description=("Asset-floor auditor v2.0: whole-market XBRL "
                         "frames screen + deep forensic audit; liquid "
                         "floor, order book, burn/dilution durability, "
                         "and a buy/watch/avoid call with reasons."),
        )
        r.kv(g1="PASS", timeout=900, memory=2048)

        r.section("G2 config reset")
        dc = ENG.DEFAULT_CONFIG
        if "market_sweep" not in dc or \
                "runway_min_months" not in dc["thresholds"]:
            r.log("FAIL G2: DEFAULT_CONFIG missing v2.0 knobs")
            sys.exit(1)
        s3.put_object(Bucket=B, Key=CFG_KEY,
                      Body=json.dumps(dc,
                                      separators=(",", ":")).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        ms = dc["market_sweep"]
        r.kv(g2="PASS", max_deep=ms["max_deep"],
             prescreen_min_cov=ms["prescreen_min_cov"],
             min_mcap_usd=ms["min_mcap_usd"])

        r.section("G3 fresh run")
        prev = (s3_json(OUT_KEY) or {}).get("as_of", "")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        payload = None
        for i in range(90):  # sweep + deep tier: allow 15 min
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev and \
                    cur.get("version") == "2.0.0":
                payload = cur
                break
            if i % 12 == 11:
                r.log("  poll %ds" % ((i + 1) * 10))
        if not payload:
            r.log("FAIL G3: no fresh v2.0.0 payload in 15 min")
            sys.exit(1)
        tks = payload["tickers"]
        r.kv(g3="PASS", as_of=payload["as_of"],
             screened=payload.get("screened_n"),
             deep=payload["universe_n"], g0_ok=payload["g0_ok_tickers"],
             alerts=len(payload["alerts"]))
        for k, v in (payload.get("screen_frames") or {}).items():
            r.kv(frame=k, resolved=v)

        r.section("G4 market breadth")
        tiers = payload.get("screen_cap_tiers") or {}
        n = payload.get("screened_n") or 0
        live_tiers = [t for t, c in tiers.items() if c > 0]
        if n < 1500 or len(live_tiers) < 4 or \
                (tiers.get("mega", 0) + tiers.get("large", 0)) < 50:
            r.log("FAIL G4: screened=%d tiers=%s -- the sweep did not "
                  "reach the whole market" % (n, tiers))
            sys.exit(1)
        r.kv(g4="PASS", screened=n, **{("tier_" + t): c
                                       for t, c in tiers.items()})
        r.kv(promoted=len(payload.get("screen_seeded") or []),
             promoted_names=",".join(
                 (payload.get("screen_seeded") or [])[:20]))
        for row in (payload.get("screen") or [])[:10]:
            r.kv(screen=row["ticker"], tier=row["cap_tier"],
                 approx_cov=row["approx_coverage"],
                 mcap_musd=round(row["mcap_usd"] / 1e6, 1))

        r.section("G5 decision layer")
        ok = [x for x in tks.values() if x.get("status") == "OK"]
        missing = [x["ticker"] for x in ok
                   if not (x.get("recommendation") or {}).get("action")]
        if missing:
            r.log("FAIL G5: no call produced for %s" % missing[:10])
            sys.exit(1)
        bad_shape = [x["ticker"] for x in ok
                     if not (x["recommendation"].get("plain") and
                             x["recommendation"].get("invalidation"))]
        if bad_shape:
            r.log("FAIL G5: call without plain text/invalidation: %s"
                  % bad_shape[:10])
            sys.exit(1)
        bad_buy = [x["ticker"] for x in ok
                   if x["recommendation"]["action"] in
                   ("BUY", "ACCUMULATE") and
                   (x["recommendation"].get("vetoes") or
                    x["verdict"]["verdict"] in ENG.NO_CALL_VERDICTS)]
        if bad_buy:
            r.log("FAIL G5: buy call on a vetoed/quarantined name: %s"
                  % bad_buy)
            sys.exit(1)
        acts = payload.get("actions") or {}
        r.kv(g5="PASS", **{("act_" + a): len(v)
                           for a, v in acts.items()})
        for a in ("BUY", "ACCUMULATE", "REDUCE", "AVOID"):
            if acts.get(a):
                r.kv(action=a, names=",".join(acts[a][:12]))
        for x in sorted(ok, key=lambda z: -(z["recommendation"]
                                            ["conviction"] or 0))[:8]:
            rc = x["recommendation"]
            r.kv(call=x["ticker"], action=rc["action"],
                 conviction=rc["conviction"], tier=x.get("cap_tier"),
                 cov=x["coverage"], durability=x.get("durability_score"),
                 runway=(x.get("runway_state")
                         if x.get("runway_months") is None
                         else x["runway_months"]),
                 quality=x.get("asset_quality_score"),
                 why=(rc["reasons"] or ["-"])[0][:80])

        r.section("G6 precedence + regression")
        bt = tks.get("BTBT") or {}
        if bt.get("status") == "OK" and \
                bt["verdict"]["verdict"] != "SENSELESS_DRAWDOWN":
            r.log("FAIL G6: BTBT regressed to %s"
                  % bt["verdict"]["verdict"])
            sys.exit(1)
        alert_set = {a["ticker"] for a in payload["alerts"]}
        quar = set((payload.get("fund_wrappers") or []) +
                   (payload.get("suspect_inputs") or []) +
                   (payload.get("broker_sheets") or []))
        if quar & alert_set:
            r.log("FAIL G6: quarantined name alerting: %s"
                  % sorted(quar & alert_set))
            sys.exit(1)
        viol = [t for t in (payload.get("contract_floors") or [])
                if (tks[t].get("coverage") or 0) >= 1.0]
        if viol:
            r.log("FAIL G6: order book outranked hard assets on %s"
                  % viol)
            sys.exit(1)
        r.kv(g6="PASS", btbt=bt.get("verdict", {}).get("verdict"),
             btbt_call=(bt.get("recommendation") or {}).get("action"),
             btbt_durability=bt.get("durability_score"),
             btbt_runway=bt.get("runway_months"),
             quarantined=len(quar),
             contract_floors=",".join(
                 payload.get("contract_floors") or []) or "none")

        r.section("G7 edge")
        st, html, n = None, "", 0
        for n in range(1, 25):
            st, html = edge("https://justhodl.ai/floor.html?cb=%d"
                            % time.time())
            if st == 200 and MARKER in html:
                break
            time.sleep(30)
        if not (st == 200 and MARKER in html):
            r.log("FAIL G7: page status=%s marker=%s (stale v1.1=%s)"
                  % (st, MARKER in html, "floor-audit-v1.1.0" in html))
            sys.exit(1)
        if "What every number on this page means" not in html:
            r.log("FAIL G7: definitions block absent from the served "
                  "page")
            sys.exit(1)
        stf, body = edge("https://justhodl.ai/data/floor-audit.json"
                         "?cb=%d" % time.time(), cap=8000000)
        try:
            fv = json.loads(body).get("version")
        except Exception:  # noqa: BLE001
            fv = None
        if stf != 200 or fv != "2.0.0":
            r.log("FAIL G7: feed status=%s version=%s" % (stf, fv))
            sys.exit(1)
        r.kv(g7="PASS", marker=MARKER, attempts=n, feed_version=fv,
             definitions="served")

        rep = {"op": 4921, "version": "2.0.0",
               "as_of": payload["as_of"],
               "screened_n": payload.get("screened_n"),
               "screen_cap_tiers": tiers,
               "deep_n": payload["universe_n"],
               "screen_seeded": payload.get("screen_seeded"),
               "actions": acts,
               "alerts": payload["alerts"],
               "top_calls": [
                   {"ticker": x["ticker"],
                    "action": x["recommendation"]["action"],
                    "conviction": x["recommendation"]["conviction"],
                    "tier": x.get("cap_tier"),
                    "coverage": x["coverage"],
                    "durability": x.get("durability_score"),
                    "runway_months": x.get("runway_months"),
                    "runway_state": x.get("runway_state"),
                    "quality": x.get("asset_quality_score"),
                    "premium_to_nav": x.get("premium_to_nav"),
                    "plain": x["recommendation"]["plain"],
                    "reasons": x["recommendation"]["reasons"],
                    "risks": x["recommendation"]["risks"]}
                   for x in sorted(
                       ok, key=lambda z: -(z["recommendation"]
                                           ["conviction"] or 0))[:15]],
               "edge": "LIVE"}
        (ROOT / "aws" / "ops" / "reports" / "4921.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN", duration_s=int(
            (datetime.now(timezone.utc) - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
