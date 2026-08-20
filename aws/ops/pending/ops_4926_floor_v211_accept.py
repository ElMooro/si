"""ops/4926 -- floor-audit v2.1.1: the guards, actually wired.

v2.1.0's own gate caught the bug in its own plumbing: it refused seven
buy calls as "untradable" -- including CUBI, a bank that trades
millions a day. The liquidity number was not thin, it was ABSENT. The
patch that added `adv_usd_20d` and `debt_bound` to the record targeted
a key name that did not exist there ("asset_quality" vs the record's
"asset_quality_score"), so the fields landed in why_block only and
every consumer -- the ops gate and the page alike -- read blank. That
is the house silent-replace trap: assert every replace.

v2.1.1 fixes the record contract and banks the lesson as a permanent
source-level check in the harness: the record dict must carry all nine
consumer-facing fields, verified on the source before any push. The
ops gate also now distinguishes "thin" from "unknown" -- absent data
is never treated as a verdict.

The first whole-market tape (4924: 2,858 filers screened, 168 audited,
13 BUYs) worked -- and reading it exposed two ways a balance-sheet
screen quietly lies to a retail reader:

  * TTEC printed 582% coverage. The floor subtracts debt, but the debt
    ladder only knew a handful of tags; on a filer that tags its debt
    differently nothing binds, debt reads zero, and the floor is
    fiction. v2.1 widens the ladder AND gates on it: an unbound debt
    leg can no longer produce a buy at any coverage -- it is published
    as a veto with the words "treat this as an upper bound".
  * The buy list was mostly nano-caps. A floor you cannot trade is not
    a floor, so the desk now measures 20-day average dollar volume and
    refuses a buy call below $250k/day, with the number shown.

Also tightened: the premium/REDUCE branch now requires coverage >=35%.
DJT was flagged at a "1328% premium for a stack you could buy directly"
on a 7% floor -- that is an operating company holding some coins, not a
wrapper, and the sentence was not honest.

Harness GREEN 97/97.

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

v2.0.0 shipped and its own G4 caught the flaw immediately: 8 filers
screened, not thousands. Cause: the CURRENT quarter's XBRL frame holds
only the handful of companies that have filed so far, and frames_map
accepted the first non-empty frame it saw. v2.0.1 merges frames
newest-first across up to five quarters until the union is dense, so
the newest filing still wins per company while the completed quarter
supplies breadth. Harness GREEN 82/82.

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
  G7 edge: page marker floor-audit-v2.1.1 + feed v2.0.0 (real UA)
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
MARKER = "floor-audit-v2.1.1"
UA = "JustHodl ops4926 edge-acceptance"

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
    with report("4926-floor-v211-accept") as r:
        r.heading("ops 4926 -- floor-audit v2.1.1 acceptance")
        t0 = datetime.now(timezone.utc)

        r.section("G1 deploy")
        if ENG.VERSION != "2.1.1":
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
            # ops 4922/4923 traps: the helper kwarg is `smoke`, not
            # `smoke_test` (grep the signature, the docstring lies),
            # and it smoke-tests SYNCHRONOUSLY.
            # A whole-market sweep runs minutes, so the boto read
            # timeout killed the op while the engine ran on happily.
            # Long engines are gated on S3 freshness, never on a
            # RequestResponse invoke.
            smoke=False, create_function_url=False,
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
        cutoff = (t0.timestamp() - 900)
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        payload = None
        for i in range(90):  # sweep + deep tier: allow 15 min
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("version") == "2.1.1" and (
                    cur.get("as_of", "") > prev or
                    datetime.fromisoformat(
                        cur["as_of"]).timestamp() >= cutoff):
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
             truncated=len(payload.get("deep_truncated") or []),
             alerts=len(payload["alerts"]))
        for k, v in (payload.get("screen_frames") or {}).items():
            r.kv(frame=k, frames_merged=v)

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
        okall = [x for x in tks.values() if x.get("status") == "OK"]
        bad_unbound = [x["ticker"] for x in okall
                       if x.get("debt_bound") is False and
                       x["recommendation"]["action"] in
                       ("BUY", "ACCUMULATE")]
        if bad_unbound:
            r.log("FAIL G5: buy call on an unbound debt leg: %s"
                  % bad_unbound)
            sys.exit(1)
        thin_buys = [(x["ticker"], round(x["adv_usd_20d"] / 1000.0))
                     for x in okall
                     if x["recommendation"]["action"] == "BUY" and
                     x.get("adv_usd_20d") is not None and
                     x["adv_usd_20d"] < 250000.0]
        blind = [x["ticker"] for x in okall
                 if x.get("adv_usd_20d") is None]
        if len(blind) > 0.5 * max(1, len(okall)):
            r.log("FAIL G5: liquidity absent on %d/%d audited names -- "
                  "the ADV memo is not wired" % (len(blind), len(okall)))
            sys.exit(1)
        if thin_buys:
            r.log("FAIL G5: buy call on an untradable name: %s"
                  % thin_buys)
            sys.exit(1)
        bad_prem = [x["ticker"] for x in okall
                    if x["recommendation"]["action"] == "REDUCE" and
                    (x.get("coverage") or 0) < 0.35]
        if bad_prem:
            r.log("FAIL G5: premium call on a name with no real "
                  "floor: %s" % bad_prem)
            sys.exit(1)
        r.kv(guards="PASS",
             unbound_debt_names=sum(1 for x in okall
                                    if x.get("debt_bound") is False),
             thin_names=sum(1 for x in okall
                            if x.get("adv_usd_20d") is not None and
                            x["adv_usd_20d"] < 250000.0),
             liquidity_unknown=len(blind))
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
                  % (st, MARKER in html, "floor-audit-v2.0.0" in html))
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
        if stf != 200 or fv != "2.1.1":
            r.log("FAIL G7: feed status=%s version=%s" % (stf, fv))
            sys.exit(1)
        r.kv(g7="PASS", marker=MARKER, attempts=n, feed_version=fv,
             definitions="served")

        rep = {"op": 4926, "version": "2.1.1",
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
                    "adv_usd_20d": x.get("adv_usd_20d"),
                    "debt_bound": x.get("debt_bound"),
                    "plain": x["recommendation"]["plain"],
                    "reasons": x["recommendation"]["reasons"],
                    "risks": x["recommendation"]["risks"]}
                   for x in sorted(
                       ok, key=lambda z: -(z["recommendation"]
                                           ["conviction"] or 0))[:15]],
               "edge": "LIVE"}
        (ROOT / "aws" / "ops" / "reports" / "4926.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN", duration_s=int(
            (datetime.now(timezone.utc) - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
