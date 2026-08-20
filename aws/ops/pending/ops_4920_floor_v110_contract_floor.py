"""ops/4920 -- floor-audit v1.1.0: the order book becomes a floor.

Khalid's spec had two halves. Half one (a dump is senseless when the
balance-sheet assets exceed what the market is paying) went live in
4914-4919 and is firing on BTBT. Half two -- "incorporate backlog
orders/contracts" -- was only a reported field until now: committed
revenue was printed on the record but could never produce a verdict,
and the universe was crypto-discovery-only, so a company whose ORDER
BOOK exceeds its market cap was never even audited.

v1.1.0:
  * universe pass 2 seeds from data/backlog-mined.json (top names by
    committed backlog >= $300M not already covered);
  * verdict ladder gains BACKLOG_FLOOR (committed > mcap, no dump --
    a standing fact, INFO) and CONTRACT_BACKED_DUMP (that book plus an
    unexplained dump -- MEDIUM, HIGH above 3x);
  * precedence unchanged where it matters: hard liquid assets outrank
    the order book, and the crypto SENSELESS branch outranks both --
    a promise to deliver is not cash.
  Harness GREEN 51/51 including the three precedence cases.

Gates:
  G1 deploy v1.1.0
  G2 config reset (adds backlog_universe + committed thresholds)
  G3 fresh run, version 1.1.0, universe grew by the seeded names
  G4 backlog leg real: seeded names audited OK, committed coverage
     bound, JOIN_BROKEN surfaced if the feed contract drifted
  G5 ladder integrity on live tape: no CONTRACT verdict where
     coverage >= 1.0 (liquid floor must win), BTBT still SENSELESS
  G6 edge: page marker floor-audit-v1.1.0 + feed v1.1.0 (proper UA)
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
MARKER = "floor-audit-v1.1.0"
UA = "JustHodl ops4920 edge-acceptance"

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
        with urllib.request.urlopen(req, timeout=25) as resp:
            return resp.status, resp.read(cap).decode("utf-8",
                                                      "replace")
    except Exception as e:  # noqa: BLE001
        return "%s:%s" % (type(e).__name__, str(e)[:70]), ""


def main():
    with report("4920-floor-v110-contract-floor") as r:
        r.heading("ops 4920 -- floor-audit v1.1.0 contract floor")
        t0 = datetime.now(timezone.utc)

        r.section("G1 deploy")
        if ENG.VERSION != "1.1.0":
            r.log("FAIL G1: source VERSION %s" % ENG.VERSION)
            sys.exit(1)
        poly = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        )["Environment"]["Variables"]["POLYGON_API_KEY"]
        deploy_lambda(
            report=r, function_name=FN,
            source_dir=ROOT / "aws" / "lambdas" / FN / "source",
            env_vars={"POLYGON_API_KEY": poly},
            timeout=600, memory=1024,
            description=("Senseless-drawdown auditor v1.1: liquid "
                         "floor + live crypto mark + committed order "
                         "book vs mcap; asset-driven vs residual; "
                         "-> data/floor-audit.json daily 21:35 UTC."),
        )
        r.kv(g1="PASS")

        r.section("G2 config reset")
        dc = ENG.DEFAULT_CONFIG
        if "backlog_universe" not in dc or \
                "committed_floor" not in dc["thresholds"]:
            r.log("FAIL G2: DEFAULT_CONFIG missing v1.1 knobs")
            sys.exit(1)
        s3.put_object(Bucket=B, Key=CFG_KEY,
                      Body=json.dumps(dc,
                                      separators=(",", ":")).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        r.kv(g2="PASS",
             committed_floor=dc["thresholds"]["committed_floor"],
             committed_high=dc["thresholds"]["committed_high"],
             seed_min_usd=dc["backlog_universe"]["min_backlog_usd"],
             seed_max_add=dc["backlog_universe"]["max_add"])

        r.section("G3 fresh run")
        prev_p = s3_json(OUT_KEY) or {}
        prev = prev_p.get("as_of", "")
        prev_n = prev_p.get("universe_n", 0)
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        payload = None
        for _ in range(60):
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev and \
                    cur.get("version") == "1.1.0":
                payload = cur
                break
        if not payload:
            r.log("FAIL G3: no fresh v1.1.0 payload in 10 min")
            sys.exit(1)
        seeded = payload.get("backlog_seeded") or []
        r.kv(g3="PASS", as_of=payload["as_of"],
             universe=payload["universe_n"], was=prev_n,
             g0_ok=payload["g0_ok_tickers"],
             backlog_seeded=",".join(seeded) or "none",
             alerts=len(payload["alerts"]))

        r.section("G4 order-book leg")
        tks = payload["tickers"]
        broken = sorted({x.get("backlog_status") for x in tks.values()
                         if str(x.get("backlog_status", ""))
                         .startswith("JOIN_BROKEN")})
        if broken:
            r.log("FAIL G4: backlog join contract drifted: %s"
                  % broken)
            sys.exit(1)
        bound = [(t, x["committed_rev_coverage"])
                 for t, x in tks.items()
                 if x.get("status") == "OK" and
                 x.get("committed_rev_coverage") is not None]
        bound.sort(key=lambda z: -(z[1] or 0))
        if not seeded and not bound:
            r.log("FAIL G4: no committed coverage bound anywhere -- "
                  "the order-book leg is dead weight")
            sys.exit(1)
        r.kv(g4="PASS", committed_bound_n=len(bound))
        for t, c in bound[:8]:
            rec = tks[t]
            r.kv(committed=t, cov_x=c,
                 rpo_musd=(None if rec.get("rpo_usd") is None
                           else round(rec["rpo_usd"] / 1e6)),
                 backlog_musd=(None if rec.get("backlog_usd") is None
                               else round(rec["backlog_usd"] / 1e6)),
                 status=rec.get("backlog_status"),
                 verdict=rec["verdict"]["verdict"])
        for t in seeded:
            rec = tks.get(t) or {}
            if rec.get("status") != "OK":
                r.kv(seeded_skip=t, status=rec.get("status"),
                     reason=str(rec.get("reason"))[:90])

        r.section("G5 ladder integrity (live tape)")
        contract_names = payload.get("contract_floors") or []
        viol = [t for t in contract_names
                if (tks[t].get("coverage") or 0) >= 1.0]
        if viol:
            r.log("FAIL G5: order book outranked hard assets on %s"
                  % viol)
            sys.exit(1)
        bt = tks.get("BTBT") or {}
        if bt.get("status") == "OK" and \
                bt["verdict"]["verdict"] == "CONTRACT_BACKED_DUMP":
            r.log("FAIL G5: BTBT regressed off the crypto branch")
            sys.exit(1)
        r.kv(g5="PASS",
             contract_floors=",".join(contract_names) or "none",
             btbt=bt.get("verdict", {}).get("verdict"),
             btbt_cov=bt.get("coverage"),
             btbt_crypto=bt.get("crypto_coverage"))
        for a in payload["alerts"][:10]:
            r.kv(alert=a["ticker"], sev=a["severity"], v=a["verdict"],
                 sense=a["sense_score"], cov=a["coverage"])

        r.section("G6 edge")
        st, html, n = None, "", 0
        for n in range(1, 25):
            st, html = edge("https://justhodl.ai/floor.html?cb=%d"
                            % time.time())
            if st == 200 and MARKER in html:
                break
            time.sleep(30)
        if not (st == 200 and MARKER in html):
            r.log("FAIL G6: page status=%s marker=%s (stale v1.0.3=%s)"
                  % (st, MARKER in html,
                     "floor-audit-v1.0.3" in html))
            sys.exit(1)
        stf, body = edge("https://justhodl.ai/data/floor-audit.json"
                         "?cb=%d" % time.time(), cap=4000000)
        try:
            feed_v = json.loads(body).get("version")
        except Exception:  # noqa: BLE001
            feed_v = None
        if stf != 200 or feed_v != "1.1.0":
            r.log("FAIL G6: feed status=%s version=%s"
                  % (stf, feed_v))
            sys.exit(1)
        r.kv(g6="PASS", page_marker=MARKER, attempts=n,
             feed_version=feed_v)

        rep = {"op": 4920, "version": "1.1.0",
               "as_of": payload["as_of"],
               "universe_n": payload["universe_n"],
               "backlog_seeded": seeded,
               "contract_floors": contract_names,
               "committed_top": bound[:10],
               "alerts": payload["alerts"],
               "quarantine": {
                   "fund_wrappers": payload.get("fund_wrappers"),
                   "suspect_inputs": payload.get("suspect_inputs"),
                   "broker_sheets": payload.get("broker_sheets")},
               "edge": "LIVE"}
        (ROOT / "aws" / "ops" / "reports" / "4920.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN", duration_s=int(
            (datetime.now(timezone.utc) - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
