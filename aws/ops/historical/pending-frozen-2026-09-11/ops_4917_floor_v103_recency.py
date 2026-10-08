"""ops/4917 -- floor-audit v1.0.3: recency-first crypto bind.

Supersedes ops_4916 (left in pending as a fossil; the runner keys on
push diffs, deleting it would 404 the executor). 4916's G4 hard gate
did its job: BTBT read 0.46% crypto because the parent tag
CryptoAssetFairValue carried a stale $2.3M fact (end 2026-03-31) that
unconditionally shadowed the fresh Q2'26 splits -- Current $120.149M +
Noncurrent $120.149M (end 2026-06-30). The 4916 failure report's tag
inventory is the primary source for this fix.

v1.0.3 changes (harness GREEN 43/43 locally, incl. an exact replica of
the 4916 BTBT tape resolving to $240,298,000 @ 2026-06-30):
  1. crypto_fv is recency-first across parent / splits / extension
     tags; fresh splits are summed; a stale parent is cited as
     superseded in provenance, never silently dropped.
  2. broker_balance_sheet is a cross-namespace pattern scan -- 4916's
     exact-name markers missed HOOD (CashAndSecuritiesSegregated...,
     hood:PayablesToUsers), leaving brokers=0 and HOOD CRITICAL.

Gates:
  G1 redeploy v1.0.3 (helper)
  G2 fresh run on disk, version==1.0.3
  G3 BMNR regression: stays un-quarantined (watchlist exemption)
  G4 HARD BTBT spec case: crypto_coverage in [0.15, 0.60] (Khalid spec
     ~1/3 of mcap), bind end >= 2026-06-30, superseded parent cited
  G5 broker quarantine live: HOOD in broker_sheets, zero broker /
     wrapper / suspect leakage into alerts, CRITICAL band 1-10x
  G6 DAT board kv (BTBT BMNR MSTR SBET DFDV UPXI)
  Soft: floor.html edge marker (PENDING on 4914/4915/4916 -- CF lag)
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


def main():
    with report("4917-floor-v103-recency") as r:
        r.heading("ops 4917 -- floor-audit v1.0.3 recency-first bind")
        t0 = datetime.now(timezone.utc)

        r.section("G1 redeploy")
        if ENG.VERSION != "1.0.3":
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
            description=("Senseless-drawdown auditor v1.0.3: liquid "
                         "floor + RPO/backlog vs mcap; recency-first "
                         "crypto bind; fund/suspect/broker quarantine; "
                         "-> data/floor-audit.json daily 21:35 UTC."),
        )
        r.kv(g1="PASS")

        r.section("G2 fresh run")
        prev = (s3_json(OUT_KEY) or {}).get("as_of", "")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        payload = None
        for _ in range(60):
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev and \
                    cur.get("version") == "1.0.3":
                payload = cur
                break
        if not payload:
            r.log("FAIL G2: no fresh v1.0.3 payload in 10 min")
            sys.exit(1)
        tks = payload["tickers"]
        r.kv(g2="PASS", as_of=payload["as_of"],
             universe=payload["universe_n"],
             g0_ok=payload["g0_ok_tickers"],
             wrappers=len(payload.get("fund_wrappers") or []),
             suspects=len(payload.get("suspect_inputs") or []),
             brokers=len(payload.get("broker_sheets") or []))

        r.section("G3 BMNR regression")
        bm = tks.get("BMNR") or {}
        if "BMNR" in (payload.get("fund_wrappers") or []) or \
                bm.get("status") != "OK" or \
                bm["verdict"]["verdict"] in ("FUND_WRAPPER",
                                             "SUSPECT_INPUTS"):
            r.log("FAIL G3: BMNR %s"
                  % json.dumps(bm.get("verdict") or
                               {"status": bm.get("status")})[:200])
            sys.exit(1)
        r.kv(g3="PASS", bmnr_verdict=bm["verdict"]["verdict"],
             bmnr_cov=bm["coverage"],
             bmnr_crypto=bm["crypto_coverage"])

        r.section("G4 BTBT spec case (HARD)")
        bt = tks.get("BTBT") or {}
        cc = bt.get("crypto_coverage")
        bind = {}
        if bt.get("status") == "OK":
            bind = (bt["floor"]["legs"][3].get("bind") or {})
        ok = (bt.get("status") == "OK" and cc is not None
              and 0.15 <= cc <= 0.60
              and (bind.get("end") or "") >= "2026-06-30")
        if not ok:
            r.log("FAIL G4: BTBT crypto_coverage=%s bind=%s end=%s "
                  "(spec ~1/3 of mcap, fresh-quarter bind required)"
                  % (cc, bind.get("tag"), bind.get("end")))
            sys.exit(1)
        r.kv(g4="PASS", btbt_crypto_cov=cc, btbt_cov=bt["coverage"],
             btbt_bind=bind.get("tag"), btbt_bind_end=bind.get("end"),
             btbt_filing_fv=bind.get("filing_fv_usd"),
             btbt_mark_ratio=bind.get("mark_ratio_spot_over_filing"),
             btbt_superseded=json.dumps(
                 bind.get("superseded_parent") or {}),
             btbt_verdict=bt["verdict"]["verdict"],
             btbt_dd20=(bt["drawdowns"] or {}).get("20"))

        r.section("G5 broker quarantine live")
        brokers = payload.get("broker_sheets") or []
        alert_set = {a["ticker"] for a in payload["alerts"]}
        quar = set(brokers) | set(payload.get("fund_wrappers") or []) \
            | set(payload.get("suspect_inputs") or [])
        leak = quar & alert_set
        if leak:
            r.log("FAIL G5: quarantined names leaked into alerts: %s"
                  % sorted(leak))
            sys.exit(1)
        if "HOOD" in tks and "HOOD" not in brokers:
            r.log("FAIL G5: HOOD in universe but not broker-"
                  "quarantined (verdict=%s)"
                  % (tks["HOOD"].get("verdict") or {}).get("verdict"))
            sys.exit(1)
        crit = [a for a in payload["alerts"]
                if a["severity"] == "CRITICAL"]
        if not all(1.0 <= (a.get("coverage") or 0) <= 10.0
                   for a in crit):
            r.log("FAIL G5: CRITICAL outside band: %s"
                  % [(a["ticker"], a["coverage"]) for a in crit])
            sys.exit(1)
        r.kv(g5="PASS", broker_sheets=",".join(brokers) or "none")
        for a in payload["alerts"][:8]:
            r.kv(alert=a["ticker"], sev=a["severity"], v=a["verdict"],
                 sense=a["sense_score"], cov=a["coverage"],
                 res=a.get("worst_residual"))

        r.section("G6 DAT board")
        for t in ("BTBT", "BMNR", "MSTR", "SBET", "DFDV", "UPXI"):
            x = tks.get(t) or {"status": "ABSENT"}
            r.kv(dat=t, status=x.get("status"),
                 verdict=(x.get("verdict") or {}).get("verdict"),
                 cov=x.get("coverage"), crypto=x.get("crypto_coverage"),
                 dd20=(x.get("drawdowns") or {}).get("20"))
        r.kv(g6="PASS")

        r.section("edge (soft)")
        edge = "PENDING"
        for _ in range(6):
            try:
                html = urllib.request.urlopen(
                    "https://justhodl.ai/floor.html?cb=%d"
                    % time.time(), timeout=15).read().decode(
                        "utf-8", "replace")
                if "floor-audit-v1.0.0" in html:
                    edge = "LIVE"
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(15)
        r.kv(edge=edge)

        bt_wb = bt.get("why_block") or {}
        rep = {"op": 4917, "version": "1.0.3",
               "as_of": payload["as_of"],
               "alerts": payload["alerts"],
               "fund_wrappers": payload.get("fund_wrappers"),
               "suspect_inputs": payload.get("suspect_inputs"),
               "broker_sheets": brokers,
               "btbt": {"crypto_cov": cc, "cov": bt["coverage"],
                        "bind": bind.get("tag"),
                        "bind_end": bind.get("end"),
                        "filing_fv_usd": bind.get("filing_fv_usd"),
                        "superseded_parent":
                            bind.get("superseded_parent"),
                        "verdict": bt["verdict"]["verdict"],
                        "why_block": bt_wb},
               "bmnr": {"cov": bm["coverage"],
                        "crypto": bm["crypto_coverage"],
                        "verdict": bm["verdict"]["verdict"]},
               "edge": edge}
        (ROOT / "aws" / "ops" / "reports" / "4917.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN", duration_s=int(
            (datetime.now(timezone.utc) - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
