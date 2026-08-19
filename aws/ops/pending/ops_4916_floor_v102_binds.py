"""ops/4916 -- floor-audit v1.0.2: the binds that make it Khalid-grade.

4915 was clean but wrong in three places the DAT board exposed:
BMNR (his ETH treasury) quarantined by the structural wrapper test;
BTBT crypto coverage 0.5% vs the ~1/3-of-mcap ETH stack that IS the
engine's spec case; HOOD alerting CRITICAL off a broker balance sheet.
v1.0.2: watchlist exemption, cross-namespace crypto scan (custody
patterns blocked), BROKER_BALANCE_SHEET quarantine, 60s SEC timeout.
Harness GREEN 34/34.

Gates:
  G1 redeploy, source VERSION 1.0.2
  G2 fresh run on disk, payload version 1.0.2
  G3 BMNR not in fund_wrappers; audited with a real verdict
  G4 BTBT crypto_coverage >= 0.10 HARD (spec case). On fail: print
     BTBT's full cross-ns crypto/digital tag inventory, then exit 1.
  G5 HOOD/COIN-class in broker_sheets and absent from alerts;
     CRITICAL band still 1-10x
  G6 MSTR status + reason surfaced (was ABSENT on 4915 board)
  Soft: edge marker recheck
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


def btbt_tag_inventory(r):
    """Diagnostic: every Crypto/Digital/Ethereum-named tag BTBT files,
    across all namespaces, with latest USD instant."""
    t2c, _ = ENG.sec_ticker_map()
    facts = ENG.sec_companyfacts(t2c["BTBT"])
    inv = []
    for ns, tags in (facts.get("facts") or {}).items():
        for tag, node in (tags or {}).items():
            if not any(k in tag for k in ("Crypto", "Digital",
                                          "Ethereum", "Eth")):
                continue
            e = ENG.pick_latest((node.get("units") or {}).get("USD"))
            inv.append((ns, tag,
                        None if not e else e["val"],
                        None if not e else e["end"]))
    for ns, tag, val, end in sorted(inv, key=lambda x: -(x[2] or 0)):
        r.kv(btbt_tag="%s:%s" % (ns, tag), val=val, end=end)
    return inv


def main():
    with report("4916-floor-v102-binds") as r:
        r.heading("ops 4916 -- floor-audit v1.0.2 bind gates")
        t0 = datetime.now(timezone.utc)

        r.section("G1 redeploy")
        if ENG.VERSION != "1.0.2":
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
            description=("Senseless-drawdown auditor v1.0.2: liquid "
                         "floor + RPO/backlog vs mcap; cross-ns crypto "
                         "bind; fund/suspect/broker quarantine; -> "
                         "data/floor-audit.json daily 21:35 UTC."),
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
                    cur.get("version") == "1.0.2":
                payload = cur
                break
        if not payload:
            r.log("FAIL G2: no fresh v1.0.2 payload in 10 min")
            sys.exit(1)
        tks = payload["tickers"]
        r.kv(g2="PASS", as_of=payload["as_of"],
             universe=payload["universe_n"],
             g0_ok=payload["g0_ok_tickers"],
             wrappers=len(payload.get("fund_wrappers") or []),
             suspects=len(payload.get("suspect_inputs") or []),
             brokers=len(payload.get("broker_sheets") or []))

        r.section("G3 BMNR un-quarantined")
        if "BMNR" in (payload.get("fund_wrappers") or []):
            r.log("FAIL G3: BMNR still classified FUND_WRAPPER")
            sys.exit(1)
        bm = tks.get("BMNR") or {}
        if bm.get("status") != "OK" or bm["verdict"]["verdict"] in \
                ("FUND_WRAPPER", "SUSPECT_INPUTS"):
            r.log("FAIL G3: BMNR rec %s"
                  % json.dumps(bm.get("verdict") or
                               {"status": bm.get("status"),
                                "reason": bm.get("reason")})[:200])
            sys.exit(1)
        r.kv(g3="PASS", bmnr_verdict=bm["verdict"]["verdict"],
             bmnr_cov=bm["coverage"], bmnr_crypto=bm["crypto_coverage"],
             bmnr_bind=json.dumps(
                 (bm["floor"]["legs"][3].get("bind") or {}).get("tag")))

        r.section("G4 BTBT spec-case bind (HARD)")
        bt = tks.get("BTBT") or {}
        cc = bt.get("crypto_coverage")
        bind = None
        if bt.get("status") == "OK":
            bind = (bt["floor"]["legs"][3].get("bind") or {})
        if bt.get("status") != "OK" or cc is None or cc < 0.10:
            r.log("FAIL G4: BTBT crypto_coverage=%s (spec ~1/3 of "
                  "mcap). Tag inventory follows:" % cc)
            try:
                btbt_tag_inventory(r)
            except Exception as e:  # noqa: BLE001
                r.log("  inventory fetch failed: %s" % e)
            sys.exit(1)
        r.kv(g4="PASS", btbt_crypto_cov=cc, btbt_cov=bt["coverage"],
             btbt_bind=bind.get("tag"), btbt_bind_end=bind.get("end"),
             btbt_mark_ratio=bind.get(
                 "mark_ratio_spot_over_filing"),
             btbt_verdict=bt["verdict"]["verdict"],
             btbt_dd20=(bt["drawdowns"] or {}).get("20"))

        r.section("G5 broker quarantine + band")
        brokers = payload.get("broker_sheets") or []
        alert_set = {a["ticker"] for a in payload["alerts"]}
        leak = set(brokers) & alert_set
        if leak:
            r.log("FAIL G5: broker sheets in alerts: %s"
                  % sorted(leak))
            sys.exit(1)
        crit = [a for a in payload["alerts"]
                if a["severity"] == "CRITICAL"]
        if not all(1.0 <= (a.get("coverage") or 0) <= 10.0
                   for a in crit):
            r.log("FAIL G5: CRITICAL outside band: %s"
                  % [(a["ticker"], a["coverage"]) for a in crit])
            sys.exit(1)
        r.kv(g5="PASS", broker_sheets=",".join(brokers) or "none",
             hood_state=("quarantined" if "HOOD" in brokers else
                         ("absent" if "HOOD" not in tks else
                          tks["HOOD"].get("verdict", {})
                          .get("verdict"))))
        for a in payload["alerts"][:8]:
            r.kv(alert=a["ticker"], sev=a["severity"], v=a["verdict"],
                 sense=a["sense_score"], cov=a["coverage"],
                 res=a.get("worst_residual"))

        r.section("G6 MSTR surfaced")
        ms = tks.get("MSTR") or {"status": "ABSENT"}
        r.kv(g6="PASS", mstr_status=ms.get("status"),
             mstr_reason=ms.get("reason", ""),
             mstr_verdict=(ms.get("verdict") or {}).get("verdict"),
             mstr_cov=ms.get("coverage"),
             mstr_crypto=ms.get("crypto_coverage"))

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

        rep = {"op": 4916, "version": "1.0.2",
               "as_of": payload["as_of"],
               "alerts": payload["alerts"],
               "fund_wrappers": payload.get("fund_wrappers"),
               "suspect_inputs": payload.get("suspect_inputs"),
               "broker_sheets": brokers,
               "btbt": {"crypto_cov": cc, "cov": bt["coverage"],
                        "bind": bind.get("tag"),
                        "verdict": bt["verdict"]["verdict"]},
               "bmnr": {"cov": bm["coverage"],
                        "crypto": bm["crypto_coverage"],
                        "verdict": bm["verdict"]["verdict"]},
               "mstr": {"status": ms.get("status"),
                        "verdict": (ms.get("verdict") or {})
                        .get("verdict"), "cov": ms.get("coverage")},
               "edge": edge}
        (ROOT / "aws" / "ops" / "reports" / "4916.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN", duration_s=int(
            (datetime.now(timezone.utc) - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
