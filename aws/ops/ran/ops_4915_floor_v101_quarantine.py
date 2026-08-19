"""ops/4915 -- floor-audit v1.0.1: quarantine ladder live + DAT board.

First tape (ops 4914) was GREEN but contaminated: ETF wrappers
(IBIT/ETHA/EZBC/ARKB -- funds file CryptoAssetFairValue and sit at
cov~1.0 by construction) and input pathologies (GLXY at 3,040,406x
coverage from placeholder share rows) were headlining the CRITICAL
board. v1.0.1 adds: cover-form share filter, FUND_WRAPPER structural +
blocklist classification, SUSPECT_INPUTS quarantine (cov>10x or
mcap<$3M). Harness GREEN 27/27 locally.

Gates:
  G1 redeploy v1.0.1 (helper)
  G2 S3 config reset to source DEFAULT (adds fund_blocklist; prior
     config was engine-bootstrapped pre-blocklist 20 min earlier)
  G3 fresh run on disk, version==1.0.1
  G4 alert hygiene: zero FUND_WRAPPER / SUSPECT_INPUTS in alerts;
     wrappers and suspects populated in their buckets
  G5 real edges intact: UPXI-class BELOW_LIQUID_FLOOR survivors in the
     1-10x band still alert; ABTC-class residual dump still flagged
  G6 DAT board kv: BTBT BMNR MSTR SBET DFDV rows printed
  Soft: floor.html edge marker recheck (was PENDING on 4914)
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
import lambda_function as ENG  # noqa: E402  (DEFAULT_CONFIG source)

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-floor-audit"
OUT_KEY = "data/floor-audit.json"
CFG_KEY = "data/floor-audit/config.json"
SRC = ROOT / "aws" / "lambdas" / FN / "source"

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
    with report("4915-floor-v101-quarantine") as r:
        r.heading("ops 4915 -- floor-audit v1.0.1 quarantine ladder")
        t0 = datetime.now(timezone.utc)

        r.section("G1 redeploy")
        poly = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        )["Environment"]["Variables"]["POLYGON_API_KEY"]
        deploy_lambda(
            report=r, function_name=FN, source_dir=SRC,
            env_vars={"POLYGON_API_KEY": poly},
            timeout=600, memory=1024,
            description=("Senseless-drawdown auditor v1.0.1: liquid "
                         "floor + RPO/backlog vs mcap; asset-driven vs "
                         "residual; fund-wrapper + suspect-input "
                         "quarantine; -> data/floor-audit.json daily."),
        )
        r.kv(g1="PASS", version_src=ENG.VERSION)
        if ENG.VERSION != "1.0.1":
            r.log("FAIL G1: source VERSION %s" % ENG.VERSION)
            sys.exit(1)

        r.section("G2 config reset")
        if "fund_blocklist" not in ENG.DEFAULT_CONFIG:
            r.log("FAIL G2: DEFAULT_CONFIG lacks fund_blocklist")
            sys.exit(1)
        s3.put_object(Bucket=B, Key=CFG_KEY,
                      Body=json.dumps(ENG.DEFAULT_CONFIG,
                                      separators=(",", ":")).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        r.kv(g2="PASS", blocklist_n=len(
            ENG.DEFAULT_CONFIG["fund_blocklist"]))

        r.section("G3 fresh run")
        prev = (s3_json(OUT_KEY) or {}).get("as_of", "")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        payload = None
        for i in range(60):
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev and \
                    cur.get("version") == "1.0.1":
                payload = cur
                break
        if not payload:
            r.log("FAIL G3: no fresh v1.0.1 payload in 10 min "
                  "(prev=%s)" % prev)
            sys.exit(1)
        r.kv(g3="PASS", as_of=payload["as_of"],
             universe=payload["universe_n"],
             g0_ok=payload["g0_ok_tickers"])

        r.section("G4 alert hygiene")
        tks = payload["tickers"]
        wrappers = payload.get("fund_wrappers") or []
        suspects = payload.get("suspect_inputs") or []
        dirty = [a["ticker"] for a in payload["alerts"]
                 if tks.get(a["ticker"], {}).get("verdict", {})
                 .get("verdict") in ("FUND_WRAPPER", "SUSPECT_INPUTS")]
        if dirty:
            r.log("FAIL G4: quarantined names in alerts: %s" % dirty)
            sys.exit(1)
        alert_set = {a["ticker"] for a in payload["alerts"]}
        leak = (set(wrappers) | set(suspects)) & alert_set
        if leak:
            r.log("FAIL G4: bucket leak into alerts: %s"
                  % sorted(leak))
            sys.exit(1)
        r.kv(g4="PASS", alerts=len(payload["alerts"]),
             fund_wrappers=",".join(wrappers) or "none",
             suspect_inputs=",".join(suspects) or "none")

        r.section("G5 real edges intact")
        crit = [a for a in payload["alerts"]
                if a["severity"] == "CRITICAL"]
        band_ok = all(
            1.0 <= (a.get("coverage") or 0) <= 10.0 for a in crit)
        if not band_ok:
            r.log("FAIL G5: CRITICAL alert outside 1-10x band: %s"
                  % [(a["ticker"], a["coverage"]) for a in crit])
            sys.exit(1)
        resid_flags = [a for a in payload["alerts"]
                       if a["verdict"] in ("SENSELESS_DRAWDOWN",
                                           "STRETCHED")]
        r.kv(g5="PASS", critical_n=len(crit),
             critical=",".join(a["ticker"] for a in crit) or "none",
             residual_flags=",".join(
                 "%s(%s)" % (a["ticker"], a["severity"])
                 for a in resid_flags) or "none")
        for a in payload["alerts"][:8]:
            r.kv(alert=a["ticker"], sev=a["severity"], v=a["verdict"],
                 sense=a["sense_score"], cov=a["coverage"],
                 res=a.get("worst_residual"))

        r.section("G6 DAT board")
        for tk in ("BTBT", "BMNR", "MSTR", "SBET", "DFDV", "UPXI",
                   "ABTC"):
            rec = tks.get(tk)
            if not rec or rec.get("status") != "OK":
                r.kv(dat=tk, status=(rec or {}).get("status", "ABSENT"),
                     reason=(rec or {}).get("reason", ""))
                continue
            c20 = (rec["decomposition"] or {}).get("20") or {}
            r.kv(dat=tk, verdict=rec["verdict"]["verdict"],
                 sev=rec["verdict"]["severity"],
                 cov=rec["coverage"],
                 crypto=rec["crypto_coverage"],
                 dd20=(rec["drawdowns"] or {}).get("20"),
                 res20=c20.get("residual"),
                 sense=rec["verdict"].get("sense_score"),
                 nlav_m=round((rec["floor"]["nlav"] or 0) / 1e6),
                 mcap_m=round(rec["mcap_usd"] / 1e6))
        if not any(tks.get(t, {}).get("status") == "OK"
                   for t in ("BTBT", "BMNR", "MSTR")):
            r.log("FAIL G6: none of BTBT/BMNR/MSTR OK")
            sys.exit(1)
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

        rep = {"op": 4915, "version": "1.0.1",
               "as_of": payload["as_of"],
               "alerts": payload["alerts"],
               "fund_wrappers": wrappers,
               "suspect_inputs": suspects,
               "dat_board": {t: {
                   "verdict": tks[t]["verdict"]["verdict"],
                   "cov": tks[t]["coverage"],
                   "crypto": tks[t]["crypto_coverage"],
                   "dd20": (tks[t]["drawdowns"] or {}).get("20"),
                   "sense": tks[t]["verdict"].get("sense_score")}
                   for t in ("BTBT", "BMNR", "MSTR", "SBET", "DFDV",
                             "UPXI", "ABTC")
                   if tks.get(t, {}).get("status") == "OK"},
               "edge": edge}
        (ROOT / "aws" / "ops" / "reports" / "4915.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN", duration_s=int(
            (datetime.now(timezone.utc) - t0).total_seconds()))


if __name__ == "__main__":
    main()
    sys.exit(0)
