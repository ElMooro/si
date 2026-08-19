"""ops/4914 -- justhodl-floor-audit birth: deploy + schedule + first run
verified on disk.

Engine: senseless-drawdown auditor (Khalid doctrine 2026-08-19). Liquid
floor = cash + investments + LIVE-marked company crypto - debt; every
dump decomposed into asset-driven vs residual; RPO/backlog leg;
SEC-frames auto-discovery of every CryptoAssetFairValue filer; custody
crypto blocked. Local harness GREEN (21/21 identities) before this push.

Gates (each hard, sys.exit(1)):
  G1 POLYGON_API_KEY recovered from justhodl-equity-research live env
  G2 deploy_lambda green (create-or-update + smoke)
  G3 EventBridge Scheduler ensured (daily 21:35 UTC M-F)
  G4 async first run -> data/floor-audit.json fresh as_of ON DISK
  G5 payload G0 re-asserted from S3 (fields, >=60% OK, BTBT-class rec)
  G6 history snapshot on disk
  Soft: edge poll for floor.html marker (pages.yml races this op; WARN
  only -- repo state is not proof of live, but CF can lag the same push).
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-floor-audit"
OUT_KEY = "data/floor-audit.json"
HIST_PREFIX = "data/floor-audit/history/"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
SRC = ROOT / "aws" / "lambdas" / FN / "source"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)


def s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B,
                                        Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def main():
    with report("4914-floor-audit-birth") as r:
        r.heading("ops 4914 -- floor-audit birth")
        t0 = datetime.now(timezone.utc)

        # G1 -- inherit POLYGON_API_KEY exactly as deploy-lambdas would
        r.section("G1 key inheritance")
        src_env = lam.get_function_configuration(
            FunctionName="justhodl-equity-research"
        ).get("Environment", {}).get("Variables", {})
        poly = src_env.get("POLYGON_API_KEY")
        if not poly:
            r.log("FAIL G1: POLYGON_API_KEY absent on donor fn")
            sys.exit(1)
        r.kv(g1="PASS", donor="justhodl-equity-research",
             key_len=len(poly))

        # G2 -- deploy via house helper (idempotent create-or-update)
        r.section("G2 deploy")
        deploy_lambda(
            report=r, function_name=FN, source_dir=SRC,
            env_vars={"POLYGON_API_KEY": poly},
            timeout=600, memory=1024,
            description=("Senseless-drawdown auditor: liquid floor + "
                         "RPO/backlog vs mcap; asset-driven vs residual "
                         "decomposition; SEC XBRL+frames + Polygon -> "
                         "data/floor-audit.json daily 21:35 UTC."),
        )
        r.kv(g2="PASS", fn=FN)

        # G3 -- EventBridge Scheduler (rule cap doctrine: Scheduler only)
        r.section("G3 schedule")
        fn_arn = lam.get_function(FunctionName=FN)["Configuration"][
            "FunctionArn"]
        sched = {
            "Name": FN + "-daily",
            "ScheduleExpression": "cron(35 21 ? * MON-FRI *)",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": fn_arn, "RoleArn": SCHED_ROLE,
                       "Input": "{}"},
            "State": "ENABLED",
        }
        try:
            sch.create_schedule(**sched)
            r.kv(g3="PASS", schedule="created")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sched)
            r.kv(g3="PASS", schedule="updated")

        # G4 -- first run, async, verified ON DISK
        r.section("G4 first run")
        prev = s3_json(OUT_KEY) or {}
        prev_asof = prev.get("as_of", "")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        r.log("async invoke fired; polling %s for fresh as_of "
              "(prev=%s)" % (OUT_KEY, prev_asof or "none"))
        payload = None
        for i in range(60):  # up to 10 min
            time.sleep(10)
            cur = s3_json(OUT_KEY)
            if cur and cur.get("as_of", "") > prev_asof and \
                    cur.get("as_of", "") >= t0.isoformat(
                        timespec="seconds")[:16]:
                payload = cur
                break
            if i % 6 == 5:
                r.log("  poll %ds: not fresh yet" % ((i + 1) * 10))
        if not payload:
            r.log("FAIL G4: no fresh payload within 10 min -- check "
                  "CloudWatch %s" % FN)
            sys.exit(1)
        r.kv(g4="PASS", as_of=payload["as_of"],
             universe=payload.get("universe_n"),
             g0_ok=payload.get("g0_ok_tickers"),
             alerts=len(payload.get("alerts") or []),
             discovered=len(payload.get("discovered") or []))

        # G5 -- re-assert G0 from S3 (independent of engine's own gate)
        r.section("G5 payload assertions")
        tks = payload.get("tickers") or {}
        ok = [t for t, x in tks.items() if x.get("status") == "OK"]
        if not (payload.get("engine") == FN and len(tks) > 0 and
                len(ok) >= max(3, int(0.6 * len(tks)))):
            r.log("FAIL G5: hollow payload ok=%d/%d" % (len(ok),
                                                        len(tks)))
            sys.exit(1)
        req = ("mcap_usd", "floor", "coverage", "drawdowns",
               "decomposition", "verdict", "why_block")
        probe = None
        for want in ("BTBT", "BMNR", "MSTR"):
            if want in ok:
                probe = want
                break
        probe = probe or ok[0]
        rec = tks[probe]
        missing = [f for f in req if rec.get(f) is None]
        if missing:
            r.log("FAIL G5: %s missing %s" % (probe, missing))
            sys.exit(1)
        c20 = (rec.get("decomposition") or {}).get("20") or {}
        r.kv(g5="PASS", probe=probe,
             verdict=rec["verdict"]["verdict"],
             severity=rec["verdict"]["severity"],
             coverage=rec["coverage"],
             crypto_coverage=rec.get("crypto_coverage"),
             nlav_musd=round((rec["floor"]["nlav"] or 0) / 1e6, 1),
             mcap_musd=round(rec["mcap_usd"] / 1e6, 1),
             dd20=(rec["drawdowns"] or {}).get("20"),
             residual20=c20.get("residual"),
             sense=rec["verdict"].get("sense_score"))
        for a in (payload.get("alerts") or [])[:5]:
            r.kv(alert=a["ticker"], sev=a["severity"], v=a["verdict"],
                 sense=a["sense_score"], cov=a["coverage"])
        broken = sorted({x.get("backlog_status") for x in tks.values()
                         if str(x.get("backlog_status", ""))
                         .startswith("JOIN_BROKEN")})
        if broken:
            r.kv(backlog_join="BROKEN -- surfaced honestly",
                 detail=";".join(broken))
        else:
            r.kv(backlog_join="bound")

        # G6 -- history snapshot
        r.section("G6 history")
        day = payload["as_of"][:10]
        if not s3_json(HIST_PREFIX + day + ".json"):
            r.log("FAIL G6: history snapshot missing")
            sys.exit(1)
        r.kv(g6="PASS", snapshot=HIST_PREFIX + day + ".json")

        # Soft -- edge poll for the page marker (pages.yml same push)
        r.section("edge (soft)")
        edge = "PENDING"
        for _ in range(9):
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
            time.sleep(20)
        r.kv(edge=edge,
             note=("marker served" if edge == "LIVE" else
                   "pages.yml may still be publishing -- repo state "
                   "is not proof of live; recheck next op if PENDING"))

        # JSON report (house convention)
        rep = {"op": 4914, "fn": FN, "as_of": payload["as_of"],
               "universe": payload.get("universe_n"),
               "g0_ok": payload.get("g0_ok_tickers"),
               "alerts": payload.get("alerts"),
               "discovered": payload.get("discovered"),
               "edge": edge}
        (ROOT / "aws" / "ops" / "reports" / "4914.json").write_text(
            json.dumps(rep, indent=1))
        r.kv(status="GREEN",
             feed="https://justhodl.ai/data/floor-audit.json",
             page="https://justhodl.ai/floor.html")


if __name__ == "__main__":
    main()
    sys.exit(0)
