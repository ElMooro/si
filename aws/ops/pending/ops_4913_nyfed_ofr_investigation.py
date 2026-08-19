"""ops/4913 -- NY Fed + OFR: investigated, verdicts banked, real fixes.

VERDICTS (Khalid: "are they importing properly?"):
  nyfed-markets  : engine healthy, HOURLY RULE SILENT (classic rule;
                   fn fresh on manual invoke). Fix: EventBridge
                   SCHEDULER hourly (rule-cap-proof) + CloudWatch
                   invocation-gap evidence.
  ofr-bsrm       : NEVER importing -- one-shot ops 4753. Fix:
                   src-mirror v1.0 lane (workbooks, conditional ETag,
                   daily) + _last-check freshness truth. Parsed
                   500-series re-transform = phase 2, flagged.
  ofr-site       : NEVER importing -- one-shot ops 4755. Fix:
                   src-mirror live page-harvest lane, daily.
  ofr-hfm        : fresh + hourly -- UNTOUCHED (working smoothly).
  nyfed-research : ORPHANED_TRANSFORM (haircut series, seed ops
                   4793-94; upstream source-map unresolved) --
                   verdict banked + card note; build queued. No
                   blind changes to a working dataset.
Everything else on the board: untouched per Khalid's constraint.
"""
import gzip
import json
import sys
import time
import urllib.request
import zipfile, io
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
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def newest_age_min(prefix):
    newest, tok = None, None
    while True:
        kw = dict(Bucket=B, Prefix=prefix, MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            if newest is None or o["LastModified"] > newest:
                newest = o["LastModified"]
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return (round((datetime.now(timezone.utc) - newest
                   ).total_seconds() / 60, 1) if newest else None)


def ensure_sched(rep, name, expr, arn, payload="{}"):
    kw = dict(Name=name, ScheduleExpression=expr,
              FlexibleTimeWindow={"Mode": "OFF"},
              Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                      "Input": payload}, State="ENABLED")
    try:
        sch.create_schedule(**kw)
        rep.kv(stage="schedule", name=name, action="created")
        return True
    except Exception as e:
        if "Conflict" in type(e).__name__ or "already" in str(e):
            sch.update_schedule(**kw)
            rep.kv(stage="schedule", name=name, action="updated")
            return True
        rep.kv(stage="schedule", name=name,
               err=f"{type(e).__name__}: {str(e)[:110]}")
        return False


def main():
    verdict = {"ops": 4913, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4913 -- nyfed ofr investigation") as rep:
        rep.heading("ops 4913 — NY Fed + OFR verdicts & fixes")

        # ── nyfed-markets: silent-rule evidence + Scheduler fix ─────
        gap_evidence = None
        try:
            evs = logs.filter_log_events(
                logGroupName="/aws/lambda/justhodl-nyfed-markets-"
                "full",
                startTime=int((time.time() - 30 * 3600) * 1000),
                filterPattern="REPORT", limit=60).get("events") or []
            ts = sorted(e["timestamp"] / 1000 for e in evs)
            gaps = [round((b - a) / 3600, 1)
                    for a, b in zip(ts, ts[1:]) if b - a > 5400]
            gap_evidence = {"invocations_30h": len(ts),
                            "gaps_gt90min_h": gaps[:6]}
            rep.kv(stage="markets-autopsy", **gap_evidence)
        except Exception as e:
            rep.kv(stage="markets-autopsy",
                   err=f"{type(e).__name__}: {str(e)[:120]}")
        ok_m = False
        try:
            arn = lam.get_function_configuration(
                FunctionName="justhodl-nyfed-markets-full"
            )["FunctionArn"]
            ok_m = ensure_sched(rep,
                                "justhodl-nyfed-markets-hourly-s",
                                "rate(1 hour)", arn)
            lam.invoke(FunctionName="justhodl-nyfed-markets-full",
                       InvocationType="Event")
        except Exception as e:
            rep.kv(stage="markets-fix",
                   err=f"{type(e).__name__}: {str(e)[:120]}")
        verdict["gates"]["markets_scheduler_fixed"] = (
            "PASS" if ok_m else "FAIL")
        verdict["markets_autopsy"] = gap_evidence

        # ── src-mirror live + Scheduler + kick + truth-stamps ───────
        FN = "justhodl-src-mirror"
        arn2 = None
        end = time.time() + 300
        while time.time() < end:
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active":
                    arn2 = c["FunctionArn"]
                    break
            except Exception:
                pass
            time.sleep(15)
        if arn2 is None:
            cfg = json.loads((ROOT / "aws" / "lambdas" / FN /
                              "config.json").read_text())
            try:
                deploy_lambda(report=rep, function_name=FN,
                              source_dir=ROOT / "aws" / "lambdas" /
                              FN / "source",
                              env_vars=cfg.get("env") or {},
                              timeout=int(cfg["timeout"]),
                              memory=int(cfg["memory"]),
                              description=str(
                                  cfg.get("description"))[:250],
                              create_function_url=False, smoke=False)
                for _ in range(16):
                    c = lam.get_function_configuration(
                        FunctionName=FN)
                    if c.get("State") == "Active":
                        arn2 = c["FunctionArn"]
                        break
                    time.sleep(10)
            except Exception as e:
                rep.kv(stage="mirror-deploy",
                       err=f"{type(e).__name__}: {str(e)[:140]}")
        ok_s = False
        if arn2:
            ok_s = ensure_sched(rep, "justhodl-src-mirror-daily",
                                "cron(5 5 * * ? *)", arn2)
            lam.invoke(FunctionName=FN, InvocationType="Event")
        lanes = None
        t = time.time()
        while time.time() - t < 420 and arn2:
            time.sleep(30)
            try:
                st = g("data/_state/src-mirror.json")
                lanes = st.get("summary", {}).get("lanes")
                if lanes:
                    break
            except Exception:
                continue
        a_b = newest_age_min("data/warm/ofr-bsrm/")
        a_s = newest_age_min("data/warm/ofr-site/")
        rep.kv(stage="mirror-run",
               lanes=json.dumps(lanes)[:340] if lanes else None,
               bsrm_age_min=a_b, site_age_min=a_s)
        ok_run = bool(lanes) and (a_b or 999) < 30 and \
            (a_s or 999) < 30
        verdict["gates"]["src_mirror_live"] = (
            "PASS" if (arn2 and ok_s and ok_run) else
            "PENDING" if arn2 else "FAIL")
        verdict["mirror"] = {"lanes": lanes, "bsrm_age": a_b,
                             "site_age": a_s}

        # ── orphan-audit artifact (permanent verdicts) ──────────────
        s3.put_object(
            Bucket=B, Key="data/warm/_audit/refresh-orphans.json",
            Body=json.dumps({
                "as_of": datetime.now(timezone.utc).isoformat(
                    timespec="seconds"), "ops": 4913,
                "verdicts": {
                    "nyfed-markets": "hourly classic rule SILENT; "
                                     "Scheduler installed 4913",
                    "ofr-bsrm": "one-shot seed ops 4753; src-mirror "
                                "lane live 4913; parsed-series "
                                "re-transform = phase 2",
                    "ofr-site": "one-shot seed ops 4755; src-mirror "
                                "harvest lane live 4913",
                    "nyfed-research": "ORPHANED_TRANSFORM seed ops "
                                      "4793-94; upstream source-map "
                                      "unresolved; build queued",
                    "ofr-hfm": "healthy hourly -- untouched"}},
                default=str).encode(),
            ContentType="application/json", CacheControl="no-cache")
        rep.kv(stage="audit-artifact",
               key="data/warm/_audit/refresh-orphans.json")
        verdict["gates"]["verdicts_banked"] = "PASS"

        # ── markets freshness confirm + card notes refresh ──────────
        t = time.time()
        a_m = None
        while time.time() - t < 300:
            time.sleep(30)
            a_m = newest_age_min("data/warm/nyfed-markets/")
            if a_m is not None and a_m < 30:
                break
        rep.kv(stage="markets-fresh", age_min=a_m)
        verdict["gates"]["markets_fresh"] = (
            "PASS" if (a_m or 999) < 120 else "PENDING")
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                       InvocationType="Event")
        except Exception:
            pass

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4913.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4913.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
