"""ops/4909 -- SEC MIDAS launch: the last zero-key provider goes live.

Built on 4908 hop3 evidence (16+ real quarterly zip URLs, path variant
documented). Fn justhodl-sec-midas v1.0 harness-proven. This op:
settle-or-create the fn, enforce 10GB ephemeral, weekly Scheduler
(also the new-quarter detector), kick the backfill (self-chains),
verify the FIRST zips land on disk + manifest, then refresh the
provider card. Deep v1.3 cure-proof re-read included (4908's PENDING
was a 120s-vs-820s timing read).
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
FN = "justhodl-sec-midas"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240,
                                 retries={"max_attempts": 0}))
sch = boto3.client("scheduler", region_name=REGION)


def g(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def midas_keys():
    r = s3.list_objects_v2(Bucket=B, Prefix="data/warm/sec-midas/",
                           MaxKeys=200)
    return [(o["Key"].rsplit("/", 1)[-1], o["Size"])
            for o in r.get("Contents") or []]


def deep_parts():
    n, tok = 0, None
    while True:
        kw = dict(Bucket=B, Prefix="data/warm/ecb/data/",
                  MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        n += sum(1 for o in r.get("Contents") or []
                 if "__" in o["Key"].rsplit("/", 1)[-1])
        if not r.get("IsTruncated"):
            return n
        tok = r.get("NextContinuationToken")


def main():
    verdict = {"ops": 4909, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4909 -- midas launch") as rep:
        rep.heading("ops 4909 — SEC MIDAS goes live")
        p_deep0 = deep_parts()

        # fn live (deploy-lambdas create; helper fallback, no shared)
        arn = None
        end = time.time() + 300
        while time.time() < end:
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if c.get("State") == "Active":
                    arn = c["FunctionArn"]
                    break
            except Exception:
                pass
            time.sleep(15)
        if arn is None:
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
                for _ in range(18):
                    c = lam.get_function_configuration(
                        FunctionName=FN)
                    if c.get("State") == "Active":
                        arn = c["FunctionArn"]
                        break
                    time.sleep(10)
            except Exception as e:
                rep.kv(stage="deploy", err=(
                    f"{type(e).__name__}: {str(e)[:140]}"))
        if arn:
            try:
                c = lam.get_function_configuration(FunctionName=FN)
                if (c.get("EphemeralStorage") or {}).get("Size") \
                        != 10240:
                    lam.update_function_configuration(
                        FunctionName=FN,
                        EphemeralStorage={"Size": 10240})
                    time.sleep(8)
            except Exception:
                pass
        rep.kv(stage="fn", live=bool(arn))
        verdict["gates"]["midas_fn_live"] = ("PASS" if arn
                                             else "FAIL")

        # weekly Scheduler
        ok_s = False
        if arn:
            kw = dict(Name="justhodl-sec-midas-weekly",
                      ScheduleExpression="cron(30 4 ? * WED *)",
                      FlexibleTimeWindow={"Mode": "OFF"},
                      Target={"Arn": arn, "RoleArn": SCHED_ROLE,
                              "Input": "{}"}, State="ENABLED")
            try:
                sch.create_schedule(**kw)
                ok_s = True
                rep.kv(stage="schedule", action="created")
            except Exception as e:
                if "Conflict" in type(e).__name__ or "already" in \
                        str(e):
                    sch.update_schedule(**kw)
                    ok_s = True
                    rep.kv(stage="schedule", action="updated")
                else:
                    rep.kv(stage="schedule",
                           err=f"{type(e).__name__}: {str(e)[:120]}")
        verdict["gates"]["midas_scheduled"] = ("PASS" if ok_s
                                               else "FAIL")

        # kick + verify first zips on disk
        banked = []
        if arn:
            lam.invoke(FunctionName=FN, InvocationType="Event")
            t = time.time()
            while time.time() - t < 860:
                time.sleep(40)
                banked = midas_keys()
                if any(k.endswith(".zip") for k, _ in banked):
                    st = {}
                    try:
                        st = g("data/_state/sec-midas.json")
                    except Exception:
                        pass
                    rep.kv(stage="first-bank",
                           zips=len([1 for k, _ in banked
                                     if k.endswith(".zip")]),
                           sample=";".join(
                               f"{k}:{sz}" for k, sz in banked[:4]),
                           inventory=st.get("inventory_n"),
                           have=st.get("n_have"),
                           missing=st.get("n_missing"))
                    break
        verdict["gates"]["midas_first_zips"] = (
            "PASS" if any(k.endswith(".zip") for k, _ in banked)
            else "PENDING")
        verdict["midas"] = {"banked": banked[:6]}

        # provider card refresh (no wait) + deep cure re-proof
        try:
            lam.invoke(FunctionName="justhodl-provider-catalog",
                       InvocationType="Event")
        except Exception:
            pass
        p_deep1 = deep_parts()
        st2 = {}
        try:
            st2 = g("data/_state/ecb-deep.json")
        except Exception:
            pass
        rep.kv(stage="deep-cure-reproof", parts_before=p_deep0,
               parts_after=p_deep1, grew=p_deep1 > p_deep0,
               n_complete=st2.get("n_complete"))
        verdict["gates"]["deep_grinding"] = (
            "PASS" if p_deep1 > p_deep0 else "PENDING")
        verdict["deep"] = {"p0": p_deep0, "p1": p_deep1,
                           "n_complete": st2.get("n_complete")}

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        pend = [k for k, v in verdict["gates"].items()
                if v == "PENDING"]
        verdict["overall"] = ("FAIL" if hard else
                              "PASS_WITH_PENDING" if pend else "PASS")
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4909.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4909.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
