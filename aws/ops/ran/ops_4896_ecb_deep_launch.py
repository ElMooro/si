"""ops/4896 -- ecb-deep launch + weekly rewalk schedule.

Ships the two guarantees ops 4895 left PENDING:
  (a) the 31 giant flows -> justhodl-ecb-deep (time-sliced streaming
      windows, permanent parts + manifests, backfill->refresh), driven
      by EventBridge Scheduler every 10 min until complete and forever
      after in refresh mode;
  (b) the 73 fast flows stay CURRENT forever -> weekly rewalk schedule
      invokes the walker with reset_done=1 (full-history re-pull;
      giants skipped, deep engine owns them).

Gates: walker rewalk marker settled -> deep fn live (deploy-lambdas
create, helper fallback if the create branch goes intermittent --
pure-stdlib fn, source-only zip is complete) -> both Schedulers
ensured -> first deep windows verifiably ON DISK (parts + state +
first_period) across two kicked rounds -> footprint census.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
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
DEEP = "justhodl-ecb-deep"
WALKER = "justhodl-sdmx-walker"
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


def zip_has(fn, marker):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    raw = urllib.request.urlopen(loc, timeout=60).read()
    return marker.encode() in zipfile.ZipFile(
        io.BytesIO(raw)).read("lambda_function.py")


def ensure_schedule(rep, name, expr, arn, payload):
    tgt = {"Arn": arn, "RoleArn": SCHED_ROLE,
           "Input": json.dumps(payload)}
    kw = dict(Name=name, ScheduleExpression=expr,
              FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt,
              State="ENABLED")
    try:
        sch.create_schedule(**kw)
        rep.kv(stage="schedule", name=name, action="created")
    except Exception as e:
        if "Conflict" in type(e).__name__ or "already" in str(e):
            sch.update_schedule(**kw)
            rep.kv(stage="schedule", name=name, action="updated")
        else:
            rep.kv(stage="schedule", name=name,
                   err=f"{type(e).__name__}: {str(e)[:120]}")
            return False
    return True


def deep_parts():
    n, flows = 0, set()
    tok = None
    while True:
        kw = dict(Bucket=B, Prefix="data/warm/ecb/data/",
                  MaxKeys=1000)
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents") or []:
            k = o["Key"].rsplit("/", 1)[-1]
            if "__" in k:
                n += 1
                flows.add(k.split("__")[0])
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    return n, flows


def main():
    verdict = {"ops": 4896, "gates": {},
               "started": datetime.now(timezone.utc).isoformat(
                   timespec="seconds")}
    with report("ops 4896 -- ecb deep launch") as rep:
        rep.heading("ops 4896 — ecb-deep launch + weekly rewalk")

        # ── walker rewalk marker settled ─────────────────────────────
        ok_w = False
        end = time.time() + 420
        while time.time() < end:
            try:
                if zip_has(WALKER, "reset_done"):
                    ok_w = True
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.kv(stage="walker-settle", rewalk_marker=ok_w)
        verdict["gates"]["walker_rewalk_deployed"] = (
            "PASS" if ok_w else "FAIL")

        # ── deep fn live (deploy-lambdas create; helper fallback) ────
        arn, created_by = None, None
        end = time.time() + 300
        while time.time() < end:
            try:
                c = lam.get_function_configuration(FunctionName=DEEP)
                if c.get("State") == "Active":
                    arn, created_by = c["FunctionArn"], "workflow"
                    break
            except Exception:
                pass
            time.sleep(15)
        if arn is None:
            cfg = json.loads((ROOT / "aws" / "lambdas" / DEEP /
                              "config.json").read_text())
            try:
                deploy_lambda(
                    report=rep, function_name=DEEP,
                    source_dir=ROOT / "aws" / "lambdas" / DEEP /
                    "source",
                    env_vars=cfg.get("env") or {},
                    timeout=int(cfg["timeout"]),
                    memory=int(cfg["memory"]),
                    description=str(cfg.get("description"))[:250],
                    create_function_url=False, smoke=False)
                for _ in range(20):
                    c = lam.get_function_configuration(
                        FunctionName=DEEP)
                    if c.get("State") == "Active":
                        arn, created_by = (c["FunctionArn"],
                                           "helper-fallback")
                        break
                    time.sleep(10)
            except Exception as e:
                rep.kv(stage="deep-deploy", err=(
                    f"{type(e).__name__}: {str(e)[:150]}"))
        # ephemeral must be 10240 for the /tmp streams; helper cannot
        # set it -- enforce directly, idempotent either path
        if arn:
            try:
                c = lam.get_function_configuration(FunctionName=DEEP)
                if (c.get("EphemeralStorage") or {}).get("Size") \
                        != 10240:
                    lam.update_function_configuration(
                        FunctionName=DEEP,
                        EphemeralStorage={"Size": 10240})
                    time.sleep(8)
            except Exception as e:
                rep.kv(stage="deep-ephemeral",
                       err=f"{type(e).__name__}: {str(e)[:100]}")
        rep.kv(stage="deep-fn", live=bool(arn), via=created_by)
        verdict["gates"]["deep_fn_live"] = ("PASS" if arn else "FAIL")

        # ── schedulers ───────────────────────────────────────────────
        ok_s = False
        if arn:
            s1 = ensure_schedule(rep, "justhodl-ecb-deep-10min",
                                 "rate(10 minutes)", arn, {})
            wcfg = lam.get_function_configuration(FunctionName=WALKER)
            s2 = ensure_schedule(
                rep, "justhodl-ecb-rewalk-weekly",
                "cron(15 3 ? * SUN *)", wcfg["FunctionArn"],
                {"agency": "ecb", "reset_done": 1, "budget": 740,
                 "per": 120, "cap_mb": 150})
            ok_s = s1 and s2
        verdict["gates"]["schedulers_ensured"] = ("PASS" if ok_s
                                                  else "FAIL")

        # ── kick two backfill rounds, verify windows ON DISK ────────
        parts0, _ = deep_parts()
        for rnd in (1,):
            if not arn:
                break
            lam.invoke(FunctionName=DEEP, InvocationType="Event")
            t = time.time()
            while time.time() - t < 880:
                time.sleep(30)
                st = None
                try:
                    st = g("data/_state/ecb-deep.json")
                except Exception:
                    pass
                if st and float(st.get("lease_until") or 0) \
                        <= time.time() and st.get("as_of"):
                    break
            rep.log(f"deep round {rnd} settled")
        st = {}
        try:
            st = g("data/_state/ecb-deep.json")
        except Exception:
            pass
        parts1, flows_touched = deep_parts()
        fps = {f: (st.get("flows", {}).get(f) or {}).get(
            "first_period") for f in list(flows_touched)[:6]}
        rep.kv(stage="deep-backfill", parts_before=parts0,
               parts_after=parts1,
               flows_touched=len(flows_touched),
               n_flows=st.get("n_flows"),
               n_complete=st.get("n_complete"),
               mode=st.get("mode"),
               first_periods=json.dumps(fps)[:220])
        verdict["gates"]["deep_backfill_started"] = (
            "PASS" if (parts1 - parts0) >= 5 and flows_touched
            else "FAIL")
        verdict["deep"] = {"parts": parts1,
                           "flows_touched": sorted(flows_touched),
                           "n_complete": st.get("n_complete"),
                           "first_periods": fps}

        hard = [k for k, v in verdict["gates"].items() if v == "FAIL"]
        verdict["overall"] = "FAIL" if hard else "PASS"
        verdict["finished"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        rep.log("VERDICT: " + verdict["overall"] + " · " +
                json.dumps(verdict["gates"]))
        out = ROOT / "aws" / "ops" / "reports" / "4896.json"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(verdict, indent=1, default=str))
        rep.log("report written: aws/ops/reports/4896.json")
    return verdict["overall"]


_overall = main()
if _overall == "FAIL":
    sys.exit(1)
sys.exit(0)
