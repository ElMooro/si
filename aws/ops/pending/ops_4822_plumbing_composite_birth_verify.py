"""ops/4822 -- justhodl-plumbing-composite birth verify (Fusion 2).
 G0  FIELD-level: repo.json walkable >=700 series rows; for each core
     history id, data/repo-history/{id}.json values[-1] numeric and
     dates[-1] parseable (container length is never enough --
     boom_score doctrine).
 (1) function Active; zip marker settle 'plumbing-composite v1.0.0'.
 (2) EventBridge Scheduler justhodl-plumbing-composite-daily
     cron(45 10 * * ? *) UTC -- 20 min before risk-gate's 11:05 read.
 (3) Event-invoke; poll data/plumbing-composite.json fresh <=5 min.
 (4) truths: LIVE v1.0.0; fails/sofr_iorb/dispersion/scarcity/
     haircuts/fima all live (hard); periphery live OR stale-excluded
     with reason (OECD lag); sftr excluded with honest deferral;
     INDEPENDENT in-op recompute of sofr_iorb + scarcity stress_z
     from the LIVE history, of the haircut breadth share from the
     LIVE board, and of the FULL composite weighted mean (+SRF
     escalator) -- all must equal the engine; posture mapping; bank
     row appended; size sane.
 (5) readout: per-leg table + exclusions + composite/posture next to
     the current risk-gate funding score it will soon enrich.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
FN = "justhodl-plumbing-composite"
B = "justhodl-dashboard-live"
OUT_KEY = "data/plumbing-composite.json"
BANK_KEY = "plumbing-composite/history.json"
MARKER = "plumbing-composite v1.0.0"
SCHED_NAME = "justhodl-plumbing-composite-daily"
SCHED_CRON = "cron(45 10 * * ? *)"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"
CORE_IDS = ("D_SOFR_IORB", "D_SOFR_P75_P25", "D_DVP_SOFR",
            "D_BUND_EA_AAA", "D_BTP_BUND", "WREPOFOR",
            "SRF_TAKEUP", "DTCC-TREASURY-FAILS")
HARD_LEGS = ("fails", "sofr_iorb", "dispersion", "scarcity",
             "haircuts", "fima")
WEIGHTS = {"fails": .20, "sofr_iorb": .15, "dispersion": .15,
           "scarcity": .12, "periphery": .10, "haircuts": .10,
           "fima": .08}
WINDOW = {"daily": 756, "weekly": 156, "monthly": 120}

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
sched = boto3.client("scheduler", region_name=REGION)
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def ind_stress(sid, polarity, cad_window):
    """Independent recompute of a single-series stress_z from the
    LIVE history -- different code path than the engine."""
    h = sread("data/repo-history/%s.json" % sid)
    pairs = [(d, float(v)) for d, v in zip(h.get("dates") or [],
                                           h.get("values") or [])
             if isinstance(v, (int, float))]
    vals = [v for _, v in pairs]
    win = vals[-(cad_window + 1):]
    hist, last = win[:-1], win[-1]
    mu = sum(hist) / len(hist)
    sd = (sum((v - mu) ** 2 for v in hist)
          / (len(hist) - 1)) ** 0.5
    z = max(-4.0, min(4.0, (last - mu) / sd))
    return round(polarity * round(z, 3), 3)


def wait_active(rep):
    for _ in range(40):
        try:
            cfg = lam.get_function_configuration(FunctionName=FN)
        except ClientError:
            time.sleep(6)
            continue
        if (cfg.get("State") == "Active"
                and cfg.get("LastUpdateStatus") != "InProgress"):
            rep.kv(state="Active", mem=cfg.get("MemorySize"),
                   timeout=cfg.get("Timeout"))
            return True
        time.sleep(6)
    rep.fail("never Active")
    FAILED.append("active")
    return False


def settle(rep):
    for att in range(30):
        try:
            gf = lam.get_function(FunctionName=FN)
            raw = urllib.request.urlopen(gf["Code"]["Location"],
                                         timeout=60).read()
            src = zipfile.ZipFile(io.BytesIO(raw)).read(
                "lambda_function.py").decode("utf-8", "replace")
            if MARKER in src:
                rep.ok("marker settled (attempt %d)" % (att + 1))
                return True
        except (ClientError, Exception):  # noqa: BLE001
            pass
        time.sleep(10)
    rep.fail("zip never carried %s" % MARKER)
    FAILED.append("settle")
    return False


def ensure_schedule(rep):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN}"
    target = {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 2,
                              "MaximumEventAgeInSeconds": 3600}}
    try:
        ex = sched.get_schedule(Name=SCHED_NAME)
        if (ex.get("State") == "ENABLED"
                and ex.get("ScheduleExpression") == SCHED_CRON):
            rep.ok(f"schedule {SCHED_NAME} already correct")
            return
        sched.update_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target)
        rep.ok(f"schedule {SCHED_NAME} updated -> {SCHED_CRON}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    try:
        sched.create_schedule(Name=SCHED_NAME,
                              ScheduleExpression=SCHED_CRON,
                              ScheduleExpressionTimezone="UTC",
                              FlexibleTimeWindow={"Mode": "OFF"},
                              State="ENABLED", Target=target,
                              Description="justhodl-plumbing-"
                              "composite daily (ops 4822)")
        rep.ok(f"schedule {SCHED_NAME} created -> {SCHED_CRON}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok("schedule create raced -- ConflictException = ok")
        else:
            rep.fail(f"schedule create: {e}")
            FAILED.append("schedule")


def main():
    with report("ops 4822 -- plumbing composite birth verify") as rep:
        rep.heading("G0. FIELD-level feed contracts")
        try:
            board = sread("data/repo.json")
            rows = [r for g in (board.get("groups") or [])
                    for r in (g.get("series") or [])
                    if isinstance(r, dict) and r.get("id")]
        except ClientError:
            rep.fail("data/repo.json unreadable")
            sys.exit(1)
        if len(rows) >= 700:
            rep.ok("  G0 board rows = %d" % len(rows))
        else:
            rep.fail("  G0 board thin: %d" % len(rows))
            FAILED.append("g0_board")
        for sid in CORE_IDS:
            try:
                h = sread("data/repo-history/%s.json" % sid)
                v = (h.get("values") or [None])[-1]
                d = (h.get("dates") or [""])[-1]
                datetime.fromisoformat(str(d)[:10])
                ok = isinstance(v, (int, float))
            except (ClientError, ValueError, TypeError):
                ok = False
                v = d = None
            if ok:
                rep.ok("  G0 %-22s last=%s @ %s" % (sid, v, d))
            else:
                rep.fail("  G0 %-22s history/field broken" % sid)
                FAILED.append("g0_" + sid)
        if FAILED:
            rep.fail("G0 broken -- fix before invoke")
            sys.exit(1)

        rep.heading("1. function + settle")
        if not wait_active(rep) or not settle(rep):
            sys.exit(1)

        rep.heading("2. daily schedule (10:45 UTC, pre-risk-gate)")
        ensure_schedule(rep)

        rep.heading("3. Event-invoke + poll (<=5 min)")
        try:
            prev = sread(OUT_KEY).get("generated_at")
        except ClientError:
            prev = None
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 300:
            time.sleep(12)
            try:
                d = sread(OUT_KEY)
            except ClientError:
                continue
            if d.get("generated_at") != prev:
                doc = d
                break
        if not doc:
            rep.fail("no fresh %s within 5 min" % OUT_KEY)
            sys.exit(1)
        rep.ok("fresh doc in %ds  runtime_ms=%s"
               % (int(time.time() - t0),
                  (doc.get("diag") or {}).get("runtime_ms")))

        rep.heading("4. truths")
        legs = doc.get("legs") or {}
        excl = doc.get("excluded") or {}
        if doc.get("status") == "LIVE" and doc.get("v") == "1.0.0":
            rep.ok("  status LIVE v1.0.0  composite=%s posture=%s"
                   % (doc.get("composite"), doc.get("posture")))
        else:
            rep.fail("  status=%s why=%s" % (doc.get("status"),
                                             doc.get("why")))
            FAILED.append("status")
        for k in HARD_LEGS:
            if k in legs:
                rep.ok("  leg %-10s live stress_z=%+.2f"
                       % (k, legs[k]["stress_z"]))
            else:
                rep.fail("  leg %-10s MISSING (excluded: %s)"
                         % (k, excl.get(k)))
                FAILED.append("leg_" + k)
        if "periphery" in legs:
            rep.ok("  leg periphery  live stress_z=%+.2f "
                   "(age %sd)" % (legs["periphery"]["stress_z"],
                                  legs["periphery"]["series"][0]
                                  .get("age_days")))
        elif "stale" in str(excl.get("periphery")):
            rep.warn("  periphery honestly stale-excluded: %s"
                     % excl["periphery"])
        else:
            rep.fail("  periphery neither live nor stale-named: %s"
                     % excl.get("periphery"))
            FAILED.append("periphery")
        if "n=" in str(excl.get("sftr", "")) and "26" in \
                str(excl.get("sftr")):
            rep.ok("  sftr honestly deferred: %s" % excl["sftr"])
        else:
            rep.fail("  sftr deferral missing: %s" % excl.get("sftr"))
            FAILED.append("sftr")

        i_sofr = ind_stress("D_SOFR_IORB", +1, WINDOW["daily"])
        got = round((legs.get("sofr_iorb") or {}).get("stress_z",
                                                      9e9), 3)
        if abs(got - i_sofr) <= 5e-4:
            rep.ok("  sofr_iorb == independent recompute (%+.3f)"
                   % i_sofr)
        else:
            rep.fail("  sofr_iorb diverges: eng=%s ind=%s"
                     % (got, i_sofr))
            FAILED.append("ind_sofr")
        i_scar = ind_stress("D_BUND_EA_AAA", -1, WINDOW["daily"])
        got = round((legs.get("scarcity") or {}).get("stress_z",
                                                     9e9), 3)
        if abs(got - i_scar) <= 5e-4:
            rep.ok("  scarcity  == independent recompute (%+.3f, "
                   "polarity inverted)" % i_scar)
        else:
            rep.fail("  scarcity diverges: eng=%s ind=%s"
                     % (got, i_scar))
            FAILED.append("ind_scar")
        n_up = n_all = 0
        for r in rows:
            sid = str(r.get("id") or "")
            if not sid.startswith("HAIRCUT-") or "share" in sid \
                    or "federal-reserve" in sid:
                continue
            m = (r.get("chg") or {}).get("m")
            if isinstance(m, (int, float)):
                n_all += 1
                if m > 0:
                    n_up += 1
        hb = legs.get("haircuts") or {}
        if (hb.get("n_series") == n_all
                and hb.get("n_widening") == n_up):
            rep.ok("  haircut breadth == independent board count "
                   "(%d/%d widening, mode=%s)"
                   % (n_up, n_all, hb.get("mode")))
        else:
            rep.fail("  breadth diverges: eng=%s/%s ind=%s/%s"
                     % (hb.get("n_widening"), hb.get("n_series"),
                        n_up, n_all))
            FAILED.append("ind_breadth")
        live = {k: v for k, v in legs.items() if k in WEIGHTS}
        ws = sum(WEIGHTS[k] for k in live)
        esc = (doc.get("srf") or {}).get("escalator") or 0.0
        comp = round(sum(WEIGHTS[k] / ws * live[k]["stress_z"]
                         for k in live) + esc, 3)
        if doc.get("composite") == comp:
            rep.ok("  composite == full independent weighted "
                   "recompute (%+.3f incl SRF %+0.2f)" % (comp, esc))
        else:
            rep.fail("  composite diverges: eng=%s ind=%s"
                     % (doc.get("composite"), comp))
            FAILED.append("ind_comp")
        srf_bn = (doc.get("srf") or {}).get("takeup_bn")
        exp_esc = (0.25 if (srf_bn or 0) > 25
                   else 0.10 if (srf_bn or 0) > 2 else 0.0)
        if esc == exp_esc:
            rep.ok("  SRF escalator consistent (takeup=%s -> %+0.2f)"
                   % (srf_bn, esc))
        else:
            rep.fail("  SRF escalator wrong: takeup=%s esc=%s"
                     % (srf_bn, esc))
            FAILED.append("srf")
        try:
            bank = sread(BANK_KEY)
            if (bank.get("rows") or [])[-1].get("date") \
                    == doc.get("as_of"):
                rep.ok("  bank row appended for %s (n=%d)"
                       % (doc["as_of"], len(bank["rows"])))
            else:
                rep.fail("  bank row missing for today")
                FAILED.append("bank")
        except ClientError:
            rep.fail("  bank file missing")
            FAILED.append("bank")
        size = s3.head_object(Bucket=B, Key=OUT_KEY)["ContentLength"]
        if size < 200_000:
            rep.ok("  output size %.0f KB" % (size / 1024))
        else:
            rep.fail("  output bloated: %d" % size)
            FAILED.append("size")

        rep.heading("5. readout -- what risk-gate will inherit")
        for k in sorted(legs, key=lambda x: -abs(
                legs[x].get("stress_z", 0))):
            L = legs[k]
            sids = ",".join(x["id"] for x in (L.get("series")
                                              or [])) or "board"
            rep.log("  %-10s %+6.2f  [%s]" % (k, L["stress_z"],
                                              sids[:52]))
        for k, why in sorted(excl.items()):
            rep.log("  excluded %-10s %s" % (k, str(why)[:70]))
        try:
            rg = sread("data/risk-gate.json")
            rep.log("  risk-gate today: posture=%s funding=%s "
                    "sizing=%s  <- plumbing_adj wiring is the next "
                    "op" % (rg.get("posture"),
                            ((rg.get("legs") or {}).get("funding")
                             or {}).get("score"),
                            rg.get("sizing_multiplier")))
        except ClientError:
            rep.warn("  risk-gate.json unreadable")

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("justhodl-plumbing-composite LIVE -- the repo master "
               "board now reaches the decision layer as sized "
               "context, never selection")


if __name__ == "__main__":
    main()
