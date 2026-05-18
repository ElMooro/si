"""ops/831 - deploy-guard + verify justhodl-trend-engine.

The Systematic Trend Desk is the platform's CTA / managed-futures sleeve -
rules-based time-series (absolute) momentum across a 21-instrument multi-
asset ETF universe, inverse-vol sized to a portfolio-vol target. It is a
DISTINCT strategy archetype from every existing engine: not single-name
equity (boom-board / best-ideas / risk-radar), not cross-sectional equity
momentum (momentum-breakout), not market-neutral equity (pairs-arb), not
discretionary macro (conviction-engine). Trend-following is the canonical
crisis diversifier - it maximises the decorrelation of the whole system.

deploy-lambdas.yml ships the engine and wires the EventBridge Scheduler
schedule from config.json. This op is self-healing: if the function or
schedule has not landed yet (CI race) it creates them, then invokes the
engine synchronously to seed data/trend-engine.json immediately (the
first scheduled run is otherwise 23:50 UTC tonight) and verifies the
output is REAL and SANE - the multi-asset universe scored on real FMP
daily closes, every position carrying a three-horizon momentum t-stat
breakdown, inverse-vol target weights, a maturity read and a deadband.
"""
import io
import json
import time
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
sched = boto3.client("scheduler", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-trend-engine"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
OUT_KEY = "data/trend-engine.json"
CLASSES = {"Equities", "Rates", "Credit", "Commodities", "FX", "Crypto"}

report = {"ops": 831, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy-guard + verify justhodl-trend-engine"}

# ---------------------------------------------------------------- package --
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())
zb = buf.getvalue()


def wait_ready():
    for _ in range(45):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and \
                c.get("State") == "Active":
            return
        time.sleep(2)


try:
    # -- function: get-or-create, ensure code + sizing current ------------
    exists = True
    try:
        lam.get_function(FunctionName=FN)
    except lam.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zb)
        wait_ready()
        lam.update_function_configuration(
            FunctionName=FN, MemorySize=CONF["memory"],
            Timeout=CONF["timeout"], Handler=CONF["handler"],
            Runtime=CONF["runtime"])
        wait_ready()
        report["deploy"] = "updated"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Code={"ZipFile": zb},
            MemorySize=CONF["memory"], Timeout=CONF["timeout"],
            Architectures=CONF.get("architectures", ["x86_64"]),
            Description=CONF.get("description", "")[:255])
        wait_ready()
        report["deploy"] = "created"

    # -- EventBridge Scheduler schedule -----------------------------------
    sc = CONF["eventbridge_scheduler"]
    target = {
        "Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
        "RoleArn": sc["role_arn"],
        "Input": "{}",
        "RetryPolicy": {"MaximumRetryAttempts": 2,
                        "MaximumEventAgeInSeconds": 3600},
    }
    sched_args = dict(
        Name=sc["schedule_name"],
        ScheduleExpression=sc["cron"],
        ScheduleExpressionTimezone=sc.get("timezone", "UTC"),
        FlexibleTimeWindow={"Mode": "OFF"},
        State="ENABLED",
        Description=sc.get("description", "")[:255],
        Target=target,
    )
    try:
        sched.get_schedule(Name=sc["schedule_name"])
        sched.update_schedule(**sched_args)
        report["schedule"] = "updated"
    except sched.exceptions.ResourceNotFoundException:
        sched.create_schedule(**sched_args)
        report["schedule"] = "created"
    st = sched.get_schedule(Name=sc["schedule_name"])
    report["schedule_state"] = st.get("State")
    report["schedule_cron"] = st.get("ScheduleExpression")

    # -- invoke synchronously to seed the feed ----------------------------
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    payload = r["Payload"].read().decode()
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload[:400]}

    # -- read + validate the output ---------------------------------------
    time.sleep(2)
    raw = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()
    d = json.loads(raw)

    s = d.get("summary", {})
    pos = d.get("positions", [])
    by_class = d.get("by_class", [])
    scored = s.get("scored", 0)
    n_long = s.get("n_long", 0)
    n_short = s.get("n_short", 0)
    n_flat = s.get("n_flat", 0)

    DIRS = {"LONG", "SHORT", "FLAT"}
    MATS = {"fresh", "developing", "extended"}

    # every position carries the full t-stat / sizing / maturity record
    pos_well_formed = all(
        p.get("symbol")
        and p.get("direction") in DIRS
        and p.get("asset_class") in CLASSES
        and isinstance(p.get("blended_tstat"), (int, float))
        and isinstance(p.get("conviction"), (int, float))
        and all(isinstance(p.get(k), (int, float))
                for k in ("mom_63d", "mom_126d", "mom_252d",
                          "annual_vol_pct", "target_weight_pct",
                          "maturity_sigma"))
        and p.get("maturity") in MATS
        and isinstance(p.get("notes"), list)
        for p in pos
    ) if pos else False

    # deadband + sign discipline: FLAT names carry zero conviction and zero
    # weight; LONG names a positive blended t-stat past the deadband, SHORT
    # a negative one
    DEADBAND = 0.35
    direction_consistent = all(
        (p["direction"] == "FLAT"
         and abs(p["blended_tstat"]) < DEADBAND
         and p["conviction"] == 0.0
         and p["target_weight_pct"] == 0.0)
        or (p["direction"] == "LONG"
            and p["blended_tstat"] >= DEADBAND
            and p["target_weight_pct"] > 0)
        or (p["direction"] == "SHORT"
            and p["blended_tstat"] <= -DEADBAND
            and p["target_weight_pct"] < 0)
        for p in pos
    ) if pos else False

    # positions ranked by conviction descending
    conv_sorted = all(
        pos[i]["conviction"] >= pos[i + 1]["conviction"]
        for i in range(len(pos) - 1)
    )

    # inverse-vol sanity: across active names, the lower-vol leg should
    # carry the larger gross weight (risk parity, not dollar parity)
    active = [p for p in pos if p["direction"] != "FLAT"]
    inv_vol_ok = True
    if len(active) >= 2:
        srt = sorted(active, key=lambda p: p["annual_vol_pct"])
        lo, hi = srt[0], srt[-1]
        if hi["annual_vol_pct"] > lo["annual_vol_pct"] * 1.25:
            inv_vol_ok = abs(lo["target_weight_pct"]) >= \
                abs(hi["target_weight_pct"])

    # by_class weights reconcile against the position book
    gross_from_pos = round(sum(abs(p["target_weight_pct"]) for p in pos), 1)
    gross_reported = s.get("portfolio_gross_pct", 0)
    gross_ok = abs(gross_from_pos - gross_reported) <= 1.0

    classes_valid = all(c.get("asset_class") in CLASSES for c in by_class)

    checks = {
        "schema_present": d.get("schema_version") == "1.0",
        "engine_tagged": d.get("engine") == FN,
        "headline_present": bool(d.get("headline")),
        "universe_is_21": s.get("universe_count") == 21,
        "scored_sane": scored >= 15,
        "counts_consistent": (n_long + n_short + n_flat == scored
                              and len(pos) == scored),
        "positions_well_formed": pos_well_formed,
        "direction_deadband_discipline": direction_consistent,
        "ranked_by_conviction": conv_sorted,
        "inverse_vol_sizing": inv_vol_ok,
        "gross_reconciles": gross_ok,
        "by_class_present": len(by_class) >= 1 and classes_valid,
        "regime_valid": s.get("regime") in
        {"BROAD TREND", "MIXED", "CHOP"},
        "methodology_present": bool(d.get("methodology")),
        "how_to_read_present": bool(d.get("how_to_read")),
    }

    report["trend_engine"] = {
        "ok": all(checks.values()),
        "checks": checks,
        "headline": d.get("headline"),
        "regime": s.get("regime"),
        "trend_breadth_pct": s.get("trend_breadth_pct"),
        "scored": scored,
        "n_long": n_long, "n_short": n_short, "n_flat": n_flat,
        "portfolio_gross_pct": gross_reported,
        "gross_recomputed": gross_from_pos,
        "net_equity_tilt_pct": s.get("net_equity_tilt_pct"),
        "by_class": [
            {"class": c.get("asset_class"), "stance": c.get("stance"),
             "net": c.get("net_weight_pct")} for c in by_class],
        "strongest_long": (
            {"sym": d["strongest_long"]["symbol"],
             "name": d["strongest_long"]["name"],
             "conv": d["strongest_long"]["conviction"],
             "wt": d["strongest_long"]["target_weight_pct"]}
            if d.get("strongest_long") else None),
        "strongest_short": (
            {"sym": d["strongest_short"]["symbol"],
             "name": d["strongest_short"]["name"],
             "conv": d["strongest_short"]["conviction"],
             "wt": d["strongest_short"]["target_weight_pct"]}
            if d.get("strongest_short") else None),
        "top5": [
            {"sym": p["symbol"], "class": p["asset_class"],
             "dir": p["direction"], "conv": p["conviction"],
             "blended_t": p["blended_tstat"],
             "wt": p["target_weight_pct"],
             "maturity": p["maturity"], "stress": p["stress_flag"]}
            for p in pos[:5]],
    }
    report["all_pass"] = (report["trend_engine"]["ok"]
                          and not report["invoke"]["fn_error"])
except Exception as e:
    import traceback
    report["error"] = f"{type(e).__name__}: {e}"
    report["trace"] = traceback.format_exc()[-1400:]
    report["all_pass"] = False

with open("aws/ops/reports/831_trend_engine_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
