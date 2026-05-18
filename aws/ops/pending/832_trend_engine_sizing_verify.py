"""ops/832 - re-deploy + verify the justhodl-trend-engine sizing fix.

ops 831 shipped and verified the Systematic Trend Desk but the verification
surfaced a real flaw: raw inverse-volatility sizing handed the near-cash
instruments (1-3y / 7-10y Treasuries) enormous notional weights - the live
book printed 364% gross and a -192% net Rates leg. That is the textbook
cash-vs-leverage degeneracy of unconstrained inverse-vol sizing, not a
trend position.

The engine now sizes inside a real managed-futures budget: sizing vol is
floored (VOL_FLOOR 6%) so a quiet instrument is not treated as free
leverage, gross notional is capped at 200% with the book scaled pro-rata
if it breaches, and no single instrument exceeds 30% of capital.

This op re-deploys the patched engine (self-healing if CI has not landed),
re-invokes it, and verifies the leverage budget actually binds in the live
output: every position within +/-30%, gross within 200%, the uncapped
figure surfaced, and the per-class book no longer carrying absurd legs -
plus a full re-run of the schema / discipline checks from ops 831.
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
MAX_POS = 30.0
MAX_GROSS = 200.0

report = {"ops": 832, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Re-verify justhodl-trend-engine leverage-budget fix"}

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

    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    payload = r["Payload"].read().decode()
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": payload[:400]}

    time.sleep(2)
    d = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())

    s = d.get("summary", {})
    pos = d.get("positions", [])
    by_class = d.get("by_class", [])
    active = [p for p in pos if p["direction"] != "FLAT"]

    weights = [p.get("target_weight_pct", 0) for p in pos]
    max_abs_pos = max((abs(w) for w in weights), default=0.0)
    gross = s.get("portfolio_gross_pct", 0)
    gross_uncapped = s.get("gross_uncapped_pct")
    gross_from_pos = round(sum(abs(w) for w in weights), 1)

    # the leverage budget must actually bind in the LIVE output
    leverage_checks = {
        "every_position_within_30pct": max_abs_pos <= MAX_POS + 0.05,
        "gross_within_200pct": gross <= MAX_GROSS + 0.5,
        "gross_uncapped_surfaced": isinstance(gross_uncapped, (int, float)),
        "final_gross_le_uncapped": (gross_uncapped is not None
                                    and gross <= gross_uncapped + 0.5),
        "leverage_capped_is_bool": isinstance(s.get("leverage_capped"), bool),
        "max_gross_pct_surfaced": s.get("max_gross_pct") == 200.0,
        "gross_reconciles": abs(gross_from_pos - gross) <= 1.0,
        # no single asset-class leg should be an absurd multiple of capital
        "no_absurd_class_leg": all(
            abs(c.get("net_weight_pct", 0)) <= 150.0 for c in by_class),
        # active book carries non-zero gross
        "active_book_has_gross": (gross > 0) if active else True,
    }

    # --- schema / discipline re-check (condensed from ops 831) -----------
    DIRS = {"LONG", "SHORT", "FLAT"}
    MATS = {"fresh", "developing", "extended"}
    DEADBAND = 0.35
    pos_well_formed = all(
        p.get("symbol") and p.get("direction") in DIRS
        and p.get("asset_class") in CLASSES
        and p.get("maturity") in MATS
        and all(isinstance(p.get(k), (int, float))
                for k in ("blended_tstat", "conviction", "mom_63d",
                          "mom_126d", "mom_252d", "annual_vol_pct",
                          "target_weight_pct"))
        for p in pos) if pos else False
    direction_consistent = all(
        (p["direction"] == "FLAT" and abs(p["blended_tstat"]) < DEADBAND
         and p["conviction"] == 0.0 and p["target_weight_pct"] == 0.0)
        or (p["direction"] == "LONG" and p["blended_tstat"] >= DEADBAND
            and p["target_weight_pct"] > 0)
        or (p["direction"] == "SHORT" and p["blended_tstat"] <= -DEADBAND
            and p["target_weight_pct"] < 0)
        for p in pos) if pos else False
    conv_sorted = all(pos[i]["conviction"] >= pos[i + 1]["conviction"]
                      for i in range(len(pos) - 1))
    # inverse-vol sense still holds AFTER the budget (lower-vol leg carries
    # at least as much gross as a materially higher-vol leg, unless both
    # are clamped at the 30% cap)
    inv_vol_ok = True
    if len(active) >= 2:
        srt = sorted(active, key=lambda p: p["annual_vol_pct"])
        lo, hi = srt[0], srt[-1]
        if hi["annual_vol_pct"] > lo["annual_vol_pct"] * 1.25:
            inv_vol_ok = (abs(lo["target_weight_pct"])
                          >= abs(hi["target_weight_pct"]) - 0.05)

    schema_checks = {
        "schema_present": d.get("schema_version") == "1.0",
        "universe_is_21": s.get("universe_count") == 21,
        "scored_sane": s.get("scored", 0) >= 15,
        "counts_consistent": (s.get("n_long", 0) + s.get("n_short", 0)
                              + s.get("n_flat", 0) == s.get("scored", 0)
                              and len(pos) == s.get("scored", 0)),
        "positions_well_formed": pos_well_formed,
        "direction_deadband_discipline": direction_consistent,
        "ranked_by_conviction": conv_sorted,
        "inverse_vol_sizing": inv_vol_ok,
        "regime_valid": s.get("regime") in {"BROAD TREND", "MIXED", "CHOP"},
        "headline_present": bool(d.get("headline")),
        "methodology_present": bool(d.get("methodology")),
    }

    all_checks = {**leverage_checks, **schema_checks}
    report["trend_engine"] = {
        "ok": all(all_checks.values()),
        "leverage_checks": leverage_checks,
        "schema_checks": schema_checks,
        "headline": d.get("headline"),
        "regime": s.get("regime"),
        "max_abs_position_pct": max_abs_pos,
        "portfolio_gross_pct": gross,
        "gross_uncapped_pct": gross_uncapped,
        "leverage_capped": s.get("leverage_capped"),
        "net_equity_tilt_pct": s.get("net_equity_tilt_pct"),
        "by_class": [
            {"class": c.get("asset_class"), "stance": c.get("stance"),
             "net": c.get("net_weight_pct"),
             "gross": c.get("gross_weight_pct")} for c in by_class],
        "top5": [
            {"sym": p["symbol"], "class": p["asset_class"],
             "dir": p["direction"], "conv": p["conviction"],
             "vol": p["annual_vol_pct"], "wt": p["target_weight_pct"]}
            for p in pos[:5]],
    }
    report["all_pass"] = (report["trend_engine"]["ok"]
                          and not report["invoke"]["fn_error"])
except Exception as e:
    import traceback
    report["error"] = f"{type(e).__name__}: {e}"
    report["trace"] = traceback.format_exc()[-1400:]
    report["all_pass"] = False

with open("aws/ops/reports/832_trend_engine_sizing_verify.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
