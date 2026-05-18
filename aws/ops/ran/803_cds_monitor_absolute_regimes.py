"""ops/803 — deploy + verify the cds-monitor false-alarm fix.

The single-name credit regimes were peer-relative: a percentile ranking
that permanently branded the weakest ~20% of banks 'ELEVATED' and the next
~25% 'WATCH' even when every bank was healthy — a structural false alarm.
They are now ABSOLUTE bands on the market-anchored synthetic CDS. This
force-deploys the fix and verifies that, in the current calm credit market,
the G-SIB universe reads STRONG/SOLID/NORMAL — not a fixed stressed quintile.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=240, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
events = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-cds-monitor"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"
CONF = json.load(open(f"aws/lambdas/{FN}/config.json"))
ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"

report = {"ops": 803, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Deploy + verify cds-monitor absolute-regime fix"}

src_txt = open(SRC, encoding="utf-8").read()
report["source_has_absolute_regimes"] = (
    "assign_credit_regimes" in src_txt
    and "assign_peer_regimes" not in src_txt)

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", src_txt)
zip_bytes = buf.getvalue()

try:
    lam.get_function(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False

try:
    if exists:
        lam.update_function_code(FunctionName=FN, ZipFile=zip_bytes)
        for _ in range(40):
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.update_function_configuration(
            FunctionName=FN, Handler=CONF["handler"], Runtime=CONF["runtime"],
            Role=ROLE, Timeout=CONF["timeout"], MemorySize=CONF["memory"],
            Environment={"Variables": CONF.get("environment", {})},
            Description=CONF["description"][:255])
        report["deploy"] = "updated"
    else:
        lam.create_function(
            FunctionName=FN, Runtime=CONF["runtime"], Role=ROLE,
            Handler=CONF["handler"], Timeout=CONF["timeout"],
            MemorySize=CONF["memory"], Architectures=CONF["architectures"],
            Description=CONF["description"][:255],
            Environment={"Variables": CONF.get("environment", {})},
            Code={"ZipFile": zip_bytes})
        report["deploy"] = "created"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

for _ in range(40):
    try:
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") == "Successful"):
            break
    except Exception:
        pass
    time.sleep(3)

try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   Payload=b"{}")
    report["invoke"] = {"status": r.get("StatusCode"),
                        "fn_error": r.get("FunctionError"),
                        "body": json.loads(r["Payload"].read() or b"{}").get(
                            "body")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

time.sleep(3)

cm = {}
try:
    cm = json.loads(s3.get_object(Bucket=BUCKET,
                    Key="data/cds-monitor.json")["Body"].read())
except Exception as e:
    report["read_err"] = str(e)[:200]

sn = cm.get("single_name_cds") or {}
banks = sn.get("banks") or []
corps = sn.get("corporates") or []
STRESSED_REGIMES = {"WATCH", "ELEVATED", "STRESSED", "DISTRESSED"}

bank_regimes = [b.get("regime") for b in banks]
corp_regimes = [c.get("regime") for c in corps]
banks_watch_or_worse = sum(1 for g in bank_regimes if g in STRESSED_REGIMES)
corps_watch_or_worse = sum(1 for g in corp_regimes if g in STRESSED_REGIMES)

report["cds_monitor"] = {
    "ok": cm.get("ok"),
    "headline": cm.get("headline"),
    "primary_signal": sn.get("primary_signal"),
    "composite": (cm.get("global_credit_stress") or {}).get("score_0_100"),
    "regime": (cm.get("global_credit_stress") or {}).get("regime"),
    "alarm_board": (cm.get("alarm_board") or {}).get("status"),
    "n_alarms_active": (cm.get("alarm_board") or {}).get("n_active"),
    "alarms": [a.get("signal") for a in (cm.get("alarm_board") or {}).get(
        "alarms", [])],
    "bank_anchor_bp": (sn.get("calibration") or {}).get("bank_anchor_bp"),
    "n_banks": len(banks), "n_corporates": len(corps),
    "banks_watch_or_worse": banks_watch_or_worse,
    "corporates_watch_or_worse": corps_watch_or_worse,
    "bank_regime_breakdown": {g: bank_regimes.count(g)
                              for g in set(bank_regimes)},
    "banks": [{"t": b.get("ticker"), "dd": b.get("distance_to_default"),
               "cds_bp": b.get("synthetic_cds_bp"),
               "regime": b.get("regime")} for b in banks],
    "errors_n": len(cm.get("errors") or []),
}

# the fix works if, in the current calm market, the G-SIB universe is NOT
# a fixed stressed quintile — most banks should read STRONG/SOLID/NORMAL.
no_false_alarm = banks_watch_or_worse <= 3

checks = {
    "source_has_absolute_regimes": report["source_has_absolute_regimes"],
    "deploy_ok": str(report.get("deploy", "")).startswith(("created",
                                                           "updated")),
    "function_active": False,
    "invoke_ok": report.get("invoke", {}).get("status") == 200
                 and not report.get("invoke", {}).get("fn_error"),
    "output_ok": cm.get("ok") is True,
    "banks_priced": len(banks) >= 8,
    "corporates_priced": len(corps) >= 8,
    "no_false_alarm_quintile": no_false_alarm,
    "absolute_regime_label": sn.get("primary_signal")
    == "absolute_market_anchored_credit_regime",
}
try:
    checks["function_active"] = lam.get_function_configuration(
        FunctionName=FN)["State"] == "Active"
except Exception:
    pass
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"CDS-MONITOR FALSE-ALARM FIX LIVE — single-name regimes are now "
    f"absolute, cycle-aware bands. In the current market {banks_watch_or_worse}"
    f"/{len(banks)} G-SIB banks read WATCH-or-worse (was a permanent ~5/12 "
    f"under peer-relative grading); composite "
    f"{(cm.get('global_credit_stress') or {}).get('score_0_100')}/100, alarm "
    f"board {(cm.get('alarm_board') or {}).get('status')}. Bank regimes: "
    f"{report['cds_monitor']['bank_regime_breakdown']}."
    if report["all_pass"]
    else "REVIEW — see checks[]/cds_monitor (no_false_alarm_quintile gates "
         "on <=3 banks WATCH-or-worse in a calm market)")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/803_cds_monitor_absolute_regimes.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/803_cds_monitor_absolute_regimes.json")
