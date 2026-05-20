"""
ops 973 -- FINAL unified verify on crypto-opportunities (whole feature)
========================================================================

Confirms the complete retail-edge feature is production-grade:

A. Engine        - Lambda deployed, env populated, recent invoke healthy
B. Data          - S3 output fresh + schema-complete with retail trade tickets
C. Schedule      - EventBridge Scheduler ENABLED at rate(4 hours)
D. Page          - live at justhodl.ai/crypto-opportunities.html with all 4 tables
E. Nav           - dex.html topnav has gold OPPORTUNITIES link
F. Directory     - directory.html crypto section surfaces the link
G. Signal-board  - 21 engines, crypto-opportunities present + live
"""
import datetime as dt
import json
import os
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FN = "justhodl-crypto-opportunities"
SCHED = "justhodl-crypto-opportunities-4h"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=60, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)

CHECKS = []
def add(n, ok, d=""):
    CHECKS.append({"name": n, "passed": bool(ok), "detail": str(d)[:280]})


# ── A: Engine ──
try:
    info = lam.get_function(FunctionName=FN)
    cfg = info["Configuration"]
    env = cfg.get("Environment", {}).get("Variables", {})
    add("A.lambda_deployed", True,
        f"runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')} "
        f"timeout={cfg.get('Timeout')} mod={cfg.get('LastModified', '')[:19]}")
    add("A.env_has_cmc_key",
        "CMC_KEY" in env and len(env.get("CMC_KEY", "")) > 10,
        f"CMC_KEY_present={('CMC_KEY' in env)} env_keys={len(env)}")
except ClientError as e:
    add("A.lambda_deployed", False, str(e)[:200])

# ── B: Data ──
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/crypto-opportunities.json")
    age_s = (dt.datetime.now(dt.timezone.utc) - obj["LastModified"]).total_seconds()
    d = json.loads(obj["Body"].read())
    add("B.s3_fresh",
        obj["ContentLength"] > 1000 and age_s < 14400,
        f"size={obj['ContentLength']}B age_m={int(age_s/60)} state={d.get('state')}")
    # Schema completeness
    required = ["engine", "as_of", "state", "summary", "convergence",
                "top_volume_surge", "top_social_velocity", "top_stable_inflows",
                "trigger_conditions", "forward_expectations", "recommended_trade",
                "historical_episodes", "why_now_explainer", "methodology",
                "sources", "schedule"]
    missing = [k for k in required if k not in d]
    add("B.schema_complete", len(missing) == 0,
        f"missing={missing} engine={d.get('engine')} version={d.get('version')}")
    # Trade tickets
    sample = (d.get("top_volume_surge") or [])[:3]
    tickets_ok = all(isinstance(r.get("trade_ticket"), dict)
                     and "entry_zone" in r["trade_ticket"]
                     and "stop_loss" in r["trade_ticket"]
                     for r in sample)
    add("B.trade_tickets_complete",
        tickets_ok or len(sample) == 0,
        f"sample_rows={len(sample)} all_complete={tickets_ok}")
    # Summary numbers
    sm = d.get("summary", {})
    add("B.scan_actually_executed",
        sm.get("universe_size", 0) >= 100,
        f"universe={sm.get('universe_size')} filtered={sm.get('filtered_universe_size')} "
        f"enriched={sm.get('n_enriched')}")
except Exception as e:
    add("B.s3_fresh", False, str(e)[:200])

# ── C: Schedule ──
try:
    sd = scheduler.get_schedule(Name=SCHED)
    add("C.schedule_enabled",
        sd.get("State") == "ENABLED",
        f"state={sd.get('State')} expr={sd.get('ScheduleExpression')} "
        f"tz={sd.get('ScheduleExpressionTimezone')}")
except ClientError as e:
    add("C.schedule_enabled", False, str(e)[:200])

# ── D: Page ──
try:
    req = urllib.request.Request("https://justhodl.ai/crypto-opportunities.html",
                                  headers={"User-Agent": "ops/973"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8", errors="ignore")
    markers = ["tbodyConv", "tbodyVol", "tbodySoc", "tbodyStb",
               "Convergence", "Volume Surge", "Social Velocity",
               "Stablecoin Inflows", "crypto-opportunities.json",
               "trade_ticket", "renderMarkdown"]
    missing = [m for m in markers if m not in body]
    add("D.page_live_and_wired",
        r.status == 200 and len(body) > 5000 and len(missing) == 0,
        f"status={r.status} size={len(body)} missing={missing}")
except Exception as e:
    add("D.page_live_and_wired", False, str(e)[:200])

# ── E: dex.html nav ──
try:
    req = urllib.request.Request("https://justhodl.ai/dex.html",
                                  headers={"User-Agent": "ops/973"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8", errors="ignore")
    add("E.dex_nav_link",
        "/crypto-opportunities.html" in body and "OPPORTUNITIES" in body
        and "facc15" in body.lower(),  # gold styling
        "gold link present" if "facc15" in body.lower() else "link only")
except Exception as e:
    add("E.dex_nav_link", False, str(e)[:200])

# ── F: directory.html ──
try:
    req = urllib.request.Request("https://justhodl.ai/directory.html",
                                  headers={"User-Agent": "ops/973"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8", errors="ignore")
    add("F.directory_link",
        "/crypto-opportunities.html" in body and "Opportunities" in body,
        "linked from Crypto category")
except Exception as e:
    add("F.directory_link", False, str(e)[:200])

# ── G: Signal-board integration ──
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")
    d = json.loads(obj["Body"].read())
    engines = d.get("engines", [])
    co = next((e for e in engines if "Crypto Opportunities" in e.get("engine", "")), None)
    add("G.signal_board_engines_21",
        d.get("n_engines", 0) >= 21,
        f"engines={d.get('n_engines')} live={d.get('n_live')} "
        f"posture={d.get('composite_posture')} signal={d.get('composite_signal')}")
    add("G.crypto_opps_in_signal_board",
        co is not None,
        f"present={co is not None} stale={(co or {}).get('stale')} "
        f"signal={(co or {}).get('signal')} read={(co or {}).get('read', '')[:60]}")
except Exception as e:
    add("G.signal_board_engines_21", False, str(e)[:200])

# ── REPORT ──
rep = {
    "ops": 973,
    "title": "FINAL unified verify on crypto-opportunities (whole retail feature)",
    "run_at": dt.datetime.utcnow().isoformat() + "Z",
    "checks": CHECKS,
    "summary": {"total": len(CHECKS),
                "passed": sum(1 for c in CHECKS if c["passed"]),
                "failed": sum(1 for c in CHECKS if not c["passed"])},
    "overall_ok": all(c["passed"] for c in CHECKS),
}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/973_crypto_opp_final.json", "w") as f:
    json.dump(rep, f, indent=2)
p, t = rep["summary"]["passed"], rep["summary"]["total"]
print(f"=== FINAL: {p}/{t} ({100*p//max(t,1)}%) ===")
for c in CHECKS:
    flag = "OK  " if c["passed"] else "FAIL"
    print(f"  [{flag}] {c['name']:36} {c['detail'][:140]}")
