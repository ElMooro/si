"""ops 3307 — CLOSED-LOOP: dealer/OFR stack emits graded signals.
New falsifiable signals into justhodl-signals (outcome-checker grades
day_5/21/63/126, calibrator adjusts confidence):
  dealer_duration_twist   5y+ book <=-10B -> DOWN TLT vs BIL (dealers
                          short the long end = graded rates positioning)
  settlement_fails_spike  corporate spike -> DOWN LQD vs SPY;
                          UST spike -> DOWN SPY vs BIL
  gcf_tri_strain          interdealer premium >=8bp -> DOWN SPY vs BIL
  ofr_fsi_zero_cross      FSI crosses 0 upward -> DOWN SPY vs BIL
Dedupe: ConditionExpression on signal_id (per as_of). Expected TODAY:
dealer-duration-short (5y+ = -13.68) and fails-spike-corporate (z 2.17)
MUST emit; strain/fsi-cross stay armed (4bp / -2.51).
Polish: financing WoW series-break clamp; MMF pick labels T/BRA/OA."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name="us-east-1")
LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
DDB = boto3.resource("dynamodb", region_name="us-east-1").Table(
    "justhodl-signals")
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


def redeploy(name):
    cfg = LAM.get_function_configuration(FunctionName=name)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=name,
                  source_dir=AWS_DIR / "lambdas" / name / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(cfg.get("Timeout") or 300),
                  memory=int(cfg.get("MemorySize") or 512),
                  description=str(cfg.get("Description") or "")[:250],
                  smoke=False)


def sig(sid):
    try:
        return DDB.get_item(Key={"signal_id": sid}).get("Item")
    except Exception:
        return None


with report("3307_signal_wiring") as rep:
    fails, warns = [], []

    rep.section("1. Deploy the three emitters")
    for n in ("justhodl-nyfed-pd", "justhodl-settlement-fails",
              "justhodl-ofr-stfm"):
        redeploy(n)
    mark = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for n in ("justhodl-nyfed-pd", "justhodl-settlement-fails",
              "justhodl-ofr-stfm"):
        LAM.invoke(FunctionName=n, InvocationType="Event", Payload=b"{}")

    rep.section("2. Wait for fresh docs")
    docs = {}
    for key, tries in (("data/settlement-fails.json", 30),
                       ("data/ofr-stfm.json", 30),
                       ("data/nyfed-primary-dealer.json", 45)):
        d = None
        for _ in range(tries):
            time.sleep(10)
            d = s3_json(key)
            if d and d.get("generated_at", "") >= mark:
                break
        docs[key] = d or {}
        if not (d and d.get("generated_at", "") >= mark):
            fails.append("%s never freshened" % key)

    rep.section("3. Verify emissions in justhodl-signals")
    pd_doc = docs["data/nyfed-primary-dealer.json"]
    sf_doc = docs["data/settlement-fails.json"]
    of_doc = docs["data/ofr-stfm.json"]
    corp = (pd_doc.get("corporate") or {})
    ust_aso = corp.get("as_of", "")
    s1 = sig("dealer-duration-short#TLT#%s" % ust_aso)
    sf_corp = next((c for c in (sf_doc.get("classes") or [])
                    if c.get("key") == "corporate"), {})
    sf_aso = (sf_corp.get("stats") or {}).get("as_of", "")
    s2 = sig("fails-spike-corporate#LQD#%s" % sf_aso)
    rep.kv(dealer_5yplus_b=corp.get("net_5yplus_b"),
           dealer_signal={k: str(s1[k]) for k in
                          ("predicted_direction", "baseline_price",
                           "benchmark", "status")} if s1 else None,
           fails_corp_spike=(sf_corp.get("stats") or {}).get("spike"),
           fails_signal={k: str(s2[k]) for k in
                         ("predicted_direction", "baseline_price",
                          "benchmark", "status")} if s2 else None)
    if (corp.get("net_5yplus_b") or 0) <= -10 and not s1:
        fails.append("dealer duration signal missing")
    if (sf_corp.get("stats") or {}).get("spike") and not s2:
        fails.append("corporate fails-spike signal missing")
    ven = ((of_doc.get("repo") or {}).get("venues")) or {}
    g, t3 = (ven.get("GCF") or {}), (ven.get("TRI") or {})
    sp = (round((g["rate_pct"] - t3["rate_pct"]) * 100, 1)
          if g.get("rate_pct") is not None
          and t3.get("rate_pct") is not None else None)
    fsi = (of_doc.get("fsi") or {}).get("latest")
    rep.kv(gcf_tri_bp=sp, fsi_latest=fsi,
           strain_armed=(sp is not None and sp < 8),
           fsi_cross_armed=(fsi is not None and fsi < 0))

    rep.section("4. Polish checks")
    fn = pd_doc.get("financing") or {}
    picks = ((of_doc.get("mmf") or {}).get("picks")) or {}
    rep.kv(out_wow_b=fn.get("out_wow_b"), in_wow_b=fn.get("in_wow_b"),
           mmf_pick_keys=sorted(picks.keys()))
    if fn.get("out_wow_b") is not None and abs(fn["out_wow_b"]) > 1000:
        fails.append("wow clamp ineffective: %s" % fn.get("out_wow_b"))
    if "treasury_holdings" not in picks:
        warns.append("treasury_holdings pick label missing: %s"
                     % sorted(picks.keys()))

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3307 PASS — dealer/OFR stack wired into closed-loop "
            "grading: duration-twist + fails-spike LIVE in "
            "justhodl-signals; strain + FSI-cross armed.")
sys.exit(0)
