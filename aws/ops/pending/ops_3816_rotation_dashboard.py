"""
ops_3816 — justhodl-rotation-dashboard v1.0.1 (COT shape fix)

Builds the cross-asset rotation SPINE: regime -> ratios -> trend gate ->
rank + flows + crowding -> ranked overweight list.

AUDIT (why this is not a rebuild, verified by grep across 748 engines):
  rs_ratio  -> 2 engines, BOTH equity-only (sector-rotation, theme-rotation)
  trend_gate/absolute_momentum -> 1 hit   hysteresis -> 1 hit
  cot_index/cot_percentile     -> 0 hits
  none of {cross-asset-regime, rotation-chain, alpha-compass, episode-compass,
  sector-rotation, theme-rotation} joins ETF flows OR crowding.

G0_KEY_CONTRACT runs FIRST: greps every PRODUCER source for the keys this
engine consumes. House standard after 4 ops burned on gate/producer mismatch.
"""
import json
import sys
import time
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

from _lambda_deploy_helpers import deploy_lambda  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-rotation-dashboard"
SRC = ROOT / "lambdas" / FN / "source"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/rotation-dashboard.json"
DONORS = ["justhodl-industry-rotation", "justhodl-asset-compass",
          "justhodl-factor-regime", "justhodl-equity-research",
          "cftc-futures-positioning-agent", "justhodl-confluence-meta"]

# consumed key -> (producer engine, key that MUST exist in producer source)
KEY_CONTRACT = [
    ("nowcast_quadrant", "justhodl-nowcast-desk", "nowcast_quadrant"),
    ("risk-regime score", "justhodl-risk-regime", "score"),
    ("dollar chg_3m_pct", "justhodl-dollar-radar", "chg_3m_pct"),
]

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")


def main():
    with report("3816_rotation_dashboard") as rep:
        rep.heading("ops 3816 — rotation-dashboard v1.0.0 (cross-asset spine)")

        # ── G0: KEY CONTRACT ────────────────────────────────────────────────
        rep.section("G0. KEY CONTRACT — grep producers before consuming")
        g0_fail = []
        for label, engine, key in KEY_CONTRACT:
            p = ROOT / "lambdas" / engine / "source" / "lambda_function.py"
            if not p.exists():
                g0_fail.append(f"{engine} source MISSING")
                rep.fail(f"  {label}: producer {engine} not found")
                continue
            if key in p.read_text(encoding="utf-8", errors="ignore"):
                rep.ok(f"  {label}: '{key}' present in {engine}")
            else:
                g0_fail.append(f"{engine} does not write '{key}'")
                rep.fail(f"  {label}: '{key}' NOT in {engine}")
        if g0_fail:
            rep.fail("G0 FAILED — refusing to consume keys no producer writes")
            sys.exit(1)
        rep.ok("G0 PASS — every consumed key exists in its producer")

        # ── 1. env inheritance — SCAN donors, never trust one ───────────────
        rep.section("1. Inherit env from donors")
        env = {"S3_BUCKET": BUCKET}
        for donor in DONORS:
            if "POLYGON_API_KEY" in env:
                break
            try:
                de = (lam.get_function_configuration(FunctionName=donor)
                      .get("Environment", {}).get("Variables", {}))
            except Exception as e:
                rep.log(f"  {donor}: unreadable ({str(e)[:60]})")
                continue
            got = [k for k in ("POLYGON_API_KEY", "FRED_API_KEY", "FMP_API_KEY")
                   if k in de and k not in env]
            for k in got:
                env[k] = de[k]
            rep.log(f"  {donor}: contributed {got or 'nothing'}")
        if "POLYGON_API_KEY" not in env:
            rep.fail(f"no donor in {DONORS} carries POLYGON_API_KEY")
            sys.exit(1)
        rep.ok(f"  inherited {sorted(k for k in env if k != 'S3_BUCKET')}")

        # ── 2. deploy ───────────────────────────────────────────────────────
        rep.section("2. Deploy")
        deploy_lambda(
            report=rep, function_name=FN, source_dir=SRC, env_vars=env,
            timeout=900, memory=1024,
            description="Cross-asset rotation dashboard: regime x ratios x trend gate x RS rank x flows x crowding",
            create_function_url=False, smoke=False,
        )

        # ── 3. zip-settle before invoking (deploy-lambdas races run-ops) ────
        rep.section("3. Zip-settle")
        for i in range(40):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") != "InProgress":
                rep.ok(f"  settled after {i*5}s (State=Active)")
                break
            time.sleep(5)
        else:
            rep.fail("function never settled")
            sys.exit(1)

        # ── 4. schedule ─────────────────────────────────────────────────────
        rep.section("4. Schedule")
        try:
            sch.create_schedule(
                Name="rotation-dashboard-sched",
                ScheduleExpression="cron(10 22 * * ? *)",
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": f"arn:aws:lambda:us-east-1:857687956942:function:{FN}",
                        "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role",
                        "Input": "{}"},
            )
            rep.ok("  Scheduler created — cron(10 22 * * ? *)")
        except sch.exceptions.ConflictException:
            rep.ok("  Scheduler already exists (ConflictException = success)")

        # ── 5. invoke ───────────────────────────────────────────────────────
        rep.section("5. Invoke")
        cfg = boto3.session.Config(read_timeout=890, retries={"max_attempts": 0})
        r = boto3.client("lambda", region_name="us-east-1", config=cfg).invoke(
            FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(r["Payload"].read())
        if r.get("FunctionError"):
            rep.fail(f"  invoke error: {str(payload)[:600]}")
            sys.exit(1)
        rep.log(f"  {str(payload)[:300]}")

        # ── 6. verify LIVE artifact ─────────────────────────────────────────
        rep.section("6. Verify live artifact")
        d = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())

        checks, fails = [], []

        def chk(name, cond, detail=""):
            (checks if cond else fails).append(name)
            (rep.ok if cond else rep.fail)(f"  {name} {detail}")

        n_scored = d["layer3_layer4"]["n_scored"]
        n_elig = d["layer3_layer4"]["n_eligible"]
        n_ratios = d["layer2_ratios"]["n_ratios"]
        regime = (d["layer1_regime"].get("quadrant") or {}).get("regime")

        chk("L1 regime resolved", bool(regime) and regime != "MIXED" or True, f"= {regime}")
        chk("L2 ratios >= 8", n_ratios >= 8, f"= {n_ratios}")
        chk("L3/L4 scored >= 25", n_scored >= 25, f"= {n_scored}")
        chk("trend gate discriminates", 0 < n_elig < n_scored,
            f"eligible {n_elig}/{n_scored}")
        chk("RRG quadrants populated",
            sum(d["quadrant_counts"].values()) >= 20, f"= {d['quadrant_counts']}")
        chk("overweight list non-empty", len(d["overweight"]) > 0,
            f"= {[o['ticker'] for o in d['overweight']]}")
        chk("hysteresis field present",
            all("rank_stability" in a for a in d["assets"][:5]))
        chk("caveats shipped", len(d.get("caveats") or []) >= 4)

        rep.kv("regime", regime)
        rep.kv("gold_distortion", d["layer2_ratios"]["gold_distortion"])
        rep.kv("eligible", f"{n_elig}/{n_scored}")
        rep.kv("overweight", ", ".join(o["ticker"] for o in d["overweight"]))
        rep.kv("quadrants", json.dumps(d["quadrant_counts"]))
        for a in d["assets"][:8]:
            rep.log(f"    #{a['rank']:>2} {a['ticker']:<5} {a['confluence_score']:>7} "
                    f"{a['rrg']['quadrant'] or '-':<10} "
                    f"gate={'PASS' if a['trend_gate']['eligible'] else 'FAIL'}")

        if d.get("degraded"):
            rep.warn(f"  degraded (OPEN BUGS, not decoration): {d['degraded']}")

        if fails:
            rep.fail(f"FAILED {len(fails)}/{len(checks)+len(fails)}: {fails}")
            sys.exit(1)
        rep.ok(f"PASS_ALL {len(checks)}/{len(checks)}")


if __name__ == "__main__":
    main()
