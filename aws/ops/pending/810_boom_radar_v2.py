"""ops/810 — redeploy boom-radar v2 + verify the output is REALISTIC.

boom-radar v1 deployed and passed mechanical checks but printed garbage —
beat-streak 0 everywhere, 100%+ revenue growth, +400% targets. v2 fixes the
FMP field mappings (probe ops 808): TTM-smoothed growth, the `earnings`
endpoint for beats, conservative forward EPS, target hard-capped at 2.5x.

This ops does NOT just check that picks exist — it checks they are SANE:
no upside beyond the +150% cap, beat streaks actually populate, and TTM
revenue growth is not implausible across the board.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(read_timeout=120, connect_timeout=20, retries={"max_attempts": 3})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)

BUCKET = "justhodl-dashboard-live"
FN = "justhodl-boom-radar"
SRC = f"aws/lambdas/{FN}/source/lambda_function.py"

report = {"ops": 810, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "Redeploy boom-radar v2 + realism verification"}

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open(SRC, encoding="utf-8").read())

try:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    for _ in range(40):
        if lam.get_function_configuration(
                FunctionName=FN).get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    report["deploy"] = "updated"
except Exception as e:
    report["deploy"] = f"ERROR {type(e).__name__}: {str(e)[:200]}"

try:
    r = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    report["invoke"] = {"async_status": r.get("StatusCode")}
except Exception as e:
    report["invoke"] = {"error": str(e)[:200]}

br, fresh = {}, False
for _ in range(26):
    time.sleep(15)
    try:
        br = json.loads(s3.get_object(Bucket=BUCKET,
                        Key="data/boom-radar.json")["Body"].read())
        gen = br.get("generated_at", "")
        if gen >= report["ts"][:10] and br.get("schema_version") == "2.0":
            ga = datetime.fromisoformat(gen.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - ga).total_seconds() < 900:
                fresh = True
                break
    except Exception:
        pass

picks = br.get("picks") or []
with_target = [p for p in picks if p.get("price_target") is not None]
with_beats = [p for p in picks if (p.get("beat_streak") or 0) > 0]
ups = [p["upside_pct"] for p in picks if p.get("upside_pct") is not None]
revg = [p["rev_growth_ttm_pct"] for p in picks
        if p.get("rev_growth_ttm_pct") is not None]
max_up = max(ups) if ups else None
# implausible = TTM revenue growth above 200% (post-TTM-smoothing this
# should be rare; a flood of them means the data layer is still wrong)
implausible_rev = [p["symbol"] for p in picks
                   if (p.get("rev_growth_ttm_pct") or 0) > 200]

report["boom_radar"] = {
    "schema": br.get("schema_version"), "ok": br.get("ok"), "fresh": fresh,
    "headline": br.get("headline"),
    "n_scanned": br.get("n_scanned"), "n_qualified": br.get("n_qualified"),
    "n_prime": br.get("n_prime"), "n_strong": br.get("n_strong"),
    "n_with_target": len(with_target), "n_with_beat_streak": len(with_beats),
    "max_upside_pct": max_up,
    "median_rev_growth_ttm": (round(sorted(revg)[len(revg) // 2], 1)
                              if revg else None),
    "implausible_rev_count": len(implausible_rev),
    "implausible_rev_syms": implausible_rev[:8],
    "top8": [{"sym": p.get("symbol"), "score": p.get("boom_score"),
              "grade": p.get("grade"),
              "rev_ttm": p.get("rev_growth_ttm_pct"),
              "accel": p.get("rev_accel_pp"),
              "beats": p.get("beat_streak"),
              "pe": p.get("pe_ttm"), "peg": p.get("peg"),
              "target": p.get("price_target"),
              "upside": p.get("upside_pct"),
              "capped": p.get("target_capped")} for p in picks[:8]],
}

checks = {
    "deploy_ok": report.get("deploy") == "updated",
    "invoke_dispatched": report.get("invoke", {}).get("async_status")
    in (200, 202),
    "fresh_v2_output": fresh and br.get("schema_version") == "2.0",
    "output_ok": br.get("ok") is True,
    "picks_produced": len(picks) >= 5,
    "targets_present": len(with_target) >= 5,
    # ── realism gates ──
    "beat_streaks_populate": len(with_beats) >= 3,
    "no_absurd_upside": (max_up is not None and max_up <= 155),
    "revenue_growth_sane": len(implausible_rev) <= 2,
}
report["checks"] = checks
report["all_pass"] = all(checks.values())
report["verdict"] = (
    f"BOOM-RADAR v2 LIVE & REALISTIC — {br.get('n_qualified')} qualified "
    f"({br.get('n_prime')} PRIME), {len(with_beats)} with real beat "
    f"streaks, max upside {max_up}% (capped), median TTM rev growth "
    f"{report['boom_radar']['median_rev_growth_ttm']}%. Data layer fixed."
    if report["all_pass"]
    else "REVIEW — realism gate failed; see checks[]/boom_radar.top8")

print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/810_boom_radar_v2.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/810_boom_radar_v2.json")
