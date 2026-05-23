"""ops 1087 — end-to-end test wealth-plan across realistic scenarios."""
import json, os, base64
from datetime import datetime, timezone
import boto3

FN = "justhodl-wealth-plan"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

SCENARIOS = [
    ("default_typical_american",
     {"age": "35", "retire_age": "65", "current_nav": "100000",
      "annual_savings": "24000", "annual_spending": "80000", "risk_profile": "moderate"}),
    ("disciplined_saver_moderate",
     {"age": "35", "retire_age": "65", "current_nav": "200000",
      "annual_savings": "50000", "annual_spending": "70000", "risk_profile": "moderate"}),
    ("lifecycle_45_yo",
     {"age": "45", "retire_age": "65", "current_nav": "400000",
      "annual_savings": "35000", "annual_spending": "85000", "risk_profile": "lifecycle"}),
    ("aggressive_young",
     {"age": "28", "retire_age": "60", "current_nav": "50000",
      "annual_savings": "30000", "annual_spending": "75000", "risk_profile": "aggressive"}),
    ("conservative_pre_retiree",
     {"age": "55", "retire_age": "65", "current_nav": "1200000",
      "annual_savings": "40000", "annual_spending": "90000", "risk_profile": "conservative"}),
]


def main():
    lam = boto3.client("lambda", region_name="us-east-1")
    out = {"started_at": datetime.now(timezone.utc).isoformat(), "scenarios": {}}

    for name, params in SCENARIOS:
        try:
            inv = lam.invoke(
                FunctionName=FN,
                InvocationType="RequestResponse",
                Payload=json.dumps({"queryStringParameters": params}).encode(),
            )
            body = json.loads(inv["Payload"].read())
            if "body" in body:
                payload = json.loads(body["body"])
                mc = payload.get("monte_carlo", {})
                v = payload.get("verdict", {})
                a = payload.get("allocation", {})
                td = payload.get("in_todays_dollars", {})
                opt = payload.get("savings_optimization", {})
                sens = payload.get("sensitivities", {})
                bench = payload.get("benchmarks", {})

                out["scenarios"][name] = {
                    "inputs": params,
                    "prob_success_pct": round(mc.get("prob_success", 0) * 100),
                    "terminal_p50": mc.get("terminal_nav_p50"),
                    "terminal_p10": mc.get("terminal_nav_p10"),
                    "terminal_p90": mc.get("terminal_nav_p90"),
                    "today_dollars_p50": td.get("p50_today_dollars"),
                    "n_bankrupt": mc.get("n_bankrupt"),
                    "verdict": v.get("status"),
                    "verdict_msg": v.get("message", "")[:250],
                    "portfolio_E_r": a.get("expected_return_pct"),
                    "portfolio_vol": a.get("volatility_pct"),
                    "real_E_r": a.get("real_expected_return_pct"),
                    "profile_label": a.get("profile_label"),
                    "required_savings": opt.get("required_annual_savings"),
                    "elapsed_s": payload.get("elapsed_seconds"),
                    "sensitivities": {
                        k: {"prob": round(s["prob_success"] * 100),
                            "delta": s["delta_pp_success"],
                            "terminal_p50": s["terminal_nav_p50"]}
                        for k, s in sens.items()
                    },
                    "benchmarks": {
                        k: {"prob": round(b["prob_success"] * 100),
                            "terminal_p50": b["terminal_nav_p50"]}
                        for k, b in bench.items()
                    },
                }
            else:
                out["scenarios"][name] = {"err": "no body in response", "raw": str(body)[:300]}
        except Exception as e:
            out["scenarios"][name] = {"err": str(e)[:300]}

    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    out_path = os.path.join(REPO_ROOT, "aws/ops/reports/1087.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, default=str)

    # Concise summary
    for name, s in out["scenarios"].items():
        if "err" in s:
            print(f"  {name}: ERR {s['err']}")
            continue
        print(f"\n=== {name} ===")
        print(f"  Inputs: {s['inputs']}")
        print(f"  Portfolio: {s['profile_label']}")
        print(f"  E[r]={s['portfolio_E_r']}%, vol={s['portfolio_vol']}%, real={s['real_E_r']}%")
        print(f"  Prob success: {s['prob_success_pct']}% — {s['verdict']}")
        print(f"  Terminal P10/P50/P90: ${s['terminal_p10']:,} / ${s['terminal_p50']:,} / ${s['terminal_p90']:,}")
        print(f"  In today's $ (P50): ${s['today_dollars_p50']:,}")
        print(f"  Bankrupt: {s['n_bankrupt']}/10000")
        print(f"  Required savings: ${s['required_savings']:,}/yr" if s.get('required_savings') else "  No optimization needed")
        print(f"  Verdict: {s['verdict_msg']}")


if __name__ == "__main__":
    main()
