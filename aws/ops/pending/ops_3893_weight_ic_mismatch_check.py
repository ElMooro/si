"""
ops_3893 — PROBE: complete (not truncated) read of the three most consequential
findings from ops 3892: factor-ic's full 8-factor + composite table (already
short enough to be complete, re-confirming), backtest-harness's COMPLETE rule
list with every PASS/FAIL (ops 3892 only showed 4 of what may be 8 rules,
truncated mid-JSON), and calibration-fleet's "summary" block (never actually
reached in the 3000-char truncation). Also cross-checks calibration-latest's
current_weights against factor-ic's measured IC directly, since ops 3892's
raw read suggested the weights may be almost inverted relative to what the
platform's own rigorous IC measurement says. Writes no code.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3893_weight_ic_mismatch_check") as rep:
        rep.heading("ops 3893 — complete factor-ic + backtest-harness + the weight-vs-IC cross-check")

        rep.section("1. factor-ic — full 8-factor table + composite (re-confirm, complete)")
        try:
            fic, fic_lm = get("data/factor-ic.json")
        except Exception as e:
            rep.fail(f"  factor-ic.json unreadable: {str(e)[:200]}")
            sys.exit(1)
        factor_ic = fic.get("factor_ic") or {}
        comp = fic.get("composite_alpha_ic") or {}
        rep.kv(maturity=fic.get("maturity"), panels_matured=fic.get("panels_matured"),
               universe_priced=fic.get("universe_priced"),
               composite_mean_ic=comp.get("mean_ic"), composite_t_stat=comp.get("t_stat"))
        for factor, d in factor_ic.items():
            rep.log(f"  {factor:<14} mean_ic={d.get('mean_ic')!s:<10} t_stat={d.get('t_stat')!s:<10} "
                    f"n_dates={d.get('n_dates')} quintile_spread={d.get('quintile_spread_avg')}")

        rep.section("2. calibration-latest — current model weights, direct comparison against factor-ic")
        try:
            cl, cl_lm = get("data/calibration-latest.json")
        except Exception as e:
            rep.fail(f"  calibration-latest.json unreadable: {str(e)[:200]}")
            sys.exit(1)
        weights = cl.get("current_weights") or {}
        rep.log("  factor          weight    measured_IC   t_stat    mismatch?")
        mismatches = []
        for factor, w in sorted(weights.items(), key=lambda x: -x[1]):
            ic_row = factor_ic.get(factor) or {}
            ic = ic_row.get("mean_ic")
            t = ic_row.get("t_stat")
            mismatch = (ic is not None and ic < -0.05 and t is not None and t < -3 and w >= 0.10)
            if mismatch:
                mismatches.append(factor)
            rep.log(f"  {factor:<14} {w:<9} {str(ic):<13} {str(t):<9} "
                    f"{'*** OVERWEIGHTED DESPITE NEGATIVE IC ***' if mismatch else ''}")
        rep.kv(n_factors_overweighted_despite_negative_ic=len(mismatches), which=str(mismatches))
        rep.log(f"  calibration-latest's OWN internal IC computation (separate from factor-ic.json): "
                f"{json.dumps(cl.get('information_coefficients'), default=str)[:600]}")

        rep.section("3. backtest-harness — COMPLETE rule list, every PASS/FAIL")
        try:
            bh, bh_lm = get("data/backtest-harness.json")
        except Exception as e:
            rep.fail(f"  backtest-harness.json unreadable: {str(e)[:200]}")
            sys.exit(1)
        rules = bh.get("rules") or []
        rep.kv(n_rules_total=len(rules), n_pass_field=bh.get("n_pass"),
               universe_n=bh.get("universe_n"), horizon_days=bh.get("horizon_days"))
        for r in rules:
            oos = r.get("oos") or {}
            rep.log(f"  {r.get('rule'):<22} family={r.get('family'):<20} "
                    f"n={oos.get('n')} sr={oos.get('sr')} hit={oos.get('hit')}% "
                    f"avg={oos.get('avg')}% maxdd={oos.get('maxdd')}% "
                    f"gate={r.get('deflated_gate_sr')} PASS={r.get('PASS')}")

        rep.section("4. calibration-fleet — the summary block (never reached in ops 3892's truncation)")
        try:
            cf, cf_lm = get("data/calibration-fleet.json")
        except Exception as e:
            rep.fail(f"  calibration-fleet.json unreadable: {str(e)[:200]}")
            sys.exit(1)
        rep.log(f"  summary: {json.dumps(cf.get('summary'), default=str)}")
        engines = cf.get("engines") or []
        graded = [e for e in engines if e.get("n_paired", 0) > 0]
        insufficient = [e for e in engines if e.get("quality_rating") == "INSUFFICIENT"]
        rep.kv(n_engines_total=len(engines), n_graded_with_real_data=len(graded),
               n_insufficient_data=len(insufficient))
        rep.log("  graded engines (n_paired>0), sorted by IC:")
        for e in sorted(graded, key=lambda x: x.get("ic_spearman") or 0):
            rep.log(f"    {e.get('name'):<20} n={e.get('n_paired'):<6} ic={e.get('ic_spearman')} "
                    f"hit={e.get('hit_rate')}% rating={e.get('quality_rating')}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
