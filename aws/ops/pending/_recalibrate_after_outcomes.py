"""Re-invoke calibrator after fresh outcomes + diff calibration state."""
import json
import time
import boto3
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("recalibrate_after_outcomes") as r:
        # 1. Capture state BEFORE
        r.heading("1) Calibration state BEFORE")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d_before = json.loads(obj["Body"].read())
            r.log(f"  generated_at:        {d_before.get('generated_at')}")
            r.log(f"  total_outcomes:      {d_before.get('total_outcomes')}")
            r.log(f"  signal_types_tracked: {d_before.get('signal_types_tracked')}")
        except Exception as e:
            r.log(f"  ✗ {e}")
            d_before = {}

        # 2. Invoke calibrator
        r.heading("2) Invoke calibrator")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  total_outcomes:    {inner.get('total_outcomes')}")
            r.log(f"  n_horizon_lift:    {inner.get('n_horizon_lift')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # 3. Capture state AFTER
        r.heading("3) Calibration state AFTER + uplift diff")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d_after = json.loads(obj["Body"].read())
            r.log(f"  generated_at:        {d_after.get('generated_at')}")
            r.log(f"  total_outcomes:      {d_after.get('total_outcomes')}  ({d_after.get('total_outcomes', 0) - d_before.get('total_outcomes', 0):+d})")
            r.log(f"  signal_types_tracked: {d_after.get('signal_types_tracked')}")
            r.log("")

            # Compare flat weights
            w_before = d_before.get("weights") or {}
            w_after = d_after.get("weights") or {}
            ww_after = d_after.get("window_weights") or {}
            wa_after = d_after.get("window_accuracy") or {}

            r.log("  Flat weight changes (≥0.05 delta):")
            any_diff = False
            for sig in sorted(set(w_before) | set(w_after)):
                b = w_before.get(sig)
                a = w_after.get(sig)
                if a is None:
                    continue
                if b is None:
                    r.log(f"    🟢 NEW {sig:30s}  w={a:.3f}")
                    any_diff = True
                elif abs(a - b) >= 0.05:
                    arrow = "↑" if a > b else "↓"
                    r.log(f"    {arrow} {sig:30s}  {b:.3f} → {a:.3f}  (Δ{a-b:+.3f})")
                    any_diff = True
            if not any_diff:
                r.log("    (no signals shifted ≥0.05)")

            # Show new horizon weight measurements (n>=5 reached this run)
            r.log("")
            r.log("  Newly-measured (signal, horizon) pairs (n>=5):")
            for sig in sorted(ww_after):
                for win, w in (ww_after[sig] or {}).items():
                    n = (wa_after.get(sig) or {}).get(win, {}).get("n", 0)
                    if 5 <= n <= 7:  # Just crossed threshold
                        acc = (wa_after.get(sig) or {}).get(win, {}).get("accuracy", 0)
                        r.log(f"    {sig:30s}  {win}: w={w:.3f}, acc={acc*100:.0f}%, n={n}  ★ just crossed n=5")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Re-invoke backtest engine to use updated weights
        r.heading("4) Re-invoke backtest-engine with updated weights")
        t0 = time.time()
        try:
            resp = LAM.invoke(FunctionName="justhodl-backtest-engine", InvocationType="RequestResponse")
            body = resp["Payload"].read().decode()
            r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  n_outcomes:        {inner.get('n_outcomes')}")
            r.log(f"  total_return_pct:  {inner.get('total_return_pct')}%")
            r.log(f"  alpha_vs_spy_pct:  {inner.get('alpha_vs_spy_pct')}%")
            r.log(f"  n_horizon_weighted: {inner.get('n_horizon_weighted')}")
            r.log(f"  horizon_breakdown: {inner.get('horizon_breakdown')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
