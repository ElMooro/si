"""Final verify after dedup + logged_at + 0.5% sizing — numbers should be sane now."""
import io
import json
import os
import time
import urllib.request
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-backtest-engine/source"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("verify_backtest_v3") as r:
        # Wait
        r.heading("0) Wait for any pending update")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  ready, mod={cfg.get('LastModified')}")
                break
            time.sleep(3)

        r.heading("1) Force redeploy")
        zb = make_zip()
        try:
            lam.update_function_code(FunctionName="justhodl-backtest-engine", ZipFile=zb)
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        for attempt in range(25):
            cfg = lam.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        r.heading("2) Re-invoke")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-backtest-engine", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  n_outcomes: {inner.get('n_outcomes')}")
            r.log(f"  total_return_pct: {inner.get('total_return_pct')}%")
            r.log(f"  final_nav: ${inner.get('final_nav')}")
            r.log(f"  max_dd_pct: {inner.get('max_dd_pct')}%")
            r.log(f"  sharpe: {inner.get('sharpe')}")
            r.log(f"  n_signals: {inner.get('n_signals')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        r.heading("3) Detailed v3 results")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary") or {}
            r.log(f"  Method: {d.get('method')}")
            r.log(f"  Window: {summ.get('first_date')} → {summ.get('last_date')} ({summ.get('n_days')} days)")
            r.log(f"  N unique trades: {summ.get('n_outcomes')} (after dedup)")
            r.log(f"  Win rate: {summ.get('win_rate')*100:.1f}% ({summ.get('n_correct')}/{summ.get('n_outcomes')})")
            r.log(f"  Final NAV: ${summ.get('final_nav')}  (return: {summ.get('total_return_pct'):+.4f}%)")
            r.log(f"  Max DD: {summ.get('max_drawdown_pct'):.2f}%")
            r.log(f"  Sharpe proxy: {summ.get('sharpe_proxy')}")
            r.log("")
            r.log(f"  Top 8 contributors:")
            for s in (d.get("by_signal") or [])[:8]:
                r.log(f"    {s['signal_type']:32s}  w={s['weight']:.3f}  n={s['n_outcomes']:>4}  win={s['win_rate']*100:>5.1f}%  contrib={s['total_contribution']:+.3f}%")
            r.log("")
            r.log(f"  Bottom 5:")
            for s in (d.get("by_signal") or [])[-5:]:
                r.log(f"    {s['signal_type']:32s}  w={s['weight']:.3f}  n={s['n_outcomes']:>4}  win={s['win_rate']*100:>5.1f}%  contrib={s['total_contribution']:+.3f}%")

            r.log("")
            r.log(f"  NAV curve (full):")
            curve = d.get("nav_curve") or []
            for n in curve[:5]:
                r.log(f"    {n['date']}: ${n['nav']:>9.0f}  daily={n['daily_pct']:+.3f}%  cum={n['cum_pct']:+.3f}%  (n={(d.get('daily') or [])[0]['n_outcomes'] if d.get('daily') else '?'})")
            if len(curve) > 10:
                r.log(f"    ... [{len(curve)-10} dates]")
            for n in curve[-5:]:
                r.log(f"    {n['date']}: ${n['nav']:>9.0f}  daily={n['daily_pct']:+.3f}%  cum={n['cum_pct']:+.3f}%")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
