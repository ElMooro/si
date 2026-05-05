"""Deploy backtest engine v1.2 + invoke + verify realistic_summary lands in S3 + verify v1.1 vs v1.2 numbers."""
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
LAM = boto3.client("lambda", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)


def make_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, source_dir)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("verify_backtest_v12_realistic") as r:
        # Wait
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # Force redeploy
        r.heading("1) Force redeploy backtest-engine v1.2")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        LAM.update_function_code(FunctionName="justhodl-backtest-engine", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # Inspect deployed source for v1.2 markers
        r.heading("2) Verify v1.2 code in deployed Lambda")
        try:
            cresp = LAM.get_function(FunctionName="justhodl-backtest-engine")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=30) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("SLIPPAGE_BPS_PER_LEG = 5", "SLIPPAGE_BPS_PER_LEG = 5" in src),
                        ("CONCENTRATION_CAP = 0.40", "CONCENTRATION_CAP = 0.40" in src),
                        ("GROSS_EXPOSURE_CAP = 1.00", "GROSS_EXPOSURE_CAP = 1.00" in src),
                        ("realistic_results loop", "realistic_results = []" in src),
                        ("v1.2 method string", '"calibrated_alpha_replay_v3_horizon_aware_realistic"' in src),
                        ("realistic_summary in output", '"realistic_summary"' in src),
                        ("realistic_nav_curve in output", '"realistic_nav_curve"' in src),
                        ("v=1.2 marker", '"v": "1.2"' in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Invoke
        r.heading("3) Invoke backtest engine — full v1.1 + v1.2 pass")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-backtest-engine", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log("")
            r.log("  ── v1.1 IDEALIZED ──")
            r.log(f"    n_outcomes:           {inner.get('n_outcomes')}")
            r.log(f"    total_return_pct:     {inner.get('total_return_pct')}")
            r.log(f"    final_nav:            {inner.get('final_nav')}")
            r.log(f"    sharpe:               {inner.get('sharpe')}")
            r.log(f"    max_dd_pct:           {inner.get('max_dd_pct')}")
            r.log(f"    alpha_vs_spy_pct:     {inner.get('alpha_vs_spy_pct')}")
            r.log(f"    n_horizon_weighted:   {inner.get('n_horizon_weighted')}")
            r.log(f"    n_flat_weighted:      {inner.get('n_flat_weighted')}")
            r.log("")
            r.log("  ── v1.2 REALISTIC ──")
            r.log(f"    realistic_return_pct:        {inner.get('realistic_return_pct')}")
            r.log(f"    realistic_sharpe:            {inner.get('realistic_sharpe')}")
            r.log(f"    realistic_max_dd_pct:        {inner.get('realistic_max_dd_pct')}")
            r.log(f"    realistic_alpha_pct:         {inner.get('realistic_alpha_pct')}")
            r.log(f"    friction_drag_pct:           {inner.get('friction_drag_pct')}")
            r.log(f"    n_concentration_capped_days: {inner.get('n_concentration_capped_days')}")
            r.log(f"    n_gross_capped_days:         {inner.get('n_gross_capped_days')}")
        except Exception as e:
            r.log(f"  parse error: {e}, body[:600]: {body[:600]}")

        # Inspect S3 output
        r.heading("4) Verify S3 backtest/results.json has realistic_summary")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  v: {d.get('v')}")
            r.log(f"  method: {d.get('method')}")
            constants = d.get("constants") or {}
            r.log(f"  constants: {constants}")
            r.log("")
            real = d.get("realistic_summary") or {}
            ideal = d.get("summary") or {}
            r.log("  Side-by-side comparison:")
            r.log(f"    {'Metric':<28s} {'IDEALIZED v1.1':<18s} {'REALISTIC v1.2':<18s} {'Δ':<10s}")
            r.log(f"    {'─'*72}")
            metrics = [
                ("Total Return %",       ideal.get("total_return_pct"),    real.get("total_return_pct")),
                ("Final NAV $",          ideal.get("final_nav"),            real.get("final_nav")),
                ("Sharpe Proxy",         ideal.get("sharpe_proxy"),         real.get("sharpe_proxy")),
                ("Max Drawdown %",       ideal.get("max_drawdown_pct"),     real.get("max_drawdown_pct")),
                ("Alpha vs SPY %",       ideal.get("alpha_vs_spy_pct"),     real.get("alpha_vs_spy_pct")),
            ]
            for name, i, rv in metrics:
                if i is None or rv is None:
                    r.log(f"    {name:<28s} {str(i):<18s} {str(rv):<18s}")
                    continue
                delta = rv - i if isinstance(i, (int, float)) and isinstance(rv, (int, float)) else "—"
                r.log(f"    {name:<28s} {i!s:<18s} {rv!s:<18s} {delta if isinstance(delta,str) else f'{delta:+.4f}':<10s}")
            r.log("")
            r.log(f"  total_slippage_cost_pct:     {real.get('total_slippage_cost_pct')}")
            r.log(f"  n_concentration_capped_days: {real.get('n_concentration_capped_days')} of {real.get('n_total_days')} total")
            r.log(f"  n_gross_capped_days:         {real.get('n_gross_capped_days')} of {real.get('n_total_days')} total")
            r.log(f"  friction_drag_pct:           {real.get('friction_drag_pct')}")
            r.log(f"  n_trades realistic:          {real.get('n_trades')}")
            r.log("")
            r.log(f"  realistic_nav_curve length: {len(d.get('realistic_nav_curve') or [])}")
            r.log(f"  nav_curve length:           {len(d.get('nav_curve') or [])}")
            # Last 3 points of each curve
            ic = (d.get("nav_curve") or [])[-3:]
            rc = (d.get("realistic_nav_curve") or [])[-3:]
            r.log("")
            r.log("  Last 3 nav points:")
            for c in ic:
                r.log(f"    IDEAL  {c.get('date')} nav=${c.get('nav')} cum_pct={c.get('cum_pct')} spy_nav=${c.get('spy_nav')}")
            for c in rc:
                r.log(f"    REAL   {c.get('date')} nav=${c.get('nav')} cum_pct={c.get('cum_pct')} spy_nav=${c.get('spy_nav')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Verify summary.json has same shape
        r.heading("5) Verify backtest/summary.json (slim) has realistic_summary")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/summary.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  v:                     {d.get('v')}")
            r.log(f"  has realistic_summary: {'realistic_summary' in d}")
            r.log(f"  has constants:         {'constants' in d}")
            r.log(f"  size:                  {len(json.dumps(d)):,} chars")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("6) Realistic vs Idealized — interpretation")
        r.log("  v1.2 should show a meaningful drop in Sharpe and total return")
        r.log("  vs v1.1, primarily because gross-exposure-cap and concentration-")
        r.log("  cap force the system to scale back when 200+ signals fire on the")
        r.log("  same day. Slippage is a smaller drag (~10bps × deployed gross).")


if __name__ == "__main__":
    main()
