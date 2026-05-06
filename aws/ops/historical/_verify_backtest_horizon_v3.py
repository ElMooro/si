"""Force redeploy + invoke + verify avg weight is sensible + page renders."""
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
    with report("verify_backtest_horizon_v3") as r:
        # Wait
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # Force redeploy with avg weight fix
        r.heading("1) Force redeploy with avg weight fix")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        LAM.update_function_code(FunctionName="justhodl-backtest-engine", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # Invoke
        r.heading("2) Invoke with avg weight fix")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-backtest-engine", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  total_return_pct: {inner.get('total_return_pct')}%")
            r.log(f"  alpha_vs_spy_pct: {inner.get('alpha_vs_spy_pct')}%")
            r.log(f"  n_horizon_weighted: {inner.get('n_horizon_weighted')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # Now check signal-level avg weights are sensible
        r.heading("3) Signal-level avg weights + windows_used")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  Top contributors with horizon mix:")
            for s in (d.get("by_signal") or [])[:8]:
                wu = s.get("windows_used") or {}
                wu_str = ", ".join(f"{k}={v}" for k, v in sorted(wu.items()))
                r.log(f"    {s.get('signal_type'):28s}  avg_w={s.get('weight'):.3f}  n={s.get('n_outcomes'):4d}  ".ljust(80) + f"[{wu_str}]")
            r.log("")
            r.log(f"  Bottom contributors:")
            for s in (d.get("by_signal") or [])[-5:]:
                wu = s.get("windows_used") or {}
                wu_str = ", ".join(f"{k}={v}" for k, v in sorted(wu.items()))
                r.log(f"    {s.get('signal_type'):28s}  avg_w={s.get('weight'):.3f}  n={s.get('n_outcomes'):4d}  ".ljust(80) + f"[{wu_str}]")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Verify backtest.html
        r.heading("4) Verify backtest.html with horizon attribution renders")
        time.sleep(5)
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/backtest.html",
                headers={"User-Agent": "justhodl-audit/1.0"},
            )
            with urllib.request.urlopen(req, timeout=15) as h:
                html = h.read().decode("utf-8", errors="replace")
                r.log(f"  ✓ status={h.status}, size={len(html):,}b")
                checks = [
                    ("title", "Backtest" in html),
                    ("horizon section", 'id="horizon-attribution-section"' in html),
                    ("renderHorizonAttribution fn", "renderHorizonAttribution" in html),
                    ("nav active", 'class="tab active" href="/backtest.html"' in html),
                    ("loads results.json", "backtest/results.json" in html),
                ]
                for label, ok in checks:
                    r.log(f"    {'✓' if ok else '✗'} {label}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("5) Deep diff: 'plumbing_stress' before/after horizon awareness")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            ps = next((s for s in (d.get("by_signal") or []) if s.get("signal_type") == "plumbing_stress"), None)
            if ps:
                r.log(f"  plumbing_stress avg_w={ps.get('weight'):.3f}  n={ps.get('n_outcomes')}  win={ps.get('win_rate'):.0%}  total_contrib={ps.get('total_contribution'):+.3f}")
                r.log(f"    horizon mix: {ps.get('windows_used')}")
                r.log(f"  Before horizon-aware (flat w=0.99): contribution would have been roughly:")
                r.log(f"    n×avg_return × flat_weight × 0.005 (position size)")
            else:
                r.log("  plumbing_stress not in by_signal")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
