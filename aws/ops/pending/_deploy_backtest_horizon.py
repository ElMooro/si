"""Deploy horizon-aware backtest engine v3 + invoke + verify."""
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
    with report("deploy_backtest_horizon") as r:
        r.heading("1) Wait + redeploy backtest-engine")
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        LAM.update_function_code(FunctionName="justhodl-backtest-engine", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        r.heading("2) Inspect deployed source for horizon code")
        try:
            cresp = LAM.get_function(FunctionName="justhodl-backtest-engine")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=30) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("get_horizon_weights fn", "def get_horizon_weights(" in src),
                        ("resolve_weight fn", "def resolve_weight(" in src),
                        ("paginator usage", 'get_paginator("get_parameters_by_path")' in src),
                        ("horizon counters", "n_horizon_weighted" in src and "horizon_breakdown" in src),
                        ("v1.1 method tag", '"v": "1.1"' in src and "calibrated_alpha_replay_v3_horizon_aware" in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("3) Invoke backtest-engine — measure horizon attribution")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-backtest-engine", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"")
            r.log(f"  Headline metrics:")
            r.log(f"    n_outcomes:        {inner.get('n_outcomes')}")
            r.log(f"    total_return_pct:  {inner.get('total_return_pct')}%")
            r.log(f"    spy_return_pct:    {inner.get('spy_return_pct')}%")
            r.log(f"    alpha_vs_spy_pct:  {inner.get('alpha_vs_spy_pct')}%")
            r.log(f"    final_nav:         ${inner.get('final_nav'):,.0f}")
            r.log(f"    max_dd_pct:        {inner.get('max_dd_pct')}%")
            r.log(f"    sharpe:            {inner.get('sharpe')}")
            r.log(f"")
            r.log(f"  Horizon attribution:")
            r.log(f"    n_horizon_weighted: {inner.get('n_horizon_weighted')}")
            r.log(f"    n_flat_weighted:    {inner.get('n_flat_weighted')}")
            br = inner.get("horizon_breakdown") or {}
            for w, n in sorted(br.items()):
                r.log(f"    {w}: {n}")
        except Exception as e:
            r.log(f"  parse: {e}")

        r.heading("4) Inspect S3 backtest/results.json — top contributors")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary", {})
            r.log(f"  method: {d.get('method')}")
            r.log(f"  v: {d.get('v')}")
            r.log(f"  generated: {d.get('generated_at')}")
            r.log(f"  by_signal: {len(d.get('by_signal') or [])} signals")
            r.log(f"")
            r.log(f"  Top 8 contributors (post-horizon):")
            for s in (d.get("by_signal") or [])[:8]:
                r.log(f"    {s.get('signal_type'):28s}  w={s.get('weight'):.2f}  n={s.get('n_outcomes'):4d}  win={s.get('win_rate'):.0%}  total_contrib={s.get('total_contribution'):+.4f}")
            r.log(f"")
            r.log(f"  Bottom 5 contributors:")
            for s in (d.get("by_signal") or [])[-5:]:
                r.log(f"    {s.get('signal_type'):28s}  w={s.get('weight'):.2f}  n={s.get('n_outcomes'):4d}  win={s.get('win_rate'):.0%}  total_contrib={s.get('total_contribution'):+.4f}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("5) Verify backtest.html still loads + renders horizon attribution")
        time.sleep(3)
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/backtest.html",
                headers={"User-Agent": "justhodl-audit/1.0"},
            )
            with urllib.request.urlopen(req, timeout=15) as h:
                body = h.read().decode("utf-8", errors="replace")
                r.log(f"  ✓ status={h.status}, size={len(body):,}b")
                checks = [
                    ("title", "Backtest" in body),
                    ("horizon section", 'id="horizon-attribution-section"' in body),
                    ("renderHorizonAttribution", "renderHorizonAttribution" in body),
                    ("nav active", 'class="tab active" href="/backtest.html"' in body),
                ]
                for label, ok in checks:
                    r.log(f"    {'✓' if ok else '✗'} {label}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
