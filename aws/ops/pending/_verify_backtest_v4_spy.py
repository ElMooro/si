"""Verify backtest with SPY benchmark integrated."""
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


def fetch(url):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "audit/1.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except Exception as e:
        return None, str(e)


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
    with report("verify_backtest_v4_spy") as r:
        # Wait
        r.heading("0) Wait for any pending update")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  ready, mod={cfg.get('LastModified')}")
                break
            time.sleep(3)

        # Force redeploy
        r.heading("1) Force redeploy with SPY benchmark integration")
        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")
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

        # Deployed source check
        r.heading("2) Verify SPY code in deployed source")
        try:
            cresp = lam.get_function(FunctionName="justhodl-backtest-engine")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=20) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("fetch_spy_window", "def fetch_spy_window(" in src),
                        ("POLYGON_KEY", "POLYGON_KEY" in src),
                        ("alpha_vs_spy_pct field", "alpha_vs_spy_pct" in src),
                        ("spy_nav in nav curve", 'n["spy_nav"]' in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Re-invoke
        r.heading("3) Re-invoke + check SPY data flowed through")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-backtest-engine", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  Strategy return: {inner.get('total_return_pct'):+.2f}%")
            r.log(f"  SPY return:      {inner.get('spy_return_pct')}%")
            r.log(f"  Alpha vs SPY:    {inner.get('alpha_vs_spy_pct')}%")
            r.log(f"  Final NAV:       ${inner.get('final_nav')}")
            r.log(f"  Max DD:          {inner.get('max_dd_pct')}%")
            r.log(f"  Sharpe:          {inner.get('sharpe')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # Inspect S3 results
        r.heading("4) Inspect backtest/results.json — full summary + spy_nav in nav curve")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary") or {}
            r.log(f"  Window: {summ.get('first_date')} → {summ.get('last_date')}")
            r.log(f"  Strategy: ${summ.get('final_nav')}  ({summ.get('total_return_pct'):+.2f}%)")
            r.log(f"  SPY     : ${summ.get('spy_final_nav')}  ({summ.get('spy_return_pct'):+.2f}%)")
            r.log(f"  Alpha   : {summ.get('alpha_vs_spy_pct'):+.2f}%")
            r.log("")
            r.log(f"  Sample nav_curve entries (with SPY):")
            curve = d.get("nav_curve") or []
            for n in curve[:3]:
                r.log(f"    {n['date']}: strat=${n['nav']:>9.0f}  spy=${n.get('spy_nav', '—')}")
            for n in curve[-3:]:
                r.log(f"    {n['date']}: strat=${n['nav']:>9.0f}  spy=${n.get('spy_nav', '—')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Page check
        r.heading("5) backtest.html updates")
        code, body = fetch("https://justhodl.ai/backtest.html")
        if code == 200:
            r.log(f"  ✓ {code}, {len(body):,}b")
            for label, check in [
                ("5 KPI columns", "grid-template-columns:repeat(5,1fr)" in body),
                ("Alpha vs SPY KPI", "Alpha vs SPY" in body),
                ("SPY Buy & Hold", "SPY Buy & Hold" in body),
                ("hasSpy chart logic", "hasSpy" in body),
                ("legend rendering", "Strategy NAV" in body and "SPY Buy" in body),
            ]:
                r.log(f"    {'✓' if check else '✗'} {label}")
        else:
            r.log(f"  ✗ {code}: {body[:200]}")


if __name__ == "__main__":
    main()
