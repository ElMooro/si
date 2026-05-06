"""Redeploy backtest engine + verify realistic numbers this time."""
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

UA = {"User-Agent": "justhodl-audit/1.0"}


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    try:
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
    with report("verify_backtest_v2") as r:
        # Wait
        r.heading("0) Wait for any pending update")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-backtest-engine")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  ready, mod={cfg.get('LastModified')}")
                break
            time.sleep(3)

        # Force redeploy
        r.heading("1) Force redeploy with 2% position sizing fix")
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

        # Verify deployed source has POSITION_SIZE
        r.heading("2) Inspect deployed source for POSITION_SIZE")
        try:
            cresp = lam.get_function(FunctionName="justhodl-backtest-engine")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=20) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("POSITION_SIZE = 0.02", "POSITION_SIZE = 0.02" in src),
                        ("POSITION_SIZE × w × sign × ret", "POSITION_SIZE * w * sign * ret" in src),
                        ("2pct_sizing label", "calibrated_alpha_replay_2pct_sizing" in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Re-invoke
        r.heading("3) Re-invoke with the fix")
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

        # Detailed top/bottom
        r.heading("4) Top 5 + bottom 5 contributors with realistic math")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary") or {}
            r.log(f"  Window: {summ.get('first_date')} → {summ.get('last_date')} ({summ.get('n_days')} days)")
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

            # Sample of NAV curve
            r.log("")
            r.log(f"  NAV curve sample:")
            for n in (d.get("nav_curve") or [])[:3]:
                r.log(f"    {n['date']}: NAV=${n['nav']}  daily={n['daily_pct']:+.3f}%  cum={n['cum_pct']:+.3f}%")
            r.log(f"    ...")
            for n in (d.get("nav_curve") or [])[-3:]:
                r.log(f"    {n['date']}: NAV=${n['nav']}  daily={n['daily_pct']:+.3f}%  cum={n['cum_pct']:+.3f}%")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Verify backtest.html
        r.heading("5) backtest.html live check")
        code, body = fetch("https://justhodl.ai/backtest.html")
        if code == 200:
            r.log(f"  ✓ {code}, {len(body):,}b")
            for label, check in [
                ("title", "<title>Backtest · JustHodl</title>" in body),
                ("nav active", 'class="tab active" href="/backtest.html"' in body),
                ("KPI row", 'id="kpi-row"' in body),
                ("NAV chart", 'id="nav-chart"' in body),
                ("contributors", 'id="top-contrib"' in body),
                ("signal table", 'id="signal-table"' in body),
                ("2% sizing in method", "0.02 × weight" in body),
            ]:
                r.log(f"    {'✓' if check else '✗'} {label}")
        else:
            r.log(f"  ✗ {code}: {body[:200]}")

        # Verify backtest tab on key pages
        r.heading("6) Backtest tab visible on key pages")
        for p in ["today.html", "brief.html", "calls.html", "performance.html", "weights.html"]:
            code, body = fetch(f"https://justhodl.ai/{p}")
            has = ('href="/backtest.html"' in (body or "")) or ('href="backtest.html"' in (body or ""))
            r.log(f"  {'✓' if has else '✗'} {p:25s}  Backtest link: {has}")


if __name__ == "__main__":
    main()
