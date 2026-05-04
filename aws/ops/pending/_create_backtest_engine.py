"""Deploy justhodl-backtest-engine + 6h schedule + nav wiring + smoke test."""
import io
import json
import os
import re
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-backtest-engine"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-backtest-engine/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

PAGES = [
    "today.html", "brief.html", "calls.html", "performance.html",
    "weights.html", "accuracy.html", "sectors.html", "allocator.html",
    "vol.html", "news.html", "momentum.html", "research.html",
    "feedback.html", "13f.html", "ticker.html", "insiders.html",
    "signals.html", "read.html", "desk.html", "edge.html", "intelligence.html",
]


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


def already_has_backtest(content):
    return bool(re.search(r'href="/?backtest\.html"', content, re.IGNORECASE))


def patch_modern(content, page):
    if already_has_backtest(content):
        return content, "already_has"
    # Insert AFTER Performance tab
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/performance\.html">Performance</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    is_self = page == "backtest.html"
    cls = 'tab active' if is_self else 'tab'
    insertion = f'\n    <a class="{cls}" href="/backtest.html">Backtest</a>'
    return content[:m.end()] + insertion + content[m.end():], "ok_modern"


def patch_topnav(content, page):
    if already_has_backtest(content):
        return content, "already_has"
    pat = re.compile(r'(<a\s+href="/?performance\.html">Performance</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    return content[:m.end()] + '\n  <a href="/backtest.html">Backtest</a>' + content[m.end():], "ok_topnav"


def patch_emoji(content, page):
    if already_has_backtest(content):
        return content, "already_has"
    pat = re.compile(r'<a\s+href="/?performance\.html"\s+class="nav-link"[^>]*>[^<]*</a>', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    return content[:m.end()] + '\n<a href="/backtest.html" class="nav-link">📈 Backtest</a>', "ok_emoji"


def main():
    with report("create_backtest_engine") as r:
        # 1. Lambda
        r.heading("1) Create / update justhodl-backtest-engine")
        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")
        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            for _ in range(20):
                cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
                if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(2)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=120,
                MemorySize=512,
                Role=ROLE_ARN,
            )
            r.ok("  ✓ updated existing")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=120,
                MemorySize=512,
                Architectures=["x86_64"],
                Description="Calibrated alpha replay — backtests historical signals against current calibration weights",
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  state: Active mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # 2. EventBridge — every 6 hours
        r.heading("2) EventBridge — rate(6 hours)")
        rule_name = f"{LAMBDA_NAME}-6h"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(6 hours)",
            State="ENABLED",
            Description="Recompute calibrated alpha replay every 6h after calibrator runs",
        )
        fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok(f"  ✓ {rule_name} → rate(6 hours)")

        # 3. Smoke
        r.heading("3) Smoke invoke — first backtest run")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
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
            r.log(f"  duration_s: {inner.get('duration_s')}")
        except Exception as e:
            r.log(f"  parse: {e}")
            r.log(f"  body head: {body[:300]}")

        # 4. Verify outputs
        r.heading("4) Verify outputs")
        for key in ["backtest/results.json", "backtest/summary.json"]:
            try:
                head = s3.head_object(Bucket="justhodl-dashboard-live", Key=key)
                r.log(f"  ✓ {key}: {head['ContentLength']:,}b mod={head['LastModified'].isoformat()}")
            except Exception as e:
                r.log(f"  ✗ {key}: {e}")

        # 5. Pull a sample of the results
        r.heading("5) Top 5 contributors and bottom 5")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="backtest/results.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary") or {}
            r.log(f"  Window: {summ.get('first_date')} → {summ.get('last_date')} ({summ.get('n_days')} days)")
            r.log(f"  Win rate: {summ.get('win_rate')*100:.1f}% ({summ.get('n_correct')}/{summ.get('n_outcomes')})")
            r.log(f"  Final NAV: ${summ.get('final_nav')}  ({summ.get('total_return_pct'):+.2f}%)")
            r.log(f"  Max DD: {summ.get('max_drawdown_pct'):.2f}%")
            r.log(f"  Sharpe proxy: {summ.get('sharpe_proxy')}")
            r.log("")
            r.log(f"  Top 5 contributors:")
            for s in (d.get("by_signal") or [])[:5]:
                r.log(f"    {s['signal_type']:32s}  w={s['weight']:.3f}  n={s['n_outcomes']:>4}  win={s['win_rate']*100:>5.1f}%  contrib={s['total_contribution']:+.2f}%")
            r.log("")
            r.log(f"  Bottom 5:")
            for s in (d.get("by_signal") or [])[-5:]:
                r.log(f"    {s['signal_type']:32s}  w={s['weight']:.3f}  n={s['n_outcomes']:>4}  win={s['win_rate']*100:>5.1f}%  contrib={s['total_contribution']:+.2f}%")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 6. Wire nav
        r.heading("6) Wire Backtest tab into nav")
        results = {}
        for page in PAGES + ["backtest.html"]:
            if not os.path.exists(page):
                results[page] = "MISSING"
                continue
            with open(page) as f:
                content = f.read()
            for fn in [patch_modern, patch_emoji, patch_topnav]:
                new, status = fn(content, page)
                if status:
                    if status.startswith("ok") and new != content:
                        with open(page, "w") as f:
                            f.write(new)
                    results[page] = status
                    break
            if page not in results:
                results[page] = "no_match"
        ok = sum(1 for v in results.values() if v.startswith("ok"))
        r.log(f"  patched: {ok}/{len(results)}")
        for p, s in sorted(results.items()):
            r.log(f"    {p:25s}  {s}")


if __name__ == "__main__":
    main()
