"""Phase: ship justhodl-position-sizer-v2.

  1) Wire 'Sizing' nav across pages
  2) Create/update justhodl-position-sizer-v2 Lambda
  3) Add daily schedule
  4) Smoke invoke + verify portfolio/sizer-v2.json
  5) Verify sizing.html renders
"""
import io
import json
import os
import re
import time
import urllib.request
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-position-sizer-v2"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-position-sizer-v2/source"

LAM = boto3.client("lambda", region_name=REGION)
EVT = boto3.client("events", region_name=REGION)
S3 = boto3.client("s3", region_name=REGION)

NAV_PAGES = [
    "today.html", "brief.html", "calls.html", "performance.html",
    "backtest.html", "weights.html", "horizons.html", "accuracy.html",
    "sectors.html", "allocator.html", "vol.html", "news.html",
    "momentum.html", "research.html", "feedback.html",
    "13f.html", "ticker.html", "insiders.html", "signals.html",
    "read.html", "desk.html", "edge.html", "intelligence.html",
    "sizing.html",
]


def already_has(content):
    return bool(re.search(r'href="/?sizing\.html"', content, re.IGNORECASE))


def patch_modern(content, page):
    if already_has(content): return content, "already_has"
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/performance\.html">Performance</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m: return content, None
    is_self = page == "sizing.html"
    cls = 'tab active' if is_self else 'tab'
    insertion = f'\n    <a class="{cls}" href="/sizing.html">Sizing</a>'
    return content[:m.end()] + insertion + content[m.end():], "ok_modern"


def patch_topnav(content, page):
    if already_has(content): return content, "already_has"
    pat = re.compile(r'(<a\s+href="/?performance\.html">Performance</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m: return content, None
    return content[:m.end()] + '\n  <a href="/sizing.html">Sizing</a>' + content[m.end():], "ok_topnav"


def patch_emoji(content, page):
    if already_has(content): return content, "already_has"
    pat = re.compile(r'<a\s+href="/?performance\.html"\s+class="nav-link"[^>]*>[^<]*</a>', re.IGNORECASE)
    m = pat.search(content)
    if not m: return content, None
    return content[:m.end()] + '\n<a href="/sizing.html" class="nav-link">📐 Sizing</a>', "ok_emoji"


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
    with report("ship_position_sizer_v2") as r:
        # 1) Nav wire
        r.heading("1) Wire Sizing tab into nav")
        results = {}
        for page in NAV_PAGES:
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
        r.log(f"  patched: {ok}")
        for p, s in sorted(results.items()):
            r.log(f"    {p:25s}  {s}")

        # 2) Create / update Lambda
        r.heading("2) Create / update justhodl-position-sizer-v2")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        try:
            LAM.get_function(FunctionName=LAMBDA_NAME)
            LAM.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            for _ in range(20):
                cfg = LAM.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
                if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(2)
            LAM.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=60,
                MemorySize=256,
                Role=ROLE_ARN,
            )
            r.ok("  ✓ updated existing")
        except LAM.exceptions.ResourceNotFoundException:
            LAM.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=60,
                MemorySize=256,
                Architectures=["x86_64"],
                Description="Horizon-aware Kelly position sizer for paper portfolio + asymmetric setups",
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = LAM.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  state: Active mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # 3) EventBridge schedule — daily 14:00 UTC (after calibrator + risk-sizer)
        r.heading("3) EventBridge — daily 14:00 UTC")
        rule_name = f"{LAMBDA_NAME}-daily"
        EVT.put_rule(
            Name=rule_name,
            ScheduleExpression="cron(0 14 * * ? *)",
            State="ENABLED",
            Description="Compute horizon-aware Kelly recommendations for open positions + setups",
        )
        fn_arn = LAM.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        EVT.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            LAM.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
            )
        except LAM.exceptions.ResourceConflictException:
            pass
        r.ok(f"  ✓ {rule_name} → cron(0 14 * * ? *)")

        # 4) Smoke invoke
        r.heading("4) Smoke invoke")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  ok:                  {inner.get('ok')}")
            r.log(f"  n_positions:         {inner.get('n_positions')}")
            r.log(f"  n_setups:            {inner.get('n_setups')}")
            r.log(f"  decisive_call:       {inner.get('decisive_call')}")
            r.log(f"  risk_mult:           {inner.get('risk_mult')}")
            r.log(f"  current_exposure:    {inner.get('current_exposure_pct')}")
            r.log(f"  recommended_exposure: {inner.get('recommended_exposure_pct')}")
            r.log(f"  actions:             {inner.get('actions')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # 5) Inspect output JSON
        r.heading("5) Inspect portfolio/sizer-v2.json")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/sizer-v2.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  method:           {d.get('method')}")
            r.log(f"  positions:        {len(d.get('positions') or [])}")
            r.log(f"  setups:           {len(d.get('setups') or [])}")
            r.log("")
            r.log("  Sample position recommendations:")
            for p in (d.get("positions") or [])[:6]:
                r.log(f"    {p['ticker']:8s}  src={p.get('source','?')[:18]:18s}  "
                      f"hor={p['horizon']:8s} w={p['weight_used']:.2f} "
                      f"cur={p['current_pct']:.4f} → rec={p['call_adjusted_pct']:.4f} "
                      f"({p['recommended_action']})")
            r.log("")
            r.log("  Top setup recommendations:")
            for s in (d.get("setups") or [])[:5]:
                r.log(f"    {s['ticker']:8s}  comp={s.get('composite_score')}  "
                      f"hor={s['horizon']:8s} w={s['weight_used']:.2f}  "
                      f"flat_k={s['flat_kelly_pct']:.4f} → hor_k={s['horizon_kelly_pct']:.4f} "
                      f"final={s['call_adjusted_pct']:.4f}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 6) Verify sizing.html
        r.heading("6) Verify sizing.html on production")
        time.sleep(5)
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/sizing.html",
                headers={"User-Agent": "justhodl-audit/1.0"},
            )
            with urllib.request.urlopen(req, timeout=15) as h:
                html = h.read().decode("utf-8", errors="replace")
                r.log(f"  ✓ status={h.status}, size={len(html):,}b")
                checks = [
                    ("title", "Sizing — Horizon-Aware Kelly" in html or "Sizing · JustHodl" in html),
                    ("nav active", 'class="tab active" href="/sizing.html"' in html),
                    ("call banner", 'id="call-banner"' in html),
                    ("position table", 'id="pos-table"' in html),
                    ("setup table", 'id="setup-table"' in html),
                    ("loads sizer-v2.json", "portfolio/sizer-v2.json" in html),
                    ("renderPositions fn", "function renderPositions" in html),
                    ("auto-refresh", "setInterval(load, 5*60*1000)" in html),
                ]
                for label, ok in checks:
                    r.log(f"    {'✓' if ok else '✗'} {label}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
