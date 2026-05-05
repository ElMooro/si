"""Phase: ship multi-horizon calibration.

  1) Wire 'Horizons' tab into nav across pages
  2) Force redeploy of justhodl-calibrator
  3) Manually invoke calibrator to populate window_weights + recommended_horizon
  4) Verify horizons.html is live + calibration/latest.json has new fields
"""
import io
import json
import os
import re
import time
import zipfile
import urllib.request
import boto3
from ops_report import report

REGION = "us-east-1"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)

NAV_PAGES = [
    "today.html", "brief.html", "calls.html", "performance.html", "backtest.html",
    "weights.html", "accuracy.html", "sectors.html", "allocator.html", "vol.html",
    "news.html", "momentum.html", "research.html", "feedback.html",
    "13f.html", "ticker.html", "insiders.html", "signals.html",
    "read.html", "desk.html", "edge.html", "intelligence.html", "horizons.html",
]
SOURCE_DIR = "aws/lambdas/justhodl-calibrator/source"


def already_has(content):
    return bool(re.search(r'href="/?horizons\.html"', content, re.IGNORECASE))


def patch_modern(content, page):
    if already_has(content): return content, "already_has"
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/weights\.html">Weights</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m: return content, None
    is_self = page == "horizons.html"
    cls = 'tab active' if is_self else 'tab'
    insertion = f'\n    <a class="{cls}" href="/horizons.html">Horizons</a>'
    return content[:m.end()] + insertion + content[m.end():], "ok_modern"


def patch_topnav(content, page):
    if already_has(content): return content, "already_has"
    pat = re.compile(r'(<a\s+href="/?weights\.html">Weights</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m: return content, None
    return content[:m.end()] + '\n  <a href="/horizons.html">Horizons</a>' + content[m.end():], "ok_topnav"


def patch_emoji(content, page):
    if already_has(content): return content, "already_has"
    pat = re.compile(r'<a\s+href="/?weights\.html"\s+class="nav-link"[^>]*>[^<]*</a>', re.IGNORECASE)
    m = pat.search(content)
    if not m: return content, None
    return content[:m.end()] + '\n<a href="/horizons.html" class="nav-link">📐 Horizons</a>', "ok_emoji"


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
    with report("ship_horizons") as r:
        # 1) Nav wire
        r.heading("1) Wire Horizons tab into nav")
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

        # 2) Force redeploy calibrator
        r.heading("2) Force redeploy calibrator")
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-calibrator")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        LAM.update_function_code(FunctionName="justhodl-calibrator", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-calibrator")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # 3) Verify deployed source
        r.heading("3) Inspect deployed source for multi-horizon code")
        try:
            cresp = LAM.get_function(FunctionName="justhodl-calibrator")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=30) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("window_weights computation", "window_weights[stype][window]" in src),
                        ("recommended_horizon", "recommended_horizon[stype]" in src),
                        ("per-horizon SSM writes", 'f"{SSM_WEIGHTS_PATH}/{window}"' in src),
                        ("horizon_lifts in response", "horizon_lifts" in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4) Manually invoke calibrator (don't wait for Sunday)
        r.heading("4) Manually invoke calibrator to populate horizons")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  total_outcomes:    {inner.get('total_outcomes')}")
            r.log(f"  n_horizon_lift:    {inner.get('n_horizon_lift')}")
            lifts = inner.get("horizon_lifts") or []
            for h in lifts[:8]:
                r.log(f"    {h.get('signal'):28s}  flat={h.get('flat_weight'):.2f} → {h.get('best_horizon')}={h.get('horizon_weight'):.2f}  (Δ+{h.get('uplift'):.2f})")
        except Exception as e:
            r.log(f"  parse: {e}")

        # 5) Verify calibration/latest.json has new fields
        r.heading("5) Verify calibration/latest.json has window_weights + recommended_horizon")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d = json.loads(obj["Body"].read())
            ww = d.get("window_weights") or {}
            rh = d.get("recommended_horizon") or {}
            r.log(f"  window_weights:        {len(ww)} signals")
            r.log(f"  recommended_horizon:   {len(rh)} signals")
            r.log("")
            r.log("  Sample window_weights structure:")
            for sig, win_map in list(ww.items())[:5]:
                horizons_str = ", ".join([f"{h}={w:.2f}" for h, w in win_map.items()])
                r.log(f"    {sig:25s}  {horizons_str}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 6) Verify per-horizon SSM params
        r.heading("6) Verify per-horizon SSM params written")
        try:
            for win in ["day_7", "day_30", "day_60", "day_90"]:
                try:
                    p = SSM.get_parameter(Name=f"/justhodl/calibration/weights/{win}")
                    val = json.loads(p["Parameter"]["Value"])
                    r.log(f"  ✓ /justhodl/calibration/weights/{win}: {len(val)} signal types")
                except Exception as e:
                    r.log(f"  - /justhodl/calibration/weights/{win}: not written ({str(e)[:60]})")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 7) Verify horizons.html live
        r.heading("7) Verify horizons.html on production")
        time.sleep(3)
        try:
            req = urllib.request.Request("https://justhodl.ai/horizons.html",
                                         headers={"User-Agent": "justhodl-audit/1.0"})
            with urllib.request.urlopen(req, timeout=15) as h:
                body = h.read().decode("utf-8", errors="replace")
                r.log(f"  ✓ status={h.status}, size={len(body):,}b")
                checks = [
                    ("title", "<title>Horizons · JustHodl</title>" in body),
                    ("uplift list", 'id="uplift-list"' in body),
                    ("matrix table", 'id="horizon-matrix"' in body),
                    ("nav active", 'class="tab active" href="/horizons.html"' in body),
                    ("loads calibration", "calibration/latest.json" in body),
                    ("HORIZONS array", "day_7" in body and "day_30" in body and "day_60" in body and "day_90" in body),
                ]
                for label, ok in checks:
                    r.log(f"    {'✓' if ok else '✗'} {label}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
