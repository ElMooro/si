"""Wire Weights tab into nav + redeploy snapshotter with accuracy fix + reseed snapshot."""
import io
import json
import os
import re
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-calibration-snapshotter/source"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

PAGES = [
    "today.html", "brief.html", "performance.html", "accuracy.html",
    "sectors.html", "allocator.html", "vol.html", "news.html",
    "momentum.html", "research.html", "feedback.html",
    "13f.html", "ticker.html", "insiders.html", "signals.html",
    "read.html", "desk.html", "edge.html", "intelligence.html",
]


def already_has_weights(content):
    return bool(re.search(r'href="/?weights\.html"', content, re.IGNORECASE))


def patch_modern_tabs(content, page):
    if already_has_weights(content):
        return content, "already_has"
    pat = re.compile(r'(<a\s+class="tab(?:\s+active)?"\s+href="/performance\.html">Performance</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    is_self = page == "weights.html"
    cls = 'tab active' if is_self else 'tab'
    insertion = f'\n    <a class="{cls}" href="/weights.html">Weights</a>'
    return content[:m.end()] + insertion + content[m.end():], "ok_modern"


def patch_topnav(content, page):
    if already_has_weights(content):
        return content, "already_has"
    pat = re.compile(r'(<a\s+href="/?performance\.html">Performance</a>)', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    return content[:m.end()] + '\n  <a href="/weights.html">Weights</a>' + content[m.end():], "ok_topnav"


def patch_intelligence_emoji(content, page):
    if already_has_weights(content):
        return content, "already_has"
    pat = re.compile(r'<a\s+href="/?performance\.html"\s+class="nav-link"[^>]*>[^<]*</a>', re.IGNORECASE)
    m = pat.search(content)
    if not m:
        return content, None
    return content[:m.end()] + '\n<a href="/weights.html" class="nav-link">⚖️ Weights</a>', "ok_emoji"


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
    with report("wire_weights_and_redeploy") as r:
        # 1. Redeploy snapshotter with the accuracy-dict fix
        r.heading("1) Redeploy snapshotter with accuracy-dict fix")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        lam.update_function_code(FunctionName="justhodl-calibration-snapshotter", ZipFile=zb)
        for _ in range(15):
            cfg = lam.get_function(FunctionName="justhodl-calibration-snapshotter")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.ok(f"  ✓ deployed {cfg.get('LastModified')}")

        # 2. Reseed
        r.heading("2) Reseed first snapshot")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibration-snapshotter", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:500]}")

        # 3. Verify outputs
        r.heading("3) Verify snapshot outputs")
        for key in ["calibration/history-index.json", "calibration/latest.json"]:
            try:
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
                d = json.loads(obj["Body"].read())
                if key.endswith("history-index.json"):
                    r.log(f"  ✓ {key}: {d.get('n_snapshots')} snapshot(s)")
                    for s in d.get("snapshots", []):
                        r.log(f"    • {s.get('iso_week')} ({s.get('week_start')} → {s.get('week_end')})  n_weights={s.get('n_weights')}  n≥30={s.get('n_calibrated_n30')}")
                else:
                    summ = d.get("summary") or {}
                    r.log(f"  ✓ {key}")
                    r.log(f"    iso_week: {d.get('iso_week')}")
                    r.log(f"    n_weights: {summ.get('n_weights_total')}")
                    r.log(f"    n_calibrated_n30: {summ.get('n_signals_calibrated_n30')}")
                    r.log(f"    highest_weight: {summ.get('highest_weight')}")
                    r.log(f"    median_weight: {summ.get('median_weight')}")
                    r.log(f"    weighted_mean_accuracy: {summ.get('weighted_mean_accuracy')}")
                    r.log("")
                    r.log("    Top 8 weights with accuracy:")
                    weights = d.get("weights") or {}
                    accuracy = d.get("accuracy") or {}
                    counts = d.get("outcome_counts_60d") or {}
                    top = sorted(weights.items(), key=lambda x: -float(x[1]))[:8]
                    for sig, w in top:
                        acc = accuracy.get(sig)
                        n = counts.get(sig, 0)
                        acc_str = f"{acc*100:.1f}%" if acc is not None else "—"
                        r.log(f"      {sig:32s}  w={w:.3f}  acc={acc_str:>6s}  n_60d={n}")
            except Exception as e:
                r.log(f"  ✗ {key}: {e}")

        # 4. Wire Weights tab into nav
        r.heading("4) Wire Weights tab into nav")
        results = {}
        for page in PAGES + ["weights.html"]:
            if not os.path.exists(page):
                results[page] = "MISSING"
                continue
            with open(page) as f:
                content = f.read()
            patched = False
            for fn in [patch_modern_tabs, patch_intelligence_emoji, patch_topnav]:
                new, status = fn(content, page)
                if status:
                    if status.startswith("ok") and new != content:
                        with open(page, "w") as f:
                            f.write(new)
                        patched = True
                    results[page] = status
                    break
            if page not in results:
                results[page] = "no_match"

        ok = sum(1 for v in results.values() if v.startswith("ok"))
        r.log(f"  patched: {ok}")
        for p, s in sorted(results.items()):
            r.log(f"    {p:25s}  {s}")


if __name__ == "__main__":
    main()
