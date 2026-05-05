"""Redeploy position-sizer-v2 with field fix + verify positions correct."""
import io
import json
import os
import time
import urllib.request
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-position-sizer-v2/source"
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
    with report("redeploy_sizer_v2_fixed") as r:
        r.heading("1) Wait + redeploy")
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-position-sizer-v2")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        zb = make_zip(SOURCE_DIR)
        LAM.update_function_code(FunctionName="justhodl-position-sizer-v2", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-position-sizer-v2")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        r.heading("2) Invoke")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-position-sizer-v2", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  current_exposure_pct: {inner.get('current_exposure_pct')}")
            r.log(f"  recommended_exposure_pct: {inner.get('recommended_exposure_pct')}")
            r.log(f"  actions: {inner.get('actions')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        r.heading("3) Inspect positions with proper sizes")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/sizer-v2.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  All 11 positions with current vs recommended:")
            for p in (d.get("positions") or []):
                r.log(f"    {p['ticker']:8s}  src={p.get('source','?')[:18]:18s}  "
                      f"hor={p['horizon']:7s} w={p['weight_used']:.3f} "
                      f"cur={p['current_pct']*100:6.2f}% (${p['current_dollars']:,.0f}) "
                      f"→ rec={p['call_adjusted_pct']*100:5.2f}%  "
                      f"Δ={p['delta_pct']*100:+5.2f}pp  [{p['recommended_action']}]")
            r.log("")
            r.log(f"  Total current exposure: {d['summary']['total_current_exposure_pct']*100:.2f}%")
            r.log(f"  Total recommended:      {d['summary']['total_recommended_exposure_pct']*100:.2f}%")
            r.log(f"  Action distribution:    {d['summary']['actions']}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("4) Verify sizing.html live")
        time.sleep(8)
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/sizing.html",
                headers={"User-Agent": "justhodl-audit/1.0"},
            )
            with urllib.request.urlopen(req, timeout=15) as h:
                html = h.read().decode("utf-8", errors="replace")
                r.log(f"  ✓ status={h.status}, size={len(html):,}b")
                checks = [
                    ("title", "Sizing · JustHodl" in html),
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
