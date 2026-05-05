"""Deploy ai-brief horizon-aware synthesis + invoke + verify horizon citations."""
import io
import json
import os
import time
import urllib.request
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-ai-brief/source"
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
    with report("verify_brief_horizon") as r:
        # Wait
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)

        # Force redeploy
        r.heading("1) Force redeploy ai-brief w/ horizon-aware prompt")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        LAM.update_function_code(FunctionName="justhodl-ai-brief", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # Inspect deployed source
        r.heading("2) Inspect deployed source for horizon-aware code")
        try:
            cresp = LAM.get_function(FunctionName="justhodl-ai-brief")
            url = cresp["Code"]["Location"]
            with urllib.request.urlopen(url, timeout=30) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("recommended_horizon read", "recommended_horizon = latest" in src),
                        ("best_horizon in row", '"best_horizon"' in src),
                        ("horizon_lifts compute", "horizon_lifts = []" in src),
                        ("horizons_tracked field", "horizons_tracked" in src),
                        ("HORIZON-AWARE WEIGHTING in prompt", "HORIZON-AWARE WEIGHTING" in src),
                        ("Match weight to call horizon", "Match weight to call horizon" in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Invoke
        r.heading("3) Invoke ai-brief with horizon-aware synthesis")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  brief_chars: {inner.get('brief_chars')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # Inspect snapshot for horizon fields
        r.heading("4) Verify snapshot has horizon fields")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            cv2 = (d.get("snapshot") or {}).get("calibration_v2") or {}
            r.log(f"  n_signals_with_horizon_data: {cv2.get('n_signals_with_horizon_data')}")
            r.log(f"  horizons_tracked: {cv2.get('horizons_tracked')}")
            r.log("")
            r.log("  Top 5 signals with best_horizon attribution:")
            for s in (cv2.get("top_weighted_signals") or [])[:5]:
                bh = s.get("best_horizon") or "—"
                bhw = s.get("best_horizon_weight") or "—"
                bha = s.get("best_horizon_accuracy") or "—"
                bhn = s.get("best_horizon_n") or "—"
                r.log(f"    {s.get('sig'):28s}  flat_w={s.get('weight')}  best={bh}: w={bhw} acc={bha} n={bhn}")
            r.log("")
            r.log("  horizon_lifts (mis-priced by flat lens):")
            for h in cv2.get("horizon_lifts") or []:
                r.log(f"    {h.get('sig'):28s}  flat={h.get('flat_weight')} → {h.get('best_horizon')}: w={h.get('horizon_weight')}  acc={h.get('horizon_accuracy')} n={h.get('horizon_n')}  Δ+{h.get('uplift')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Inspect brief markdown for horizon citations
        r.heading("5) Scan brief markdown for horizon citations")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.md")
            md = obj["Body"].read().decode("utf-8", errors="replace")
            r.log(f"  brief size: {len(md):,}b")
            # Search for horizon-aware citation patterns
            patterns = ["day_30", "day_14", "day_7", "day_3", "day_1", "horizon", "best_horizon", "at day"]
            r.log("  horizon-keyword hits:")
            for p in patterns:
                cnt = md.lower().count(p.lower())
                if cnt > 0:
                    r.log(f"    '{p}': {cnt} mentions")

            # Show the call section
            r.log("")
            r.log("  Last 1500 chars of brief (where DECISIVE CALL lives):")
            r.log("  " + "─" * 60)
            for line in md[-1800:].split("\n"):
                r.log(f"    {line[:160]}")
            r.log("  " + "─" * 60)
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
