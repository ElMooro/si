"""Redeploy calibrator with n>=5 threshold + verify spurious uplifts gone."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-calibrator/source"
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
    with report("redeploy_calibrator_n5") as r:
        r.heading("1) Wait + redeploy")
        for _ in range(20):
            cfg = LAM.get_function(FunctionName="justhodl-calibrator")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        zb = make_zip(SOURCE_DIR)
        LAM.update_function_code(FunctionName="justhodl-calibrator", ZipFile=zb)
        for _ in range(25):
            cfg = LAM.get_function(FunctionName="justhodl-calibrator")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        r.heading("2) Invoke calibrator")
        t0 = time.time()
        resp = LAM.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")

        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  total_outcomes: {inner.get('total_outcomes')}")
            r.log(f"  n_horizon_lift: {inner.get('n_horizon_lift')}")
            r.log("")
            r.log("  Genuine horizon-uplifts (n>=5 at best horizon):")
            for h in inner.get("horizon_lifts") or []:
                acc_pct = "—"
                # We have to pull the accuracy from S3 since response doesn't include it
                r.log(f"    {h.get('signal'):28s}  flat={h.get('flat_weight'):.2f} → {h.get('best_horizon')}: w={h.get('horizon_weight'):.2f}  Δ+{h.get('uplift'):.2f}")
        except Exception as e:
            r.log(f"  parse: {e}")

        r.heading("3) Inspect calibration JSON for clean uplifts")
        try:
            obj = S3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d = json.loads(obj["Body"].read())
            ww = d.get("window_weights") or {}
            rh = d.get("recommended_horizon") or {}
            wa = d.get("window_accuracy") or {}
            flat = d.get("weights") or {}

            n_with_horizons = sum(1 for s, m in ww.items() if m)
            r.log(f"  signals with measured horizons (n>=5): {n_with_horizons}/{len(ww)}")
            r.log("")

            # Compute uplifts strictly with full info
            uplifts = []
            for sig, rec in rh.items():
                fw = flat.get(sig, 0)
                hw = rec.get("weight", 0)
                d_v = hw - fw
                if d_v >= 0.15:
                    uplifts.append({
                        "sig": sig,
                        "win": rec.get("window"),
                        "fw": fw,
                        "hw": hw,
                        "d": d_v,
                        "acc": rec.get("accuracy"),
                        "n": rec.get("n"),
                    })
            uplifts.sort(key=lambda x: -x["d"])
            r.log(f"  Verified uplifts (≥0.15 + n>=5):")
            for u in uplifts:
                acc = f"{u['acc']*100:.0f}%" if u['acc'] is not None else "—"
                r.log(f"    {u['sig']:28s}  flat={u['fw']:.2f} → {u['win']}: w={u['hw']:.2f}  acc={acc} n={u['n']}  Δ+{u['d']:.2f}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
