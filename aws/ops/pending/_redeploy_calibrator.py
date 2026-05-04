"""Redeploy calibrator with SSM 4KB-cap fix + retrigger end-to-end."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-calibrator/source"

lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


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
    with report("redeploy_calibrator") as r:
        r.heading("1) Redeploy calibrator with slim-summary SSM fix")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        lam.update_function_code(FunctionName="justhodl-calibrator", ZipFile=zb)
        for _ in range(15):
            cfg = lam.get_function(FunctionName="justhodl-calibrator")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.ok(f"  ✓ deployed at {cfg.get('LastModified')}")

        r.heading("2) Re-invoke calibrator end-to-end")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            if "errorMessage" in outer:
                r.log(f"  ✗ ERROR: {outer['errorMessage']}")
            else:
                inner = json.loads(outer.get("body", "{}"))
                r.log(f"  total_outcomes: {inner.get('total_outcomes')}")
                weights = inner.get("weights_updated") or {}
                r.log(f"  n_weights: {len(weights)}")
                top = sorted(weights.items(), key=lambda x: -x[1])[:8]
                r.log(f"  Top 8 weights:")
                for sig, w in top:
                    r.log(f"    {sig:35s}  w={w:.3f}")
        except Exception as e:
            r.log(f"  parse: {e}")
            r.log(f"  body: {body[:600]}")

        r.heading("3) Verify slim summary now in SSM (was the failing 4KB write)")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/report")["Parameter"]
            r.log(f"  ✓ size: {len(v['Value'])} chars (well under 4096 limit)")
            d = json.loads(v["Value"])
            for k in ["generated_at", "total_outcomes", "signal_types_tracked",
                      "n_weights", "n_accuracy", "n_recommendations"]:
                r.log(f"    {k}: {d.get(k)}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("4) Confirm full report still at S3 calibration/latest.json")
        try:
            head = s3.head_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            r.log(f"  ✓ {head['ContentLength']:,}b modified {head['LastModified'].isoformat()}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("5) Snapshotter once more — capture the W19 final state")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibration-snapshotter", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  resp: {body[:300]}")

        # Pull final state
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
        d = json.loads(obj["Body"].read())
        summ = d.get("summary") or {}
        r.log(f"")
        r.log(f"  iso_week: {d.get('iso_week')}")
        r.log(f"  highest_weight: {summ.get('highest_weight')}")
        r.log(f"  median_weight: {summ.get('median_weight')}")
        r.log(f"  weighted_mean_accuracy: {summ.get('weighted_mean_accuracy')}")
        r.log(f"  n_signals_calibrated_n30: {summ.get('n_signals_calibrated_n30')}")


if __name__ == "__main__":
    main()
