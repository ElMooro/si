"""Auto-deploy already updated the calibrator code; just need to invoke + verify."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("invoke_calibrator_clean") as r:
        # Wait for any in-progress update to settle
        r.heading("0) Wait for Lambda to stabilize")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-calibrator")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ ready, last_modified={cfg.get('LastModified')}")
                break
            r.log(f"  attempt {attempt}: state={cfg['State']}, lastUpdate={cfg.get('LastUpdateStatus')}")
            time.sleep(3)

        r.heading("1) Invoke calibrator clean (with SSM-summary fix)")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            if "errorMessage" in outer:
                r.log(f"  ✗ errorMessage: {outer['errorMessage'][:300]}")
            else:
                inner = json.loads(outer.get("body", "{}"))
                r.log(f"  total_outcomes: {inner.get('total_outcomes')}")
                weights = inner.get("weights_updated") or {}
                r.log(f"  n_weights: {len(weights)}")
                top = sorted(weights.items(), key=lambda x: -x[1])[:8]
                r.log(f"")
                r.log(f"  Top 8 weights AFTER backfill + recalibration:")
                for sig, w in top:
                    star = "  ★" if sig == "screener_top_pick" else "   "
                    r.log(f"    {star} {sig:35s}  w={w:.3f}")
        except Exception as e:
            r.log(f"  parse: {e}")
            r.log(f"  body head: {body[:600]}")

        r.heading("2) Verify slim summary now in SSM (was 4KB-failing before)")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/report")["Parameter"]
            r.log(f"  ✓ size: {len(v['Value'])} chars (was crashing >4096)")
            d = json.loads(v["Value"])
            for k, val in d.items():
                r.log(f"    {k:25s} = {str(val)[:80]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("3) Re-snapshot — capture final W19 state into weights ledger")
        resp = lam.invoke(FunctionName="justhodl-calibration-snapshotter", InvocationType="RequestResponse")
        r.log(f"  resp: {resp['Payload'].read().decode()[:400]}")

        # Pull final
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
        d = json.loads(obj["Body"].read())
        weights = d.get("weights") or {}
        accuracy = d.get("accuracy") or {}
        meta = d.get("accuracy_meta") or {}
        counts = d.get("outcome_counts_60d") or {}
        summ = d.get("summary") or {}

        r.heading("4) Final state — top 12 weights in 2026-W19 snapshot")
        r.log(f"  iso_week: {d.get('iso_week')}")
        r.log(f"  highest_weight: {summ.get('highest_weight')}")
        r.log(f"  median_weight: {summ.get('median_weight')}")
        r.log(f"  weighted_mean_accuracy: {summ.get('weighted_mean_accuracy')}")
        r.log(f"  n_calibrated_n30: {summ.get('n_signals_calibrated_n30')}")
        r.log("")
        top = sorted(weights.items(), key=lambda x: -float(x[1]))[:12]
        for sig, w in top:
            acc = accuracy.get(sig)
            n_60d = counts.get(sig, 0)
            n_meta = meta.get(sig, {}).get("n")
            avg_ret = meta.get(sig, {}).get("avg_return")
            acc_s = f"{acc*100:.1f}%" if acc is not None else "—"
            ret_s = f"{avg_ret:+.2f}%" if avg_ret is not None else "—"
            r.log(f"    {sig:35s}  w={w:.3f}  acc={acc_s:>7s}  n_60d={n_60d:<5}  avg_ret={ret_s}")


if __name__ == "__main__":
    main()
