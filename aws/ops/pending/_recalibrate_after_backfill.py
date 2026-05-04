"""Trigger calibrator + re-snapshot now that screener_top_pick has 1405 scored outcomes.

Expected: weight should jump dramatically for screener_top_pick (from 0.85 default
to ~1.5 max because accuracy=76.5%) once calibrator runs with the new data.
"""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("recalibrate_after_backfill") as r:
        # 1. Trigger calibrator
        r.heading("1) Invoke justhodl-calibrator (will rescan all outcomes)")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        # Truncate output as it can be large
        r.log(f"  resp head: {body[:600]}")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"")
            r.log(f"  total_outcomes analyzed: {inner.get('total_outcomes')}")
            new_weights = inner.get("weights_updated", {})
            r.log(f"  signals with weights:    {len(new_weights)}")
            stp = new_weights.get("screener_top_pick")
            r.log(f"")
            r.log(f"  screener_top_pick NEW WEIGHT: {stp}  (was 0.85 default)")
            # Top 8
            r.log(f"")
            r.log(f"  Top 8 weights:")
            top = sorted(new_weights.items(), key=lambda x: -x[1])[:8]
            for sig, w in top:
                r.log(f"    {sig:35s}  w={w:.3f}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # 2. Pull SSM accuracy to confirm screener_top_pick now has an entry
        r.heading("2) Verify SSM accuracy now has screener_top_pick")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/accuracy")["Parameter"]
            d = json.loads(v["Value"])
            r.log(f"  accuracy keys: {len(d)}")
            r.log(f"  last_modified: {v.get('LastModifiedDate')}")
            stp = d.get("screener_top_pick")
            if stp:
                r.log(f"  ✓ screener_top_pick: {stp}")
            else:
                r.log("  ✗ screener_top_pick still missing from accuracy SSM")
            # Show the others above 0.6 acc
            r.log(f"")
            r.log(f"  All accuracy entries:")
            for sig, info in sorted(d.items(), key=lambda x: -(x[1].get("accuracy") or 0) if isinstance(x[1], dict) else 0):
                if isinstance(info, dict):
                    acc = info.get("accuracy")
                    n = info.get("n")
                    avg = info.get("avg_return")
                    r.log(f"    {sig:35s}  acc={f'{acc*100:.1f}%' if acc is not None else '—':>7s}  n={n}  avg_ret={avg}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 3. Re-run snapshotter to capture the updated state
        r.heading("3) Re-run snapshotter to capture updated calibration in this week's snapshot")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibration-snapshotter", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        # 4. Verify the new latest.json
        r.heading("4) Pull updated calibration/latest.json")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d = json.loads(obj["Body"].read())
            summ = d.get("summary") or {}
            r.log(f"  iso_week: {d.get('iso_week')}")
            r.log(f"  n_weights: {summ.get('n_weights_total')}")
            r.log(f"  highest_weight: {summ.get('highest_weight')}")
            r.log(f"  median_weight: {summ.get('median_weight')}")
            r.log(f"  weighted_mean_accuracy: {summ.get('weighted_mean_accuracy')}")
            stp_w = (d.get("weights") or {}).get("screener_top_pick")
            stp_meta = (d.get("accuracy_meta") or {}).get("screener_top_pick")
            stp_n = (d.get("outcome_counts_60d") or {}).get("screener_top_pick")
            r.log(f"")
            r.log(f"  screener_top_pick in snapshot:")
            r.log(f"    weight:        {stp_w}")
            r.log(f"    accuracy_meta: {stp_meta}")
            r.log(f"    n_outcomes_60d: {stp_n}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("5) Top 10 weights from the new snapshot")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
            d = json.loads(obj["Body"].read())
            weights = d.get("weights") or {}
            counts = d.get("outcome_counts_60d") or {}
            accuracy = d.get("accuracy") or {}
            top = sorted(weights.items(), key=lambda x: -float(x[1]))[:12]
            for sig, w in top:
                acc = accuracy.get(sig)
                n = counts.get(sig, 0)
                acc_s = f"{acc*100:.1f}%" if acc is not None else "—"
                badge = " ★ NEW" if sig == "screener_top_pick" and stp_meta else ""
                r.log(f"    {sig:35s}  w={w:.3f}  acc={acc_s:>6s}  n_60d={n:<5}{badge}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
