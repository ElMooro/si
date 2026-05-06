"""Final calibration recompute after directional backfill of 420 outcomes."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("recalibrate_post_directional") as r:
        r.heading("1) Trigger justhodl-calibrator")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-calibrator", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            if "errorMessage" in outer:
                r.log(f"  ✗ {outer['errorMessage'][:200]}")
            else:
                inner = json.loads(outer.get("body", "{}"))
                r.log(f"  total_outcomes: {inner.get('total_outcomes')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        r.heading("2) New SSM accuracy after backfill")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/accuracy")["Parameter"]
            d = json.loads(v["Value"])
            r.log(f"  accuracy keys: {len(d)} (was 17)")
            r.log("")
            r.log(f"  All entries sorted by accuracy:")
            entries = []
            for sig, info in d.items():
                if isinstance(info, dict):
                    acc = info.get("accuracy")
                    n = info.get("n")
                    avg = info.get("avg_return")
                    entries.append((sig, acc, n, avg))
            entries.sort(key=lambda x: -(x[1] or 0))
            for sig, acc, n, avg in entries:
                acc_s = f"{acc*100:.1f}%" if acc is not None else "—"
                avg_s = f"{avg:+.2f}%" if avg is not None else "—"
                r.log(f"    {sig:35s}  acc={acc_s:>7}  n={n:<5}  avg_ret={avg_s}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("3) New SSM weights")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]
            d = json.loads(v["Value"])
            top = sorted(d.items(), key=lambda x: -float(x[1]))
            r.log(f"  weights count: {len(d)}")
            r.log("")
            r.log(f"  Top 15 weights:")
            for sig, w in top[:15]:
                r.log(f"    {sig:35s}  w={float(w):.3f}")
            r.log("")
            r.log(f"  Bottom 5 weights:")
            for sig, w in top[-5:]:
                r.log(f"    {sig:35s}  w={float(w):.3f}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("4) Re-snapshot W19 with the new state")
        resp = lam.invoke(FunctionName="justhodl-calibration-snapshotter", InvocationType="RequestResponse")
        r.log(f"  resp: {resp['Payload'].read().decode()[:300]}")

        r.heading("5) Final view of 2026-W19 snapshot")
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="calibration/latest.json")
        d = json.loads(obj["Body"].read())
        weights = d.get("weights") or {}
        accuracy = d.get("accuracy") or {}
        meta = d.get("accuracy_meta") or {}
        counts = d.get("outcome_counts_60d") or {}
        summ = d.get("summary") or {}

        r.log(f"  highest: {summ.get('highest_weight')}")
        r.log(f"  median: {summ.get('median_weight')}")
        r.log(f"  weighted_mean_acc: {summ.get('weighted_mean_accuracy')}")
        r.log(f"  n_calibrated_n30: {summ.get('n_signals_calibrated_n30')}")
        r.log("")
        top = sorted(weights.items(), key=lambda x: -float(x[1]))[:18]
        r.log(f"  Full ranking by weight:")
        for sig, w in top:
            acc = accuracy.get(sig)
            n_60d = counts.get(sig, 0)
            avg_ret = (meta.get(sig) or {}).get("avg_return")
            acc_s = f"{acc*100:.1f}%" if acc is not None else "—"
            ret_s = f"{avg_ret:+.2f}%" if avg_ret is not None else "—"
            badge = " ★" if n_60d >= 30 and (acc or 0) >= 0.7 else "  "
            r.log(f"  {badge} {sig:35s}  w={float(w):.3f}  acc={acc_s:>7}  n={n_60d:<5}  avg_ret={ret_s}")


if __name__ == "__main__":
    main()
