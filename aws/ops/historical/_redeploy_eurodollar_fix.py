"""Redeploy eurodollar-stress with bug fixes and reinvoke wave-logger v3 to verify
the eurodollar_stress translator picks up the new file.

Two fixes applied:
  1. realized_vol off-by-one (was rejecting 60-len windows due to <window+2 check)
  2. OFRFSI → STLFSI4 (OFR's index moved/discontinued; St Louis Fed v4 is the active replacement)

Expected: 8/8 signals fire, composite recomputed, wave-logger eurodollar_stress
fires only if score crosses 70 or 30 thresholds (likely no signal today since
prior run was 41.4 = CALM, mid-zone).
"""
import io
import json
import os
import time
import zipfile
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime, timezone, timedelta
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-eurodollar-stress/source"

lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
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
    with report("redeploy_eurodollar_fix") as r:
        r.heading("1) Redeploy justhodl-eurodollar-stress with 2 bug fixes")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        lam.update_function_code(FunctionName="justhodl-eurodollar-stress", ZipFile=zb)
        for _ in range(15):
            cfg = lam.get_function(FunctionName="justhodl-eurodollar-stress")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(2)
        r.ok(f"  ✓ deployed at {cfg.get('LastModified')}")

        r.heading("2) Invoke + check 8/8 signals")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-eurodollar-stress", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:400]}")

        # S3 verify
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/eurodollar-stress.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  composite_score: {d.get('composite_score')}")
            r.log(f"  severity: {d.get('severity')}  regime: {d.get('regime')}")
            r.log(f"  n_signals_used: {d.get('n_signals_used')}/{d.get('n_signals_total')}")
            r.log("")
            r.log("  Signal breakdown:")
            for s in d.get("signals", []):
                bar = "█" * int(s["score_0_100"] / 5)
                r.log(f"    {s['id']:14s}  value={s['value']:>10}  score={s['score_0_100']:>5.1f}/100  {bar}")
            if d.get("hot_signals"):
                r.log("")
                r.log("  🔴 hot signals (>=70):")
                for s in d["hot_signals"]:
                    r.log(f"    {s['id']:14s}  score={s['score']:.1f}  ({s['label']})")
            if d.get("cold_signals"):
                r.log("")
                r.log("  🟢 cold signals (<=30):")
                for s in d["cold_signals"]:
                    r.log(f"    {s['id']:14s}  score={s['score']:.1f}  ({s['label']})")
            if d.get("failures"):
                r.log("")
                r.log("  ⚠ failures:")
                for f in d["failures"]:
                    r.log(f"    {f}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("3) Reinvoke wave-signal-logger v3 — verify eurodollar_stress dispatch lights up")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-wave-signal-logger", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}  duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        r.heading("4) Confirm in DDB — recent eurodollar_stress signals")
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        tbl = ddb.Table("justhodl-signals")
        items = []
        last_key = None
        pages = 0
        while True:
            kw = {"Limit": 1000, "FilterExpression": Attr("logged_at").gte(cutoff)}
            if last_key:
                kw["ExclusiveStartKey"] = last_key
            r2 = tbl.scan(**kw)
            items.extend(r2.get("Items", []))
            last_key = r2.get("LastEvaluatedKey")
            pages += 1
            if not last_key or pages > 6:
                break
        from collections import Counter
        types = Counter(it.get("signal_type", "?") for it in items)
        r.log(f"  total recent signals (5min): {len(items)}")
        for t, n in types.most_common():
            star = "★ " if t == "eurodollar_stress" else "  "
            r.log(f"  {star}{t:30s} n={n}")
        # Eurodollar shows only if score >= 70 or <= 30 (CALM mid-zone produces 0)
        eurorec = [it for it in items if it.get("signal_type") == "eurodollar_stress"]
        if eurorec:
            for it in eurorec:
                r.log(f"  ★ eurodollar_stress sample:")
                r.log(f"      val: {it.get('signal_value')}")
                r.log(f"      pred: {it.get('predicted_direction')}, conf: {it.get('confidence')}")
                r.log(f"      rationale: {str(it.get('rationale',''))[:140]}")
        else:
            r.log("  (no eurodollar_stress signal — score is in mid-zone 30-70, no actionable extreme)")


if __name__ == "__main__":
    main()
