"""Audit Loop 1 calibration + re-invoke morning-intelligence now that credits are back.

3 things to check:
1. Did Loop 1 cross 30-outcomes-per-signal threshold? (was projected ~May 2, today May 4)
2. Re-invoke justhodl-morning-intelligence to confirm credits unblocked it
3. Quick health check on other Anthropic-dependent Lambdas
"""
import json
import time
from collections import Counter
from datetime import datetime, timezone, timedelta

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)


def main():
    with report("loop1_audit_and_morning_intel") as r:
        # ─────────────────────────────────────────────
        r.heading("1) Loop 1 calibration snapshot — did badge flip GREEN?")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/calibration-snapshot.json")
            cs = json.loads(obj["Body"].read())
            r.log(f"  generated_at: {cs.get('generated_at') or cs.get('as_of')}")
            summ = cs.get("summary") or {}
            r.log(f"  signal types tracked: {summ.get('n_signal_types')}")
            r.log(f"  total outcomes 60d:   {summ.get('n_outcomes_60d')}")
            r.log(f"  weighted accuracy:    {summ.get('weighted_acc')}")
            r.log(f"  best signal:          {summ.get('best_signal')}")
            r.log(f"  worst signal:         {summ.get('worst_signal')}")

            # Per-signal coverage analysis
            per_signal = cs.get("by_signal") or cs.get("per_signal") or []
            if per_signal:
                # Map to list if dict
                if isinstance(per_signal, dict):
                    per_signal = [{"signal_type": k, **v} for k, v in per_signal.items()]
                r.log("")
                r.log(f"  Per-signal n vs 30-outcome calibration threshold:")
                meeting = 0
                for ps in per_signal:
                    n = ps.get("n_60d") or ps.get("n") or ps.get("count") or 0
                    if n >= 30:
                        meeting += 1
                    badge = "✅" if n >= 30 else ("🟡" if n >= 10 else "🔴")
                    acc = ps.get("acc_60d") or ps.get("accuracy") or ps.get("acc")
                    weight = ps.get("weight")
                    name = ps.get("signal_type") or ps.get("name") or "?"
                    if isinstance(acc, (int, float)):
                        acc_pct = f"{acc*100:.1f}%" if acc <= 1 else f"{acc:.1f}%"
                    else:
                        acc_pct = "—"
                    r.log(f"    {badge} {name:32s}  n={n:>4}  acc={acc_pct:>7}  w={weight}")
                r.log("")
                r.log(f"  → {meeting}/{len(per_signal)} signal types meeting 30-outcome threshold")
                r.log(f"  → Loop 1 badge: {'GREEN ✅ (calibration meaningful)' if meeting >= len(per_signal)*0.5 else 'YELLOW 🟡 (still gathering)'}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # ─────────────────────────────────────────────
        r.heading("2) Direct DDB count — outcomes per signal type, last 60d")
        try:
            tbl = ddb.Table("justhodl-outcomes")
            cutoff = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
            items = []
            last_key = None
            pages = 0
            while True:
                kw = {
                    "Limit": 1000,
                    "FilterExpression": Attr("checked_at").gte(cutoff) & Attr("is_legacy").ne(True),
                }
                if last_key:
                    kw["ExclusiveStartKey"] = last_key
                resp = tbl.scan(**kw)
                items.extend(resp.get("Items", []))
                last_key = resp.get("LastEvaluatedKey")
                pages += 1
                if not last_key or pages > 12:
                    break
            r.log(f"  total non-legacy outcomes 60d: {len(items)}  (paginated {pages}x)")
            counts = Counter(it.get("signal_type", "?") for it in items)
            # rank, ≥30 first
            r.log("")
            r.log(f"  Signal types by outcome count:")
            for sig, n in counts.most_common():
                badge = "✅" if n >= 30 else ("🟡" if n >= 10 else "🔴")
                r.log(f"    {badge} {sig:32s}  n={n}")
            ready = sum(1 for v in counts.values() if v >= 30)
            r.log("")
            r.log(f"  → {ready}/{len(counts)} types calibration-ready (n>=30)")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # ─────────────────────────────────────────────
        r.heading("3) Re-invoke justhodl-morning-intelligence (credits back)")
        try:
            t0 = time.time()
            resp = lam.invoke(FunctionName="justhodl-morning-intelligence", InvocationType="RequestResponse")
            body = resp["Payload"].read().decode()
            r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {body[:500]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # ─────────────────────────────────────────────
        r.heading("4) Quick smoke on other Anthropic Lambdas")
        for name in [
            "justhodl-investor-agents",
            "justhodl-watchlist-debate",
            "justhodl-financial-secretary",
        ]:
            try:
                cfg = lam.get_function_configuration(FunctionName=name)
                r.log(f"  {name:35s}  state={cfg['State']:8s} mod={cfg.get('LastModified', '?')[:19]}  reserved={cfg.get('ReservedConcurrentExecutions', '—')}")
            except Exception as e:
                r.log(f"  ✗ {name}: {e}")

        # ─────────────────────────────────────────────
        r.heading("5) SSM calibration weights — anything updated recently?")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/weights")["Parameter"]
            d = json.loads(v["Value"])
            r.log(f"  last_modified: {v.get('LastModifiedDate')}")
            r.log(f"  n_weights: {len(d)}")
            top = sorted(d.items(), key=lambda x: -float(x[1]))[:5]
            r.log(f"  top 5 weights:")
            for sig, w in top:
                r.log(f"    {sig:32s}  w={w}")
        except Exception as e:
            r.log(f"  ✗ weights: {e}")
        try:
            v = ssm.get_parameter(Name="/justhodl/calibration/accuracy")["Parameter"]
            d = json.loads(v["Value"])
            r.log(f"  accuracy params last_modified: {v.get('LastModifiedDate')}")
            r.log(f"  n_acc keys: {len(d)}")
        except Exception as e:
            r.log(f"  ✗ accuracy: {e}")


if __name__ == "__main__":
    main()
