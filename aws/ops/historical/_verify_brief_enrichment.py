"""Trigger ai-brief after the auto-deploy + verify enrichment + decisive-call ledger."""
import json
import time
import boto3
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("verify_brief_enrichment") as r:
        # 1. Wait for Lambda update
        r.heading("0) Wait for Lambda update")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ ready, mod={cfg.get('LastModified')}")
                break
            r.log(f"  state={cfg['State']}, lastUpdate={cfg.get('LastUpdateStatus')}")
            time.sleep(3)

        # 2. Invoke
        r.heading("1) Invoke ai-brief end-to-end")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  brief_chars: {inner.get('brief_chars')}")
            r.log(f"  duration_s:  {inner.get('duration_s')}")
            r.log(f"  snapshot_keys: {inner.get('snapshot_keys')}")
            r.log(f"  error: {inner.get('error')}")
        except Exception as e:
            r.log(f"  parse: {e}")
            r.log(f"  body head: {body[:600]}")

        # 3. Inspect the snapshot (calibration_v2 + paper_portfolio)
        r.heading("2) Pull data/ai-brief.json — verify new snapshot keys present")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            snap = d.get("snapshot") or {}
            keys = list(snap.keys())
            r.log(f"  snapshot keys: {keys}")

            # calibration_v2
            cal_v2 = snap.get("calibration_v2") or {}
            r.log(f"")
            r.log(f"  calibration_v2:")
            r.log(f"    iso_week: {cal_v2.get('iso_week')}")
            r.log(f"    weighted_mean_accuracy: {cal_v2.get('weighted_mean_accuracy')}")
            r.log(f"    n_calibrated_n30: {cal_v2.get('n_calibrated_n30')}")
            r.log(f"    highest_weight: {cal_v2.get('highest_weight')}")
            top = cal_v2.get("top_weighted_signals") or []
            r.log(f"    top 5 weighted signals:")
            for x in top[:5]:
                r.log(f"      {x.get('sig'):28s}  w={x.get('weight')}  acc={x.get('accuracy')}  n={x.get('n_outcomes_60d')}  ret={x.get('avg_return_pct')}%")

            # paper_portfolio
            pp = snap.get("paper_portfolio") or {}
            r.log(f"")
            r.log(f"  paper_portfolio:")
            sp = pp.get("signal_portfolio") or {}
            r.log(f"    n_open: {sp.get('n_open')}")
            r.log(f"    n_closed: {sp.get('n_closed')}")
            r.log(f"    current_nav_pct_chg: {sp.get('current_nav_pct_chg')}")
            r.log(f"    near_target: {len(sp.get('near_target') or [])}")
            r.log(f"    near_stop: {len(sp.get('near_stop') or [])}")
            r.log(f"    source_breakdown: {sp.get('source_breakdown')}")
            ml = pp.get("macro_loop2") or {}
            r.log(f"    macro Loop2: phase={ml.get('phase')} regime={ml.get('regime')} alpha={ml.get('system_alpha_pct')}%")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 4. Decisive-call ledger
        r.heading("3) Verify data/decisive-call-history.json was written")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  n_snapshots: {d.get('n_snapshots')}")
            r.log(f"  last_updated: {d.get('last_updated')}")
            for i, snap in enumerate((d.get("snapshots") or [])[-5:]):
                r.log(f"  [{i}] ts={snap.get('timestamp')[:19]}  call={snap.get('call_verb')}  regime={snap.get('regime')}  phase={snap.get('phase')}  ki={snap.get('khalid_score')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 5. Sample the brief
        r.heading("4) Sample of generated brief")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.md")
            md = obj["Body"].read().decode("utf-8")
            r.log(f"  size: {len(md):,} chars")
            r.log("")
            # Show first 2000 chars
            r.log(md[:2500])
            r.log("...")
            r.log("")
            # Find DECISIVE CALL section
            import re
            m = re.search(r'(?i)decisive\s*call(.*?)(?=\n#{1,3}\s|\Z)', md, re.DOTALL)
            if m:
                r.log("=== DECISIVE CALL section ===")
                r.log(m.group(0)[:1500])
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
