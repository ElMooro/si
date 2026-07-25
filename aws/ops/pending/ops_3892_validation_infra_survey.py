"""
ops_3892 — PROBE (writes no code): read the live state of the platform's
existing signal-validation infrastructure, comprehensively, before drawing
any conclusion about "which signals are reflected in price." 22 dedicated
calibration/backtest engines were found on repo audit; this reads the 7 most
fleet-wide/comprehensive ones rather than re-deriving anything from scratch:

  data/confluence-meta.json     — Fade Index: engines with >=10 graded calls
                                   and <35% hit rate (broken, should be inverted
                                   or dropped)
  data/calibration-fleet.json   — universal IC-calibration: every registered
                                   signal engine's score vs 21-session forward SPY
  data/meta-labeler.json        — Lopez de Prado meta-labeling: which primary
                                   signals get TRUSTED vs suppressed
  data/backtest-harness.json    — anchored walk-forward Sharpe/deflated-Sharpe
                                   per rule archetype (the desk's own bar for
                                   "does this actually work")
  data/signal-backtest.json     — forward-return proof for opportunity-engine/
                                   dislocation/best-setups snapshots
  data/factor-ic.json           — factor-level information coefficient
  data/alert-backtests.json     — institutional alert-rule replay hit rates
  data/calibration-latest.json  — 8-factor alpha-score model calibration
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

FEEDS = {
    "confluence_meta": "data/confluence-meta.json",
    "calibration_fleet": "data/calibration-fleet.json",
    "meta_labeler": "data/meta-labeler.json",
    "backtest_harness": "data/backtest-harness.json",
    "signal_backtest": "data/signal-backtest.json",
    "factor_ic": "data/factor-ic.json",
    "alert_backtests": "data/alert-backtests.json",
    "calibration_latest": "data/calibration-latest.json",
}


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def main():
    with report("3892_validation_infra_survey") as rep:
        rep.heading("ops 3892 — survey the platform's existing self-validation infrastructure")
        docs = {}
        failures = []

        for name, key in FEEDS.items():
            rep.section(f"reading {key}")
            try:
                doc, lm = get(key)
                docs[name] = doc
                rep.ok(f"  {age_h(lm)}h old, generated_at={doc.get('generated_at')}")
                rep.log(f"  top-level keys: {sorted(doc.keys())[:20]}")
            except Exception as e:
                rep.fail(f"  unreadable: {str(e)[:180]}")
                failures.append(key)

        rep.section("A. confluence-meta Fade Index — engines actively broken (<35% hit, n>=10)")
        cm = docs.get("confluence_meta") or {}
        fade = cm.get("fade_index") or []
        rep.kv(n_faded_engines=len(fade))
        for f in fade[:20]:
            rep.log(f"    {f}")

        rep.section("B. calibration-fleet — IC distribution across the registered fleet")
        cf = docs.get("calibration_fleet") or {}
        rep.log(f"  FULL DOC (first 3000 chars): {json.dumps(cf, default=str)[:3000]}")

        rep.section("C. meta-labeler — trust gate outcome")
        ml = docs.get("meta_labeler") or {}
        rep.log(f"  FULL DOC (first 2500 chars): {json.dumps(ml, default=str)[:2500]}")

        rep.section("D. backtest-harness — walk-forward per archetype")
        bh = docs.get("backtest_harness") or {}
        rep.log(f"  FULL DOC (first 3000 chars): {json.dumps(bh, default=str)[:3000]}")

        rep.section("E. signal-backtest — forward-return proof")
        sb = docs.get("signal_backtest") or {}
        rep.log(f"  FULL DOC (first 2500 chars): {json.dumps(sb, default=str)[:2500]}")

        rep.section("F. factor-ic — factor-level information coefficient")
        fic = docs.get("factor_ic") or {}
        rep.log(f"  FULL DOC (first 2000 chars): {json.dumps(fic, default=str)[:2000]}")

        rep.section("G. alert-backtests — institutional alert rule hit rates")
        ab = docs.get("alert_backtests") or {}
        rep.log(f"  FULL DOC (first 2000 chars): {json.dumps(ab, default=str)[:2000]}")

        rep.section("H. calibration-latest — 8-factor alpha model")
        cl = docs.get("calibration_latest") or {}
        rep.log(f"  FULL DOC (first 2000 chars): {json.dumps(cl, default=str)[:2000]}")

        rep.section("verdict")
        rep.kv(failures=str(failures), n_docs_read=len(docs))
        if len(failures) >= 4:
            rep.fail(f"most feeds unreadable: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
