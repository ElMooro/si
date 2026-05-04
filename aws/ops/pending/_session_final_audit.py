"""Final session audit:
  1. Lambdas live (count + recently-modified)
  2. S3 outputs producing fresh data
  3. List Lambdas that depend on Anthropic API (broken until credits topped up)
"""
import json
import boto3
from datetime import datetime, timezone, timedelta
from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("session_final_audit") as r:
        r.heading("1) Lambda count + recent activity (last 24h)")
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        all_lambdas = []
        next_marker = None
        while True:
            kw = {"MaxItems": 100}
            if next_marker:
                kw["Marker"] = next_marker
            resp = lam.list_functions(**kw)
            all_lambdas.extend(resp["Functions"])
            next_marker = resp.get("NextMarker")
            if not next_marker:
                break
        recent = [f for f in all_lambdas if f["LastModified"] >= cutoff]
        r.log(f"  total Lambdas in account: {len(all_lambdas)}")
        r.log(f"  modified last 24h: {len(recent)}")
        for f in sorted(recent, key=lambda x: -float(x["LastModified"].split("T")[0].replace("-", "")))[:15]:
            r.log(f"    {f['LastModified'][:19]}  {f['FunctionName']}")

        r.heading("2) Fresh S3 outputs (modified < 6h)")
        fresh_cutoff = datetime.now(timezone.utc) - timedelta(hours=6)
        keys_to_check = [
            "intelligence-report.json",
            "data/calibration-snapshot.json",
            "data/sector-rotation.json",
            "data/momentum-scanner.json",
            "data/alert-history.json",
            "data/correlation-surface.json",
            "data/macro-surprise.json",
            "data/yield-curve.json",
            "data/eurodollar-stress.json",
            "data/auction-crisis.json",
            "divergence/current.json",
            "cot/extremes/current.json",
            "data/etf-flows.json",
            "data/earnings-tracker.json",
            "data/short-interest.json",
            "data/insider-trades.json",
            "data/historical-analogs.json",
            "data/event-study.json",
            "data/whats-changed.json",
            "data/feedback.json",
            "data/ab-test-results.json",
            "portfolio/signal-portfolio-state.json",
            "data/allocator.json",
            "data/risk-sizer.json",
            "data/asymmetric-scorer.json",
            "data/ai-brief.json",
        ]
        fresh = stale = missing = 0
        for k in keys_to_check:
            try:
                head = s3.head_object(Bucket=BUCKET, Key=k)
                last = head["LastModified"]
                age_min = int((datetime.now(timezone.utc) - last).total_seconds() / 60)
                size = head["ContentLength"]
                status = "✓ fresh" if last >= fresh_cutoff else "⚠ stale"
                if last >= fresh_cutoff:
                    fresh += 1
                else:
                    stale += 1
                r.log(f"  {status}  {k:48s}  age={age_min:>5d}min  size={size:>9,}b")
            except Exception:
                missing += 1
                r.log(f"  ✗ MISS   {k}")
        r.log(f"  totals: fresh={fresh}, stale={stale}, missing={missing}")

        r.heading("3) Anthropic-dependent Lambdas (BROKEN until credits topped up)")
        # Search Lambda environment variables for ANTHROPIC keys
        anthropic_lambdas = []
        for f in all_lambdas:
            try:
                cfg = lam.get_function_configuration(FunctionName=f["FunctionName"])
                env = (cfg.get("Environment") or {}).get("Variables") or {}
                if any("ANTHROPIC" in k.upper() for k in env.keys()):
                    anthropic_lambdas.append(f["FunctionName"])
            except Exception:
                continue
        r.log(f"  found {len(anthropic_lambdas)} Lambdas with ANTHROPIC_KEY/ANTHROPIC_API_KEY env:")
        for n in sorted(anthropic_lambdas):
            r.log(f"    • {n}")

        r.heading("4) Wave 1+2+3 Lambda inventory (this rebuild)")
        wave_lambdas = [
            # Wave 1
            "justhodl-earnings-tracker", "justhodl-short-interest", "justhodl-etf-flows",
            "justhodl-macro-surprise", "justhodl-yield-curve", "justhodl-signal-portfolio",
            "justhodl-historical-analogs", "justhodl-event-study", "justhodl-correlation-surface",
            "justhodl-ab-test", "justhodl-feedback", "justhodl-morning-brief-tg",
            "justhodl-whats-changed",
            # Wave 2
            "justhodl-calibration-snapshot", "justhodl-sector-rotation", "justhodl-alert-router",
            # Wave 3
            "justhodl-momentum-scanner", "justhodl-wave-signal-logger",
            # New today
            "justhodl-ai-brief", "justhodl-allocator",
        ]
        for n in wave_lambdas:
            try:
                cfg = lam.get_function_configuration(FunctionName=n)
                r.log(f"    ✓ {n:42s} state={cfg['State']:8s} mod={cfg['LastModified'][:19]}")
            except Exception as e:
                r.log(f"    ✗ {n:42s} {str(e)[:60]}")


if __name__ == "__main__":
    main()
