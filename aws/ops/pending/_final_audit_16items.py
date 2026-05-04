"""Final audit: verify all 16 items are live + on schedule."""
import json
import time
import boto3
import urllib.request
from ops_report import report

REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

ITEMS = [
    ("#1",  "Wire Tier S+A + Tier 1-3 into morning-intel + ai-chat",  None,                                    None),
    ("#2",  "RAG-over-Crisis-KB",                                      None,                                    "data/crisis-knowledge-base.json"),
    ("#3",  "Earnings tracker (PEAD signals)",                         "justhodl-earnings-tracker",             "data/earnings-tracker.json"),
    ("#4",  "Short interest tracker (FINRA + Polygon)",                "justhodl-short-interest",               "data/short-interest.json"),
    ("#5",  "ETF flows tracker (z-scores)",                            "justhodl-etf-flows",                    "data/etf-flows.json"),
    ("#6",  "Macro Surprise Index (CESI proxy)",                       "justhodl-macro-surprise",               "data/macro-surprise.json"),
    ("#7",  "Yield curve shape decomposition",                         "justhodl-yield-curve",                  "data/yield-curve.json"),
    ("#8",  "Per-signal paper portfolio + PnL",                        "justhodl-signal-portfolio",             "portfolio/signal-portfolio-state.json"),
    ("#9",  "Historical Analog Finder",                                "justhodl-historical-analogs",           "data/historical-analogs.json"),
    ("#10", "Event Study Automation",                                  "justhodl-event-study",                  "data/event-study.json"),
    ("#11", "Cross-Asset Correlation Surface",                         "justhodl-correlation-surface",          "data/correlation-surface.json"),
    ("#12", "A/B Test Of Competing Models",                            "justhodl-ab-test",                      "data/ab-test.json"),
    ("#13", "User Feedback Labeling",                                  "justhodl-feedback",                     None),
    ("#14", "Telegram Morning Brief Delivery",                         "justhodl-morning-brief-tg",             None),
    ("#15", "Unified Ticker Page",                                     None,                                    None),
    ("#16", "What Changed Today (daily diff)",                         "justhodl-whats-changed",                "data/whats-changed.json"),
]


def main():
    with report("final_audit_16items") as r:
        r.heading("Final audit — 16 exponential items")

        live_count = 0
        scheduled_count = 0
        s3_count = 0

        for item, desc, fn, s3_key in ITEMS:
            r.log(f"")
            r.log(f"━━ {item} {desc} ━━")

            # Check Lambda
            if fn:
                try:
                    cfg = lam.get_function(FunctionName=fn)["Configuration"]
                    age = cfg.get("LastUpdateStatus", "?")
                    r.ok(f"  Lambda {fn} — {cfg['Runtime']} {cfg['MemorySize']}MB age={age}")
                    live_count += 1

                    # Check EventBridge schedule
                    rules = events.list_rule_names_by_target(TargetArn=cfg["FunctionArn"]).get("RuleNames", [])
                    if rules:
                        for rn in rules:
                            rule = events.describe_rule(Name=rn)
                            r.log(f"    schedule {rn}: {rule.get('ScheduleExpression', '?')}  ({rule.get('State')})")
                            scheduled_count += 1
                    else:
                        r.log(f"    no EventBridge schedule (manual/event-driven)")
                except Exception as e:
                    r.log(f"  ✗ Lambda check fail: {e}")
            else:
                r.log(f"  (no Lambda — this is a frontend/wiring item)")

            # Check S3 output
            if s3_key:
                try:
                    h = s3.head_object(Bucket="justhodl-dashboard-live", Key=s3_key)
                    age_min = (time.time() - h["LastModified"].timestamp()) / 60
                    r.ok(f"  S3 {s3_key}: {h['ContentLength']:,}b  age={age_min:.1f}min")
                    s3_count += 1
                except Exception as e:
                    r.log(f"  ✗ S3 {s3_key}: missing")

        r.log(f"")
        r.log(f"═══ SUMMARY ═══")
        r.log(f"  Lambdas live:     {live_count} / 13 expected")
        r.log(f"  Schedules wired:  {scheduled_count}")
        r.log(f"  S3 outputs:       {s3_count} / 11 expected")

        # Verify ticker.html and feedback.html exist
        for f in ["ticker.html", "feedback.html"]:
            try:
                h = s3.head_object(Bucket="justhodl-dashboard-live", Key=f)
                r.ok(f"  ✓ {f} on S3 ({h['ContentLength']:,}b)")
            except:
                r.log(f"  ✗ {f} NOT on S3")

        # Verify feedback URL manifest
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="feedback-url.json")
            manifest = json.loads(obj["Body"].read())
            r.ok(f"  ✓ feedback-url.json → {manifest.get('feedback_url', '?')}")
        except Exception as e:
            r.log(f"  ✗ feedback-url.json: {e}")


if __name__ == "__main__":
    main()
