"""End-to-end verify: calls.html live, ledger populated, position-monitor running."""
import json
import urllib.request
import boto3
from ops_report import report

UA = {"User-Agent": "justhodl-audit/1.0"}
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), None
    except Exception as e:
        return None, "", str(e)


def main():
    with report("verify_calls_e2e") as r:
        # 1. calls.html live
        r.heading("1) calls.html on production")
        code, body, err = fetch("https://justhodl.ai/calls.html")
        if err:
            r.log(f"  ✗ {err}")
        else:
            r.log(f"  ✓ status={code}, size={len(body):,}b")
            checks = [
                ("title", "<title>Calls · JustHodl</title>" in body),
                ("nav active", 'class="tab active" href="/calls.html"' in body),
                ("now banner", 'id="now-banner"' in body),
                ("KPI row", 'id="kpi-row"' in body),
                ("timeline svg", 'id="timeline-chart"' in body),
                ("changes section", 'id="changes-section"' in body),
                ("history table", 'id="history-table"' in body),
                ("auto-refresh", "setInterval(load, 5*60*1000)" in body),
                ("loads ledger", "decisive-call-history.json" in body),
                ("verb colors", "EXIT_ALL_RISK" in body and "VERB_COLOR" in body),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")

        # 2. Ledger state
        r.heading("2) Decisive-call ledger state")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/decisive-call-history.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  n_snapshots: {d.get('n_snapshots')}")
            r.log(f"  last_updated: {d.get('last_updated')}")
            r.log(f"")
            r.log(f"  All snapshots:")
            for s in d.get("snapshots") or []:
                r.log(f"    ts={s.get('timestamp')[:19]}  call={s.get('call_verb'):20s}  highest={s.get('highest_weight_signal')}  acc={s.get('weighted_mean_accuracy')}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # 3. Calls tab visible across pages
        r.heading("3) Calls tab visible on key pages")
        for p in ["today.html", "brief.html", "performance.html", "weights.html", "accuracy.html"]:
            code, body, err = fetch(f"https://justhodl.ai/{p}")
            has = ('href="/calls.html"' in body) or ('href="calls.html"' in body)
            r.log(f"  {'✓' if has else '✗'} {p:25s}  Calls link: {has}")

        # 4. position-monitor schedule
        r.heading("4) position-monitor schedule + recent metrics")
        try:
            cfg = lam.get_function(FunctionName="justhodl-position-monitor")["Configuration"]
            r.log(f"  state: {cfg['State']}, mem={cfg['MemorySize']}MB, timeout={cfg['Timeout']}s")
            r.log(f"  last modified: {cfg.get('LastModified')}")
        except Exception as e:
            r.log(f"  ✗ {e}")
        try:
            rule = events.describe_rule(Name="justhodl-position-monitor-30min")
            r.log(f"  ✓ schedule: {rule['ScheduleExpression']} state={rule['State']}")
        except Exception as e:
            r.log(f"  ✗ rule: {e}")

        # 5. position-monitor state
        r.heading("5) position-monitor state")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/position-monitor-state.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  last_run: {d.get('last_run')}")
            r.log(f"  last_call_verb_seen: {d.get('last_call_verb')}")
            r.log(f"  alerts_in_dedup_window: {len(d.get('alerts') or {})}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
