"""Final verify of weights.html live + Weights tab visible across pages."""
import json
import urllib.request
from ops_report import report

UA = {"User-Agent": "justhodl-audit/1.0"}


def fetch(url):
    req = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, r.read().decode("utf-8", errors="replace"), None
    except Exception as e:
        return None, "", str(e)


def main():
    with report("verify_weights_live") as r:
        r.heading("1) weights.html on production")
        code, body, err = fetch("https://justhodl.ai/weights.html")
        if err:
            r.log(f"  ✗ {code}: {err}")
        else:
            r.log(f"  ✓ status: {code}, size: {len(body):,}b")
            checks = [
                ("title", '<title>Weights · JustHodl</title>' in body),
                ("nav active", 'class="tab active" href="/weights.html"' in body),
                ("KPI row", 'id="kpi-row"' in body),
                ("chart svg", 'id="weight-chart"' in body),
                ("legend", 'id="legend"' in body),
                ("movers", 'id="risers"' in body and 'id="fallers"' in body),
                ("table", 'id="weights-table"' in body),
                ("loads history-index", 'history-index.json' in body),
                ("loads latest", 'calibration/latest.json' in body),
                ("auto-refresh 10min", 'setInterval(load, 10' in body),
                ("color palette", 'PALETTE' in body),
                ("click-to-highlight", "highlighted = highlighted === sig" in body),
            ]
            for label, ok in checks:
                r.log(f"    {'✓' if ok else '✗'} {label}")

        r.heading("2) Snapshot data check")
        try:
            d = fetch("https://justhodl-dashboard-live.s3.amazonaws.com/calibration/latest.json")[1]
            d = json.loads(d)
            r.log(f"  iso_week: {d.get('iso_week')}")
            r.log(f"  weights count: {len(d.get('weights', {}))}")
            r.log(f"  accuracy_meta count: {len(d.get('accuracy_meta', {}))}")
            r.log(f"  outcome_counts_60d count: {len(d.get('outcome_counts_60d', {}))}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("3) Weights tab visible on key pages")
        for p in ["today.html", "brief.html", "performance.html", "accuracy.html"]:
            code, body, err = fetch(f"https://justhodl.ai/{p}")
            has = 'href="/weights.html"' in body or 'href="weights.html"' in body
            r.log(f"  {'✓' if has else '✗'} {p:25s}  Weights link: {has}")


if __name__ == "__main__":
    main()
