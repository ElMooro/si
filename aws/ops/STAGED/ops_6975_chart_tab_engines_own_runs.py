"""ops 6975 - read-only: did each engine's own run carry the change made for the chart tabs?

The five engines changed on 2026-11-26 were deployed without being invoked; each takes effect at its own run:
options (five-minute lane on market days), on-chain 15:31 UTC, insider trades 21:10 UTC, the catalyst calendar
12:00 UTC, plumbing 14:21 UTC. This op reads each document from the bucket and each function's last day of runs.

Nothing is invoked and nothing is written. Safe to run again.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6975_chart_tab_engines_own_runs"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
DEPLOYED = datetime(2026, 11, 26, 14, 59, tzinfo=timezone.utc)
ENGINES = ["justhodl-options-analytics", "justhodl-onchain-ratios", "justhodl-insider-trades", "justhodl-catalyst-calendar", "justhodl-plumbing-aggregator"]


def main():
    import boto3
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)
    now = datetime.now(timezone.utc)
    done, waiting, wrong = [], [], []

    def kept(key):
        o = s3.get_object(Bucket=LIVE, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]

    with report(NAME) as r:
        r.heading("6975 - the five engines' own runs since the deploy of 14:59 UTC on 2026-11-26 (read-only), %s" % now.isoformat(timespec="minutes"))

        def verdict(name, written, ok, said):
            ran = written > DEPLOYED
            (done if ran and ok else wrong if ran else waiting).append(name)
            r.log("%s | written %s (%s) | %s | %s" % (name, written.isoformat(timespec="minutes"), "after the deploy" if ran else "before the deploy: its own run has not come yet", "CARRIES THE CHANGE" if ran and ok else "DOES NOT CARRY IT" if ran else "waiting", said))

        r.section("1. The documents")
        try:
            d, w = kept("data/options-analytics.json")
            verdict("options", w, d.get("version") == "1.1.0" and d.get("universe_size") == 155 and (d.get("n_analyzed") or 0) >= 140,
                    "version %s, %s of %s names analysed in %s s, not analysed: %s" % (d.get("version"), d.get("n_analyzed"), d.get("universe_size"), d.get("elapsed_s"), ", ".join(d.get("not_analyzed") or []) or "none"))
        except Exception as e:  # noqa: BLE001
            r.log("options not read: %s" % str(e)[:160])
        try:
            d, w = kept("data/onchain-ratios.json")
            cq, eth = d.get("cryptoquant") or {}, (d.get("eth") or {}).get("cryptoquant") or {}
            verdict("on-chain", w, bool(cq) and len(eth) >= 10, "CryptoQuant block %s (status %s, %s metrics kept), Ethereum metrics %d, Ethereum errors %s" % ("present" if cq else "absent", cq.get("status"), cq.get("n_metrics_kept"), len(eth), (d.get("eth") or {}).get("errors")))
        except Exception as e:  # noqa: BLE001
            r.log("on-chain not read: %s" % str(e)[:160])
        try:
            d, w = kept("data/insider-trades.json")
            aside, st = d.get("set_aside") or [], d.get("stats") or {}
            verdict("insider trades", w, d.get("version") == "3.1" and "price_check" in d and (st.get("total_value_usd") or 9e12) < 5e9,
                    "version %s, buying total $%s over %s buys, price check %s, set aside: %s" % (d.get("version"), format(st.get("total_value_usd") or 0, ",.0f"), st.get("total_buys"), json.dumps(d.get("price_check")), "; ".join("%s $%s (%s)" % (a.get("ticker"), format(a.get("value") or 0, ",.0f"), a.get("reason")) for a in aside[:6]) or "none"))
        except Exception as e:  # noqa: BLE001
            r.log("insider trades not read: %s" % str(e)[:160])
        try:
            d, w = kept("data/catalyst-calendar.json")
            own = w > datetime(2026, 11, 27, 11, 59, tzinfo=timezone.utc)
            r.log("catalyst calendar | written %s (%s) | version %s, %s events, %s ahead | ahead by type %s" % (w.isoformat(timespec="minutes"), "by the engine's own 12:00 UTC run" if own else "by the publication from the runner (ops 6974); the engine's own run is 12:00 UTC", d.get("version"), d.get("n_events"), d.get("n_ahead"), json.dumps(d.get("by_type_ahead"))))
            for s in d.get("sources") or []:
                if s.get("note") or not s.get("events"):
                    r.log("      source %s: %s events%s%s" % (s.get("source"), s.get("events"), (" | " + str(s.get("document"))) if s.get("document") else "", (" | " + s["note"]) if s.get("note") else ""))
            (done if own and d.get("version") == "2.0.0" and (d.get("n_ahead") or 0) >= 100 else wrong if own else waiting).append("catalyst calendar")
        except Exception as e:  # noqa: BLE001
            r.log("catalyst calendar not read: %s" % str(e)[:160])
        try:
            d, w = kept("data/plumbing-stress.json")
            rows = len(d.get("raw_indicators") or {})
            verdict("plumbing", w, d.get("n_indicators") == rows and d.get("n_base_indicators") is not None, "header %s indicators (%s with data) against %d rows; base list %s" % (d.get("n_indicators"), d.get("n_with_data"), rows, d.get("n_base_indicators")))
        except Exception as e:  # noqa: BLE001
            r.log("plumbing not read: %s" % str(e)[:160])

        r.section("2. Each function since the deploy")
        for n in ENGINES:
            try:
                q = [{"Id": tag, "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m, "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 3600, "Stat": stat}}
                     for m, stat, tag in (("Invocations", "Sum", "i"), ("Errors", "Sum", "e"), ("Duration", "Average", "a"), ("Duration", "Maximum", "x"))]
                got = {x["Id"]: x.get("Values") or [] for x in cw.get_metric_data(MetricDataQueries=q, StartTime=DEPLOYED, EndTime=now)["MetricDataResults"]}
                runs = int(sum(got["i"]))
                r.log("%s | %d runs, %d errors, %.1f s a run on average, %.1f s at most" % (n.replace("justhodl-", ""), runs, int(sum(got["e"])), (sum(got["a"]) / len(got["a"]) / 1000.0) if got["a"] else 0.0, (max(got["x"]) / 1000.0) if got["x"] else 0.0))
                if sum(got["e"]):
                    wrong.append(n + " (errors)")
            except Exception as e:  # noqa: BLE001
                r.log("%s | metrics not read: %s" % (n, str(e)[:120]))

        r.kv(carry_the_change=", ".join(done) or "none", waiting_for_their_own_run=", ".join(waiting) or "none", wrong=", ".join(wrong) or "none")
        if wrong:
            r.fail("ran since the deploy without the change, or with errors: " + ", ".join(wrong))
            sys.exit(1)
        r.ok("%d carry the change, %d wait for their own run" % (len(done), len(waiting)))


if __name__ == "__main__":
    main()
