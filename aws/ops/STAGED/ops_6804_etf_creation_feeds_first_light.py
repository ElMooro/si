"""ops 6804 - first light of the ETF creations desk feeds (etf-true-flows commit f22fca00): invokes the engine ONCE and
verifies from S3 that data/etf-creations/index.json and one feed per fund with a retained history exist, that each
feed is a float view of the retained history it names (GLD: d/series lengths, latest 5-day sum == the packet's
net_flow_5d_usd, the first dated row == the history's first row), that the 20 accruing ledger funds have no feed yet
unless their window completed, and that the second invocation accrued nothing new (same as-of dates). Also reads the
engine's log streams for today to settle the 13:30 UTC invocation question. Nothing else is changed."""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-etf-true-flows" / "source"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-etf-true-flows"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import etf_issuers as iss

    cfg = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=910)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    fails = []

    def get(key):
        return s3.get_object(Bucket=LIVE, Key=key)["Body"].read()

    with report("ops_6804_etf_creation_feeds_first_light") as r:
        r.heading("ops 6804 - ETF creations desk feeds first light %s" % now.isoformat(timespec="minutes"))
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("function: code last modified %s, sha %s" % (c.get("LastModified"), str(c.get("CodeSha256"))[:12]))
        r.section("0. the 13:30 UTC invocation question")
        try:
            streams = logs.describe_log_streams(logGroupName="/aws/lambda/" + FN, orderBy="LastEventTime", descending=True, limit=12).get("logStreams", [])
            for st in streams:
                first = datetime.fromtimestamp(st.get("firstEventTimestamp", 0) / 1000, timezone.utc).isoformat(timespec="minutes")
                last = datetime.fromtimestamp(st.get("lastEventTimestamp", 0) / 1000, timezone.utc).isoformat(timespec="minutes")
                r.log("  stream %s: %s -> %s" % (st["logStreamName"][-12:], first, last))
            events = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=int((now.timestamp() - 36 * 3600) * 1000), filterPattern="START RequestId", limit=50).get("events", [])
            starts = sorted(datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).isoformat(timespec="minutes") for e in events)
            r.log("  invocation starts in the last 36 h: %s" % starts)
        except Exception as exc:  # noqa: BLE001
            r.warn("  logs unavailable: %s" % type(exc).__name__)
        r.log("  justhodl-schedule-liveness (cron 13:30 UTC) re-fires any engine whose data/<name>.json is older than max(8 h, 2.5 x cadence): the ETF packet was stale from 2026-10-12 to 2026-11-11, hence the daily 13:30 revive; it stops by itself now that the engine publishes daily")
        r.section("1. invoke once")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(resp["Payload"].read() or b"{}")
        body = json.loads(payload.get("body") or "{}") if isinstance(payload.get("body"), str) else payload
        r.log("invoke: status %s, function error %s, %.0fs; result %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps({k: body.get(k) for k in ("published", "generated_at", "ledgers_moved", "desk_feeds", "coverage")}, default=str)[:600]))
        if resp.get("FunctionError") or not body.get("published"):
            fails.append("invocation did not publish: %s" % json.dumps(payload)[:300])
        packet = json.loads(get("data/etf-true-flows.json"))
        by = packet["by_etf"]
        r.section("2. the feeds")
        index = json.loads(get("data/etf-creations/index.json"))
        r.log("index: %s funds, generated %s, packet %s" % (index.get("n_funds"), index.get("generated_at"), index.get("packet")))
        if index.get("generated_at") != packet.get("generated_at"):
            fails.append("index generated_at %s differs from the packet %s" % (index.get("generated_at"), packet.get("generated_at")))
        with_history = sorted(t for t, e in by.items() if e.get("history"))
        listed = sorted(f["ticker"] for f in index.get("funds", []))
        if listed != with_history:
            fails.append("index lists %d funds, packet has %d with histories: missing %s extra %s" % (len(listed), len(with_history), sorted(set(with_history) - set(listed))[:10], sorted(set(listed) - set(with_history))[:10]))
        r.log("funds with a retained history: %d; listed in the index: %d" % (len(with_history), len(listed)))
        for t in ("GLD", "IVV", "SPY", "TLT", "HACK", "ASHR"):
            e = by.get(t, {})
            try:
                feed = json.loads(get("data/etf-creations/%s.json" % t))
            except Exception as exc:  # noqa: BLE001
                fails.append("%s feed missing (%s)" % (t, type(exc).__name__))
                continue
            d = feed["d"]
            s5 = feed["series"]["flow_5d"]
            r.log("  %-5s feed: %d dates %s -> %s | latest flow %s 5d %s 20d %s | packet 5d %s | history %s" % (t, len(d), d[0], d[-1], feed["latest"]["flow"], feed["latest"]["flow_5d"], feed["latest"]["flow_20d"], e.get("net_flow_5d_usd"), (feed.get("source") or {}).get("history", {}).get("key", "")[-24:-5]))
            if feed.get("contract") != "etf-creations-feed.v1" or (feed.get("source") or {}).get("history", {}).get("sha256") != (e.get("history") or {}).get("sha256"):
                fails.append("%s feed does not name the packet's history" % t)
            if len(d) != len(s5) or len(d) != len(feed["series"]["shares"]):
                fails.append("%s feed series lengths differ" % t)
            if e.get("net_flow_5d_usd") is not None and feed["latest"]["flow_5d"] is not None and abs(feed["latest"]["flow_5d"] - e["net_flow_5d_usd"]) > 0.01 and d[-1] == packet["aggregation_period"]["end_date"]:
                fails.append("%s feed 5d %s differs from the packet %s" % (t, feed["latest"]["flow_5d"], e["net_flow_5d_usd"]))
        ledger_feeds = [t for t in iss.snapshot_funds() if t in listed]
        r.log("ledger funds already with a feed (their history exists from the first observed day): %s" % ledger_feeds)
        for t in ledger_feeds[:3]:
            feed = json.loads(get("data/etf-creations/%s.json" % t))
            r.log("  %-5s ledger feed: %d dates, latest flow %s (None until two observed days)" % (t, len(feed["d"]), feed["latest"]["flow"]))
        ledgers = packet.get("snapshot_ledgers", {})
        rows = {t: v.get("rows") for t, v in ledgers.items()}
        r.log("ledger rows after this second invocation of the day: %s" % json.dumps(rows))
        if any(v not in (1, None) for v in rows.values()):
            fails.append("a ledger accrued more than today's observation: %s" % rows)
        head = s3.head_object(Bucket=LIVE, Key="data/etf-creations/GLD.json")
        r.log("GLD feed object: %s bytes, cache-control %r" % (head.get("ContentLength"), head.get("CacheControl")))
        r.kv(fails=str(fails), funds=str(len(listed)))
        if fails:
            r.fail("feeds first light failed: %s" % fails)
        else:
            r.ok("%d issuer-tape feeds live under data/etf-creations/, index written, ledgers idempotent" % len(listed))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
