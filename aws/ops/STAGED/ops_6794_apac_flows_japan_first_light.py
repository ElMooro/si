"""ops 6794 - first light for justhodl-apac-flows 1.1.0 (commit f9420c74): invokes the engine ONCE synchronously
(its own slot is 09:30 UTC) and verifies from S3 that Japan is LIVE again - data/apac-flows.json japan.status LIVE
with the newest JPX week (2026-11-06, TSE Prime foreigners -81.762 JPY bn per the reviewed workbook), and
data/history/apac-flows.json now carries jp_weeks (the ~5 weeks JPX lists) and today's Korea nets (KOSPI / KOSDAQ
foreign / institution / individual, KRW billions) beside Taiwan. One invocation; nothing else changed."""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-apac-flows"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=360)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    fails, facts = [], {}
    with report("ops_6794_apac_flows_japan_first_light") as r:
        r.heading("ops 6794 - apac-flows 1.1.0 first light (Japan weekly + Korea daily) %s" % now.isoformat(timespec="minutes"))
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("function: code last modified %s, sha %s, timeout %ss" % (c.get("LastModified"), str(c.get("CodeSha256"))[:12], c.get("Timeout")))
        if str(c.get("LastModified", ""))[:10] != now.date().isoformat():
            fails.append("the deployed code is not today's (LastModified %s)" % c.get("LastModified"))
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(resp["Payload"].read() or b"{}")
        r.log("invoke: status %s, function error %s, %.0fs, payload %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(payload, default=str)[:400]))
        if resp.get("FunctionError"):
            fails.append("function error")
        doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/apac-flows.json")["Body"].read())
        jp = doc.get("japan") or {}
        r.log("apac-flows.json: version %s, generated %s | japan status %s week_ending %s foreign_net %s individual_net %s investment_trusts %s trust_banks %s | files %s | err %s" % (
            doc.get("version"), doc.get("generated_at") or doc.get("as_of"), jp.get("status"), jp.get("week_ending"), jp.get("foreign_net"), jp.get("individual_net"),
            jp.get("investment_trusts_net"), jp.get("trust_banks_net"), jp.get("new_files"), jp.get("err") or jp.get("note")))
        kr = (doc.get("korea") or {}).get("market_wide") or {}
        r.log("korea market_wide: %s" % json.dumps(kr)[:400])
        if jp.get("status") != "LIVE":
            fails.append("japan not LIVE: %s" % (jp.get("err") or jp.get("note")))
        if jp.get("week_ending") and jp["week_ending"] == "2026-11-06" and jp.get("foreign_net") != -81.762:
            fails.append("TSE Prime foreigners net for the 2026-11-06 week differs from the reviewed workbook: %s" % jp.get("foreign_net"))
        hist = json.loads(s3.get_object(Bucket=LIVE, Key="data/history/apac-flows.json")["Body"].read())
        weeks = hist.get("jp_weeks") or {}
        days = hist.get("days") or {}
        today = days.get(now.date().isoformat()) or {}
        r.log("history: %d days (%s -> %s) | jp_weeks %d: %s | jp_unit %s" % (len(days), min(days) if days else None, max(days) if days else None, len(weeks), sorted(weeks), hist.get("jp_unit")))
        r.log("today's day row: %s" % json.dumps({k: v for k, v in today.items() if not k.startswith("tw_")})[:600])
        if len(weeks) < 4:
            fails.append("fewer than 4 Japan weeks stored: %s" % sorted(weeks))
        if not all(k in today for k in ("kr_kospi_foreign_net_krw_bn", "kr_kosdaq_foreign_net_krw_bn", "kr_foreign_net_krw_bn")):
            fails.append("Korea daily nets missing from today's history row")
        elif today.get("kr_foreign_net_krw_bn") is None:
            fails.append("Korea foreign net is null today (Naver unreachable?)")
        latest_week = max(weeks) if weeks else None
        prime = ((weeks.get(latest_week) or {}).get("markets") or {}).get("prime") or {}
        r.log("latest week %s TSE Prime nets (JPY bn): %s" % (latest_week, json.dumps(prime)[:500]))
        facts.update({"japan": "%s %s foreign %s" % (jp.get("status"), jp.get("week_ending"), jp.get("foreign_net")), "jp_weeks": len(weeks), "korea_today": today.get("kr_foreign_net_krw_bn")})
        r.kv(**{k: str(v) for k, v in facts.items()}, fails=str(fails))
        if fails:
            r.fail("first light failed: %s" % fails)
        else:
            r.ok("Japan weekly flows LIVE (%d weeks stored), Korea daily nets stored" % len(weeks))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
