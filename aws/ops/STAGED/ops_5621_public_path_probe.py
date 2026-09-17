"""ops 5621 -- public-path probe (Claude, 2026-09-17). READ-ONLY. Direct lane only (STAGED never runs serially).

Grok reports "Carry 503", "liquidity-flow public packet still 16 Sep" and "auction labels unchanged". The worker
serves seven REVIEWED_HISTORY_KEYS (reviewed-artifacts.js) only when the JSON carries
public_history_review == "20260910.v1" and no raw diagnostics -- and no writer in the repo sets that marker.
This probe measures, for each key:
  1. what justhodl.ai/data/<key> answers right now (status, cache headers, body head)
  2. what S3 holds (LastModified, size, marker present?, ok/quality/generated_at)
  3. for auction-crisis-detector and liquidity-flow: last invocation / error counts (CloudWatch metrics, 24h)
     and the newest log stream (prefix-only listing) so a failed invoke is visible
No writes, no invokes.
"""
from __future__ import annotations

import json
import sys
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

REGION, PUB = "us-east-1", "justhodl-dashboard-live"
KEYS = ["data/carry-surface.json", "data/jh-fusion.json", "data/usd-funding.json", "data/floor-audit.json",
        "data/cascade-validation-log.json", "etf-flows/daily.json", "macro/regime.json",
        "data/auction-crisis.json", "data/liquidity-flow.json"]
FUNCTIONS = ["justhodl-carry-surface", "justhodl-auction-crisis-detector", "justhodl-auction-crisis-ai", "justhodl-liquidity-flow"]
NOW = datetime.now(timezone.utc)


def http(url):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5621", "Cache-Control": "no-cache"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            body = r.read(400)
            return r.status, dict(r.headers), body
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read(400)
    except Exception as e:  # noqa: BLE001
        return None, {}, str(e).encode()


def main():
    cfg = Config(read_timeout=60, retries={"max_attempts": 3})
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    with report("5621_public_path_probe") as r:
        r.heading("ops 5621 -- public path vs S3 for the reviewed artifacts (read-only)")
        r.section("1. justhodl.ai/data/<key> vs the S3 object")
        for key in KEYS:
            status, headers, body = http(f"https://justhodl.ai/{key}")
            try:
                h = s3.head_object(Bucket=PUB, Key=key)
                lm, size = h["LastModified"].strftime("%m-%d %H:%M"), h["ContentLength"]
                doc = json.loads(s3.get_object(Bucket=PUB, Key=key)["Body"].read()) if size < 5_000_000 else {}
                marker = doc.get("public_history_review") if isinstance(doc, dict) else None
                extra = {k: doc.get(k) for k in ("ok", "generated_at", "as_of", "status") if isinstance(doc, dict) and k in doc}
                q = doc.get("quality") if isinstance(doc, dict) else None
            except Exception as e:  # noqa: BLE001
                lm, size, marker, extra, q = "MISSING", 0, None, {"s3_error": str(e)[:80]}, None
            line = (f"{key}: edge HTTP {status} | S3 {lm} {size:,}B | marker={marker!r} | {json.dumps(extra, default=str)[:160]}"
                    + (f" | quality={json.dumps(q, default=str)[:120]}" if q else ""))
            (r.ok if status == 200 else r.warn)(line)
            if status != 200:
                r.log(f"    edge body: {body[:200]!r}")
        r.section("1b. Which raw-diagnostic fields does carry-surface.json still carry? (paths + types only, never values)")
        DIAG = {'body','raw_sample','raw_status','request_id','request_url','response','response_body','raw_response','headers',
                'exception','traceback','stack_trace','error_message','telegram_info','wss_broadcast_info'}
        ERR = {'error','err','report_error','error_code','fetch_err'}
        try:
            doc = json.loads(s3.get_object(Bucket=PUB, Key="data/carry-surface.json")["Body"].read())
            hits = []
            def walk(node, path):
                if isinstance(node, dict):
                    for k, v in node.items():
                        empty = v is None or v == "" or v is False or v == 0 or (isinstance(v, (list, dict)) and not v)
                        if (k in DIAG or k in ERR) and not empty:
                            hits.append(f"{path}.{k} ({type(v).__name__}, {len(v) if hasattr(v, '__len__') else 1})")
                        walk(v, f"{path}.{k}")
                elif isinstance(node, list):
                    for i, v in enumerate(node[:50]):
                        walk(v, f"{path}[{i}]")
            walk(doc, "$")
            r.kv(diagnostic_fields=len(hits))
            for h in hits[:25]:
                r.log("    " + h)
        except Exception as e:  # noqa: BLE001
            r.warn(f"scan failed: {str(e)[:120]}")
        r.section("2. Invocations / errors in the last 24h + newest log stream")
        for fn in FUNCTIONS:
            try:
                conf = lam.get_function_configuration(FunctionName=fn)
                mod = conf.get("LastModified")
            except Exception as e:  # noqa: BLE001
                r.warn(f"{fn}: get_function failed: {str(e)[:100]}"); continue
            stats = {}
            for metric in ("Invocations", "Errors"):
                try:
                    dp = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                                  StartTime=NOW - timedelta(hours=24), EndTime=NOW, Period=86400, Statistics=["Sum"]).get("Datapoints", [])
                    stats[metric] = int(sum(d["Sum"] for d in dp))
                except Exception as e:  # noqa: BLE001
                    stats[metric] = f"err {str(e)[:40]}"
            newest = "-"
            try:
                streams = logs.describe_log_streams(logGroupName=f"/aws/lambda/{fn}", logStreamNamePrefix=NOW.strftime("%Y/%m/%d"), limit=50).get("logStreams", [])
                if not streams:
                    streams = logs.describe_log_streams(logGroupName=f"/aws/lambda/{fn}", logStreamNamePrefix=(NOW - timedelta(days=1)).strftime("%Y/%m/%d"), limit=50).get("logStreams", [])
                if streams:
                    st = max(streams, key=lambda s: s.get("lastEventTimestamp", 0))
                    newest = datetime.fromtimestamp(st.get("lastEventTimestamp", 0) / 1000, tz=timezone.utc).strftime("%m-%d %H:%M")
                    ev = logs.get_log_events(logGroupName=f"/aws/lambda/{fn}", logStreamName=st["logStreamName"], limit=40, startFromHead=False).get("events", [])
                    tail = [e["message"].strip()[:160] for e in ev if any(w in e["message"] for w in ("Error", "Traceback", "ERROR", "Task timed out", "REPORT"))][-4:]
                else:
                    tail = []
            except Exception as e:  # noqa: BLE001
                tail = [f"logs: {str(e)[:100]}"]
            r.log(f"{fn}: code LastModified {mod} | 24h invocations={stats.get('Invocations')} errors={stats.get('Errors')} | newest log {newest}")
            for t in tail:
                r.log(f"    {t}")
        r.ok("probe complete (no writes, no invokes)")


if __name__ == "__main__":
    main()
