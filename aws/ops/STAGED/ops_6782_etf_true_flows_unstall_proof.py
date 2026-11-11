"""ops 6782 - un-stall proof for justhodl-etf-true-flows 3.0.1 (commit b5ff40ff, deployed 2026-11-11 06:12 UTC): invokes
the function ONCE synchronously (the next scheduled run is 15:45 UTC; Khalid runs nothing locally) and verifies from
S3 that it published again after 30 days: data/etf-true-flows.json LastModified moves, generated_at is this run,
contract etf-original-research.v1, quality / coverage, TLT (the fund whose percent-escaped page slug broke every run
since 2026-10-12) back with a native history, the vendor-context breaker's verdict in source_status_codes, the new
run manifest under data/etf-research/runs/, and the invocation's duration. One invocation; nothing else changed.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-etf-true-flows"
OUT = "data/etf-true-flows.json"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    cfg = Config(retries={"max_attempts": 2, "mode": "standard"}, connect_timeout=10, read_timeout=960)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    facts = {}
    with report("ops_6782_etf_true_flows_unstall_proof") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6782 - justhodl-etf-true-flows 3.0.1 un-stall proof %s" % now.isoformat(timespec="minutes"))
        before = s3.head_object(Bucket=LIVE, Key=OUT)
        r.log("before: %s LastModified %s (%d B)" % (OUT, before["LastModified"].isoformat(timespec="minutes"), before["ContentLength"]))
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("function: code last modified %s, sha %s, timeout %ss" % (c.get("LastModified"), str(c.get("CodeSha256"))[:12], c.get("Timeout")))
        runs_before = sum(1 for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/etf-research/runs/") for _ in page.get("Contents", []))
        r.section("invoke once (RequestResponse)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail", Payload=b"{}")
        took = time.time() - t0
        payload = json.loads(resp["Payload"].read() or b"{}")
        r.log("invoke: status %s, function error %s, %.0fs" % (resp.get("StatusCode"), resp.get("FunctionError"), took))
        body = payload.get("body")
        try:
            body = json.loads(body) if isinstance(body, str) else body
        except Exception:  # noqa: BLE001
            pass
        r.log("response: statusCode %s, body %s" % (payload.get("statusCode"), json.dumps(body, default=str)[:900]))
        facts["invoke_seconds"] = round(took)
        facts["statusCode"] = payload.get("statusCode")
        if isinstance(body, dict):
            facts["published"] = body.get("published")
            facts["quality"] = json.dumps(body.get("quality"))[:200]
            codes = body.get("source_status_codes") or {}
            facts["vendor_context"] = codes.get("vendor_context")
            facts["identity_defects"] = json.dumps({k: v for k, v in codes.items() if "_identity" in k})[:200]
            r.log("source_status_codes: %d keys; non-vendor: %s; vendor 429: %d; vendor skipped: %d" % (
                len(codes), json.dumps({k: v for k, v in codes.items() if not k.startswith("vendor_")})[:500],
                sum(1 for k, v in codes.items() if k.startswith("vendor_") and v == "HTTP_429"), sum(1 for k, v in codes.items() if v == "RateLimitedSkip")))
        r.section("verify from S3")
        after = s3.head_object(Bucket=LIVE, Key=OUT)
        moved = after["LastModified"] > before["LastModified"]
        r.log("after: %s LastModified %s (%d B) -> %s" % (OUT, after["LastModified"].isoformat(timespec="minutes"), after["ContentLength"], "MOVED" if moved else "unchanged"))
        facts["output_moved"] = moved
        doc = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        q = doc.get("quality") or {}
        r.log("payload: contract %s | generated_at %s | reference latest %s | quality %s | native_histories %s/%s | aligned %s | gaps %d" % (
            doc.get("contract"), doc.get("generated_at"), (doc.get("reference_calendar") or {}).get("latest_date"), q.get("status"),
            q.get("native_histories"), q.get("configured_funds"), q.get("aligned_five_observation_estimates"), len(doc.get("gaps") or [])))
        facts["generated_at"] = str(doc.get("generated_at"))[:19]
        facts["native_histories"] = "%s/%s" % (q.get("native_histories"), q.get("configured_funds"))
        tlt = (doc.get("by_etf") or {}).get("TLT") or {}
        r.log("TLT: source_status %s | nav_source %s | observation_date %s | nav %s | shares %s | net_flow_5d_usd %s | history %s | source_error %s" % (
            tlt.get("source_status"), tlt.get("nav_source"), tlt.get("observation_date"), tlt.get("nav"), tlt.get("shares_outstanding"),
            tlt.get("net_flow_5d_usd"), (tlt.get("history") or {}).get("key", "-")[-20:], tlt.get("source_error")))
        facts["tlt"] = "%s %s flow5d %s" % (tlt.get("source_status"), tlt.get("observation_date"), tlt.get("net_flow_5d_usd"))
        ivv = (doc.get("by_etf") or {}).get("IVV") or {}
        r.log("IVV: %s obs %s net_flow_5d_usd %s | top inflows %s | top outflows %s" % (
            ivv.get("source_status"), ivv.get("observation_date"), ivv.get("net_flow_5d_usd"),
            [(x.get("ticker"), x.get("net_flow_5d_usd")) for x in (doc.get("inflows") or [])[:5]], [(x.get("ticker"), x.get("net_flow_5d_usd")) for x in (doc.get("outflows") or [])[:5]]))
        runs_after = sum(1 for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/etf-research/runs/") for _ in page.get("Contents", []))
        r.log("run manifests: %d -> %d; replay ref %s" % (runs_before, runs_after, json.dumps(doc.get("replay"))[:200]))
        facts["run_manifests"] = "%d -> %d" % (runs_before, runs_after)
        ok = moved and doc.get("contract") == "etf-original-research.v1" and str(doc.get("generated_at", ""))[:10] == now.date().isoformat() and tlt.get("history")
        r.kv(**{k: str(v) for k, v in facts.items()})
        if ok:
            r.ok("un-stalled: a fresh etf-original-research.v1 packet published today with TLT's native history back")
        else:
            r.fail("not proven: moved=%s contract=%s generated=%s tlt_history=%s" % (moved, doc.get("contract"), doc.get("generated_at"), bool(tlt.get("history"))))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
