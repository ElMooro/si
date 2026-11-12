"""ops 6802 - first light for justhodl-etf-true-flows 3.1.0 (issuer registry): invokes the engine ONCE synchronously
and verifies from S3 that the publication is the registry build - version 3.1.0, coverage with issuer histories (GLD,
ASHR, HACK) and accruing snapshot ledgers (Global X, VanEck, Invesco, USCF, First Trust, ROBO), the reviewed limits,
the ledger pointers written for every fund whose site answered, each pointed ledger byte-verified and its newest row
traceable to a retained original, and the replay manifest of the run. Nothing else is changed."""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import hashlib
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
    now = datetime.now(timezone.utc)
    fails = []

    def get(key):
        return s3.get_object(Bucket=LIVE, Key=key)["Body"].read()

    with report("ops_6802_etf_registry_first_light") as r:
        r.heading("ops 6802 - etf-true-flows 3.1.0 first light (issuer registry) %s" % now.isoformat(timespec="minutes"))
        c = lam.get_function_configuration(FunctionName=FN)
        r.log("function: code last modified %s, sha %s, timeout %ss, memory %s" % (c.get("LastModified"), str(c.get("CodeSha256"))[:12], c.get("Timeout"), c.get("MemorySize")))
        if str(c.get("LastModified", ""))[:10] != now.date().isoformat():
            fails.append("the deployed code is not today's (LastModified %s)" % c.get("LastModified"))
        before = json.loads(get("data/etf-true-flows.json"))
        r.log("before: version %s generated %s gaps %d" % (before.get("version"), before.get("generated_at"), len(before.get("gaps", []))))
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(resp["Payload"].read() or b"{}")
        body = json.loads(payload.get("body") or "{}") if isinstance(payload.get("body"), str) else payload
        r.log("invoke: status %s, function error %s, %.0fs" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0))
        r.log("result: %s" % json.dumps({k: body.get(k) for k in ("published", "generated_at", "ledgers_moved", "coverage", "history_shards", "quality")}, default=str)[:900])
        codes = body.get("source_status_codes") or {}
        r.log("source status codes (%d): %s" % (len(codes), json.dumps({k: v for k, v in codes.items() if not k.startswith("vendor_")})[:1200]))
        if resp.get("FunctionError") or payload.get("statusCode") not in (200, None):
            fails.append("function error or non-200: %s" % json.dumps(payload)[:300])
        packet = json.loads(get("data/etf-true-flows.json"))
        r.section("1. publication")
        r.log("version %s generated %s | quality %s" % (packet.get("version"), packet.get("generated_at"), json.dumps(packet.get("quality"))))
        r.log("coverage %s" % json.dumps(packet.get("coverage")))
        gaps = packet.get("gaps", [])
        r.log("gaps (%d): %s" % (len(gaps), " ".join(gaps)))
        r.log("nav_source_counts %s" % json.dumps(packet.get("nav_source_counts")))
        if packet.get("version") != "3.1.0":
            fails.append("published version is %s" % packet.get("version"))
        if packet.get("generated_at") != body.get("generated_at"):
            fails.append("the publication is not this invocation's (published %s, invoked %s)" % (packet.get("generated_at"), body.get("generated_at")))
        by = packet.get("by_etf", {})
        for t in iss.history_funds():
            e = by.get(t, {})
            r.log("  %-5s %-32s 5d %s 20d %s obs %s first %s latest %s quality %s" % (t, e.get("source_status"), e.get("net_flow_5d_usd"), e.get("net_flow_20d_usd"), e.get("history_observations"), e.get("history_first_date"), e.get("observation_date"), (e.get("quality") or {}).get("status")))
            if e.get("source_status") != "retained_native_history":
                fails.append("%s not retained: %s" % (t, e.get("source_error")))
        for t in iss.snapshot_funds():
            e = by.get(t, {})
            lg = e.get("ledger") or {}
            r.log("  %-5s %-32s ledger rows %s latest %s observed_today %s status %s nav %s shares %s" % (t, e.get("source_status"), lg.get("rows"), lg.get("latest_date"), lg.get("observed_today"), lg.get("status"), e.get("nav"), e.get("shares_outstanding")))
            if lg.get("status") != "accruing":
                fails.append("%s ledger not accruing: %s / %s" % (t, lg.get("status"), e.get("source_error")))
        limits = packet.get("reviewed_limits", {})
        r.log("reviewed limits (%d): %s" % (len(limits), json.dumps(limits)))
        for t in gaps:
            e = by[t]
            if t not in limits and (e.get("ledger") or {}).get("status") != "accruing" and t not in ("MUB",):
                r.warn("  gap %s is neither a reviewed limit nor an accruing ledger: %s" % (t, json.dumps(e.get("source_error"))))
        r.section("2. ledger pointers and traceability")
        pointed = 0
        for t in iss.snapshot_funds():
            key = "data/etf-research/ledgers/%s/current.json" % t
            try:
                pointer = json.loads(get(key))
            except Exception as exc:  # noqa: BLE001
                r.warn("  %-5s no pointer (%s)" % (t, type(exc).__name__))
                continue
            ledger_raw = get(pointer["key"])
            ok = hashlib.sha256(ledger_raw).hexdigest() == pointer["sha256"] == pointer["key"].rsplit("/", 1)[-1][:-5]
            ledger = json.loads(ledger_raw)
            last = ledger["rows"][-1]
            src = last["source"][0]
            evidence = gzip.decompress(get(src["key"]))
            traced = hashlib.sha256(evidence).hexdigest() == src["sha256"]
            pointed += 1
            r.log("  %-5s pointer rows %s latest %s -> ledger %s bytes verified %s | newest row %s nav %s shares %s from %s (%d B, sha verified %s)" % (t, pointer["rows"], pointer.get("latest_date"), pointer["key"][-20:-5], ok, last["date"], last["nav_decimal"], last["shares_decimal"], src["url"][:60], len(evidence), traced))
            if not ok or not traced or ledger.get("contract") != iss.LEDGER_CONTRACT or ledger.get("ticker") != t:
                fails.append("%s ledger integrity" % t)
        r.log("pointers present: %d of %d" % (pointed, len(iss.snapshot_funds())))
        replay = packet.get("replay") or {}
        manifest = json.loads(get(replay["manifest_key"]))
        r.log("replay manifest %s: compilers %s, output sha %s" % (replay["manifest_key"][-20:-5], sorted(manifest.get("compilers", {})), manifest.get("output_sha256", "")[:12]))
        if "etf_issuers" not in manifest.get("compilers", {}):
            fails.append("the run manifest does not list the registry compiler")
        inputs = json.loads(get(manifest["input"]["key"]))
        r.log("inputs: %d originals, %d ledger references, acquisition errors %s" % (len(inputs["originals"]), len(inputs.get("ledgers", {})), json.dumps({k: v for k, v in inputs["acquisition_errors"].items() if not k.startswith("vendor_")})[:600]))
        leaked = [k for k, v in inputs["originals"].items() if "key=" in v["url"] or "Bearer" in json.dumps(v)]
        if leaked:
            fails.append("web key in retained references: %s" % leaked)
        r.log("web keys absent from every retained reference: %s" % (not leaked))
        r.kv(version=str(packet.get("version")), gaps=str(len(gaps)), pointers=str(pointed), fails=str(fails))
        if fails:
            r.fail("first light failed: %s" % fails)
        else:
            r.ok("registry live: %d gaps (was %d), %d ledgers accruing, histories for %s" % (len(gaps), len(before.get("gaps", [])), pointed, " ".join(iss.history_funds())))
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    main()
