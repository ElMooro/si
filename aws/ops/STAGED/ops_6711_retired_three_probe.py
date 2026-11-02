"""ops 6711 - READ-ONLY: before turning macro-financial-intelligence, ofrapi and treasury-api back on (Khalid,
2026-11-02 11:08 ET: "turn these on ... first").  Held at reserved concurrency 0 under the governor's operator hold since
2026-10-20 (retire plan: "switch off the orphans and the broken writers"); the Oct 20 review called them broken because
their declared outputs were missing from justhodl-dashboard-live - but they write elsewhere: macro-financial-intelligence
-> $S3_BUCKET/latest_brief.json, ofrapi -> $S3_BUCKET (default macro-data-lake)/scraped_data/ofr/latest.json,
treasury-api -> openbb-lambda-data/treasury_historical_comprehensive.json.  Reads, per engine: role, env names (and the
non-secret S3_BUCKET value), destination bucket + output object age, every EventBridge rule that names it or once did
(ofr-daily-collection, ofr-weekly-report, treasury-auto-update, macro-financial-daily-execution), the live fan-out
manifest membership, API Gateway routes integrating it, and error lines in its logs Sep 15 - Oct 17.  Nothing changed.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=60)
FNS = {"macro-financial-intelligence": ["latest_brief.json"], "ofrapi": ["scraped_data/ofr/latest.json"],
       "treasury-api": ["treasury_historical_comprehensive.json"]}
DEFAULT_BUCKET = {"ofrapi": "macro-data-lake", "treasury-api": "openbb-lambda-data"}
RULES = ["macro-financial-daily-execution", "ofr-daily-collection", "ofr-weekly-report", "treasury-auto-update"]
APIS = ["6nl5fzfus7", "i1hgpjotq7", "klehdyiwrl"]


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    ev = boto3.client("events", region_name=REGION, config=CFG)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    apigw = boto3.client("apigateway", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6711_retired_three_probe") as r:
        r.heading("ops 6711 - the three Oct 20 switch-offs: where they write, what fires them, did they work (read-only)")
        r.section("A. engine, destination, output age")
        for fn, keys in FNS.items():
            c = lam.get_function_configuration(FunctionName=fn)
            env = (c.get("Environment") or {}).get("Variables") or {}
            bucket = env.get("S3_BUCKET") or DEFAULT_BUCKET.get(fn)
            r.log("%s: role %s | env names %s | S3_BUCKET=%s | handler %s | %s MB / %s s" % (
                fn, c["Role"].split("/")[-1], sorted(env), env.get("S3_BUCKET"), c["Handler"], c["MemorySize"], c["Timeout"]))
            for b in sorted({bucket, DEFAULT_BUCKET.get(fn) or bucket, "openbb-lambda-data" if fn == "treasury-api" else bucket}):
                try:
                    s3.head_bucket(Bucket=b)
                    exists = True
                except Exception as e:  # noqa: BLE001
                    exists = "NO (%s)" % getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
                r.log("   bucket %s exists: %s" % (b, exists))
                if exists is True:
                    for k in keys:
                        try:
                            h = s3.head_object(Bucket=b, Key=k)
                            r.log("   s3://%s/%s: %d B, written %s (%.1f d ago)" % (b, k, h["ContentLength"], h["LastModified"].isoformat()[:16],
                                                                             (now - h["LastModified"]).total_seconds() / 86400))
                        except Exception as e:  # noqa: BLE001
                            r.log("   s3://%s/%s: %s" % (b, k, getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)))
                    if fn == "ofrapi":
                        objs = s3.list_objects_v2(Bucket=b, Prefix="scraped_data/ofr/", MaxKeys=1000).get("Contents", [])
                        objs.sort(key=lambda o: o["LastModified"])
                        r.log("   scraped_data/ofr/: %d objects, newest %s" % (len(objs), [(o["Key"], o["LastModified"].isoformat()[:16]) for o in objs[-3:]]))
            for k in keys:
                try:
                    h = s3.head_object(Bucket=LIVE, Key=k)
                    r.log("   (live bucket %s: %s)" % (k, h["LastModified"].isoformat()[:16]))
                except Exception:  # noqa: BLE001
                    pass
        r.section("B. triggers: the rules that name them, the live fan-out manifest")
        for name in RULES:
            try:
                d = ev.describe_rule(Name=name)
                t = ev.list_targets_by_rule(Rule=name).get("Targets", [])
                r.log("rule %s [%s] %s | targets %s" % (name, d.get("State"), d.get("ScheduleExpression"),
                                                        [(x["Arn"].split(":")[-1], bool(x.get("Input"))) for x in t]))
            except Exception as e:  # noqa: BLE001
                r.log("rule %s: %s" % (name, getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)))
        fan = json.loads(s3.get_object(Bucket=LIVE, Key="config/fanout-manifest.json")["Body"].read())
        for fn in list(FNS) + ["treasury-auto-updater"]:
            r.log("live fan-out: %s in ticks %s | assignment %s" % (fn, [t for t, fs in fan["ticks"].items() if fn in fs], fan.get("assignment", {}).get(fn)))
        r.log("live fan-out retired blocks: %s" % {k: v.get("removed") for k, v in fan.items() if k.startswith("retired_")})
        r.section("C. API Gateway routes that integrate them")
        for api in APIS:
            try:
                info = apigw.get_rest_api(restApiId=api)
                res = apigw.get_resources(restApiId=api, limit=500, embed=["methods"]).get("items", [])
                hits = []
                for it in res:
                    for m, meth in (it.get("resourceMethods") or {}).items():
                        uri = ((meth.get("methodIntegration") or {}).get("uri") or "")
                        if any(fn in uri for fn in FNS):
                            hits.append("%s %s -> %s" % (m, it.get("path"), [fn for fn in FNS if fn in uri][0]))
                r.log("api %s (%s, created %s): %d resources; routes to the three: %s" % (api, info.get("name"), str(info.get("createdDate"))[:10], len(res), hits[:12]))
            except Exception as e:  # noqa: BLE001
                r.log("api %s: %s" % (api, getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)))
        r.section("D. did they work before the hold?  errors Sep 15 - Oct 17")
        start = int(datetime(2026, 9, 15, tzinfo=timezone.utc).timestamp() * 1000)
        end = int(datetime(2026, 10, 17, tzinfo=timezone.utc).timestamp() * 1000)
        for fn in FNS:
            evs, tok = [], None
            while len(evs) < 2000:
                kw = dict(logGroupName="/aws/lambda/" + fn, startTime=start, endTime=end,
                          filterPattern='?ERROR ?Error ?Traceback ?Exception ?"Task timed out" ?error ?AccessDenied')
                if tok:
                    kw["nextToken"] = tok
                try:
                    resp = logs.filter_log_events(**kw)
                except Exception as e:  # noqa: BLE001
                    r.log("%s logs: %s" % (fn, type(e).__name__))
                    break
                evs += resp.get("events", [])
                tok = resp.get("nextToken")
                if not tok:
                    break
            r.log("%s: %d error-looking lines" % (fn, len(evs)))
            seen = set()
            for e in evs:
                msg = e["message"].strip().replace("\n", " | ")[:300]
                sig = msg[:80]
                if sig in seen:
                    continue
                seen.add(sig)
                r.log("   %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).isoformat()[:16], msg))
                if len(seen) >= 6:
                    break
            try:
                tail = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=end - 3 * 86400000, endTime=end, limit=12).get("events", [])
                r.log("   last lines before the hold: %s" % [x["message"].strip()[:160] for x in tail[-6:]])
            except Exception:  # noqa: BLE001
                pass
        r.ok("ops 6711 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
