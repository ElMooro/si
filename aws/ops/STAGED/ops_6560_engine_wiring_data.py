"""ops 6560 - READ-ONLY data for the infrastructure review (Khalid, 2026-10-20: "review my entire system ... old,
stale, duplicated engines, engines not tied to the front end").

Collects the runtime side of what docs/audit/dependency-map.json knows statically:
  1. every output key an engine declares (2,067): does it exist on justhodl-dashboard-live, when was it last written,
     how big (HeadObject; ~2k calls)
  2. who actually reads what: a 15,000-object sample of one normal day's access logs (2026-10-10, x12) -> GET count
     per key, split anonymous (site readers) / IAM user (the runner) / Lambda (engines)
  3. API Gateway REST + HTTP integrations -> which functions serve an API (engines with no S3 output can still be live)
  4. Lambda last-modified / runtime / package size (which code is old)
Written gzip+base64 next to the report and to the private bucket.  Cost: a few thousand S3 reads (< $0.10).
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import base64
import concurrent.futures as cf
import gzip
import json
import os
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
BUCKET, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-857687956942"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, max_pool_connections=40)
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')
OUT = {"generated_at": datetime.now(timezone.utc).isoformat(), "errors": []}


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def who(req):
    if "assumed-role" in req and req.count("/") >= 2:
        return "lambda:" + req.split("/")[-1]
    if ":user/" in req:
        return "iam-user"
    if req == "-":
        return "anonymous"
    return "other"


def heads(log):
    dep = json.loads((ROOT / "docs/audit/dependency-map.json").read_text(encoding="utf-8"))
    keys = sorted(k for k in dep["writers"] if "{" not in k and not k.endswith("/"))
    s3 = c("s3")

    def one(k):
        try:
            h = s3.head_object(Bucket=BUCKET, Key=k)
            return k, {"exists": True, "last_modified": h["LastModified"].isoformat(), "bytes": h.get("ContentLength"),
                       "storage": h.get("StorageClass") or "STANDARD"}
        except Exception as e:  # noqa: BLE001
            code = getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__)
            return k, {"exists": False, "error": code}
    out = {}
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        for k, v in ex.map(one, keys):
            out[k] = v
    OUT["heads"] = out
    missing = sum(1 for v in out.values() if not v["exists"])
    log("heads: %d declared output keys, %d missing on S3" % (len(out), missing))


def access_sample(log, day="2026/10/10", n=15000):
    s3 = c("s3")
    lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
    tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
    base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET, day)
    keys, token = [], None
    while True:
        kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated") or len(keys) >= 800000:
            break
        token = resp.get("NextContinuationToken")
    step = max(1, len(keys) // n)
    sample = keys[::step][:n]
    scale = len(keys) / float(len(sample) or 1)

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""
    gets = Counter()
    puts = Counter()
    with cf.ThreadPoolExecutor(max_workers=40) as ex:
        for text in ex.map(read, sample):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m:
                    continue
                op, key, w = m.group(7), m.group(8), who(m.group(5))
                if op == "REST.GET.OBJECT":
                    gets[(key, "anon" if w == "anonymous" else "runner" if w == "iam-user" else "lambda")] += 1
                elif op in ("REST.PUT.OBJECT", "REST.POST.OBJECT", "REST.COPY.OBJECT"):
                    puts[(key, w)] += 1
    OUT["access"] = {"day": day, "log_objects": len(keys), "sampled": len(sample), "scale": round(scale, 2),
                     "gets": [[k, kind, int(v * scale)] for (k, kind), v in gets.most_common(20000)],
                     "puts": [[k, w, int(v * scale)] for (k, w), v in puts.most_common(20000)]}
    log("access logs %s: %d objects, sampled %d (x%.1f): %d distinct GET keys, %d distinct PUT keys" % (
        day, len(keys), len(sample), scale, len({k for k, _ in gets}), len({k for k, _ in puts})))


def apis(log):
    out = {"rest": [], "http": []}
    ag = c("apigateway")
    for api in ag.get_rest_apis(limit=500).get("items", []):
        fns = set()
        try:
            for res in ag.get_resources(restApiId=api["id"], limit=500).get("items", []):
                for method in (res.get("resourceMethods") or {}):
                    try:
                        integ = ag.get_integration(restApiId=api["id"], resourceId=res["id"], httpMethod=method)
                        m = re.search(r":function:([^/:]+)", integ.get("uri") or "")
                        if m:
                            fns.add(m.group(1))
                    except Exception:  # noqa: BLE001
                        pass
        except Exception as e:  # noqa: BLE001
            OUT["errors"].append("rest %s: %s" % (api.get("name"), type(e).__name__))
        out["rest"].append({"name": api.get("name"), "id": api["id"], "functions": sorted(fns)})
    v2 = c("apigatewayv2")
    for api in v2.get_apis(MaxResults="500").get("Items", []):
        fns = set()
        try:
            for integ in v2.get_integrations(ApiId=api["ApiId"], MaxResults="500").get("Items", []):
                m = re.search(r":function:([^/:]+)", integ.get("IntegrationUri") or "")
                if m:
                    fns.add(m.group(1))
        except Exception as e:  # noqa: BLE001
            OUT["errors"].append("http %s: %s" % (api.get("Name"), type(e).__name__))
        out["http"].append({"name": api.get("Name"), "id": api["ApiId"], "functions": sorted(fns), "endpoint": api.get("ApiEndpoint")})
    OUT["apis"] = out
    log("apis: %d REST, %d HTTP; functions behind an API: %d" % (
        len(out["rest"]), len(out["http"]), len({f for a in out["rest"] + out["http"] for f in a["functions"]})))


def lambdas(log):
    lam = c("lambda")
    fns = []
    for p in lam.get_paginator("list_functions").paginate():
        fns += p["Functions"]
    OUT["lambda"] = {f["FunctionName"]: {"last_modified": f.get("LastModified"), "runtime": f.get("Runtime"),
                                        "code_bytes": f.get("CodeSize"), "mem": f.get("MemorySize"), "timeout": f.get("Timeout"),
                                        "desc": (f.get("Description") or "")[:120]} for f in fns}
    log("lambda: %d functions" % len(fns))


def main():
    with report("ops_6560_engine_wiring_data") as r:
        r.heading("ops 6560 - engine wiring / freshness / consumption data (read-only)")
        for step in (heads, access_sample, apis, lambdas):
            try:
                step(r.log)
            except Exception as e:  # noqa: BLE001
                OUT["errors"].append("%s: %s" % (step.__name__, str(e)[:200]))
                r.warn("%s failed: %s" % (step.__name__, type(e).__name__))
        blob = gzip.compress(json.dumps(OUT, separators=(",", ":"), default=str).encode(), 9)
        c("s3").put_object(Bucket=PRIVATE, Key="audit/engine-wiring-%s.json.gz" % datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                           Body=blob, ServerSideEncryption="AES256")
        out = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws" / "ops" / "reports" / "latest" / "ops_6560_engine_wiring_data_data.md"
        b64 = base64.b64encode(blob).decode()
        out.write_text("# ops 6560 engine wiring data (gzip+base64 JSON)\n\n```\n" +
                       "\n".join(b64[i:i + 120] for i in range(0, len(b64), 120)) + "\n```\n", encoding="utf-8")
        r.log("data: %d KB compressed -> %s" % (len(blob) // 1024, out.name))
        r.ok("done (%d errors)" % len(OUT["errors"]))


if __name__ == "__main__":
    main()
