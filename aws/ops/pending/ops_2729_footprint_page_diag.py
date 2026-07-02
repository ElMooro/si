"""ops 2729 — SURVEILLANCE DESK feed-unavailable: full-chain diagnosis + remediation.

Khalid reports institutional-footprint.html gate-fails ("Feed unavailable").
Working pages use identical relative fetch and the /data/* zone route is
generic (gfd key is 24h old and serves). Runner probes every hop and
remediates in-flight: (a) S3 object truth via boto3 + public URL; (b) domain
URL status/content-type/body-head + cf-cache-status; (c) deployed page HTML
vs repo (stale Pages?); (d) if object stale/missing -> re-invoke engine;
(e) if domain serves error while S3 serves JSON -> re-put with cache-bust
headers and re-probe. Report: aws/ops/reports/2729_footprint_page_diag.json.
"""
import os, json, time, urllib.request, urllib.error
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
KEY = "data/institutional-footprint.json"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=290, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2729, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 jh-diag", "Cache-Control": "no-cache", "Pragma": "no-cache"}

def probe(url, label):
    out = {"url": url}
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=25) as r:
            body = r.read()
            out.update(status=r.status, ctype=r.headers.get("Content-Type"),
                       clen=len(body), cf_cache=r.headers.get("CF-Cache-Status"),
                       age=r.headers.get("Age"), head=body[:220].decode("utf-8", "ignore"))
    except urllib.error.HTTPError as e:
        out.update(status=e.code, ctype=e.headers.get("Content-Type"),
                   cf_cache=e.headers.get("CF-Cache-Status"),
                   head=(e.read() or b"")[:220].decode("utf-8", "ignore"))
    except Exception as e:
        out.update(status=None, err=str(e)[:120])
    print("  [%s] %s ct=%s cf=%s | %s" % (label, out.get("status"), out.get("ctype"),
          out.get("cf_cache"), (out.get("head") or out.get("err", ""))[:130].replace("\n", " ")))
    return out

print("== 1) S3 object truth (boto3) ==")
try:
    h = s3.head_object(Bucket=BUCKET, Key=KEY)
    body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    doc = json.loads(body)
    R["s3_obj"] = {"exists": True, "bytes": len(body), "last_modified": str(h["LastModified"]),
                   "ctype": h.get("ContentType"), "cache_control": h.get("CacheControl"),
                   "top_keys": sorted(doc.keys())[:12], "has_posture": bool(doc.get("posture")),
                   "version": doc.get("version"), "generated_at": doc.get("generated_at")}
except Exception as e:
    R["s3_obj"] = {"exists": False, "err": str(e)[:120]}
print("  ", json.dumps(R["s3_obj"], default=str)[:340])

print("== 2) HTTP chain ==")
R["p_domain"] = probe("https://justhodl.ai/" + KEY, "domain")
R["p_s3"] = probe("https://%s.s3.amazonaws.com/%s" % (BUCKET, KEY), "s3-url")
R["p_proxy"] = probe("https://justhodl-data-proxy.raafouis.workers.dev/institutional-footprint.json", "workers.dev")
R["p_gfd"] = probe("https://justhodl.ai/data/global-flow-desk.json", "gfd-contrast")

print("== 3) deployed page vs repo ==")
pg = probe("https://justhodl.ai/institutional-footprint.html", "page")
R["page_deployed"] = {"status": pg.get("status"),
                      "has_asset_ledger": "ASSET LEDGER" in (pg.get("head") or "")}
try:
    req = urllib.request.Request("https://justhodl.ai/institutional-footprint.html", headers=UA)
    with urllib.request.urlopen(req, timeout=25) as r:
        html = r.read().decode("utf-8", "ignore")
    R["page_deployed"].update(bytes=len(html), has_marker="ASSET LEDGER" in html,
                              fetch_line=("data/institutional-footprint.json" in html))
    print("   page bytes=%s marker=%s fetch-line=%s" % (len(html), R["page_deployed"]["has_marker"], R["page_deployed"]["fetch_line"]))
except Exception as e:
    print("   page read err", str(e)[:80])

print("== 4) remediation branches ==")
if not R["s3_obj"].get("exists") or not R["s3_obj"].get("has_posture"):
    print("  -> object missing/malformed: re-invoking engine")
    r = lam.invoke(FunctionName="justhodl-institutional-footprint", InvocationType="RequestResponse")
    print("     invoke:", (r["Payload"].read() or b"")[:160])
    R["reinvoked"] = True
dom, s3u = R["p_domain"], R["p_s3"]
if R["s3_obj"].get("exists") and (dom.get("status") != 200 or "json" not in str(dom.get("ctype", ""))):
    print("  -> domain serving error while S3 healthy: cache-busting re-put")
    body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=body, ContentType="application/json",
                  CacheControl="public, max-age=120")
    R["reput"] = True
    time.sleep(8)
    R["p_domain_after"] = probe("https://justhodl.ai/" + KEY + "?cb=%d" % int(time.time()), "domain-after")

print("== 5) verdict ==")
final = R.get("p_domain_after") or R["p_domain"]
ok_feed = final.get("status") == 200 and "json" in str(final.get("ctype", "")) and '"posture"' in (final.get("head", "") + "")
R["verdict"] = ("FEED_SERVES_JSON" if final.get("status") == 200 and "json" in str(final.get("ctype", ""))
                else "S3_OBJECT_MISSING" if not R["s3_obj"].get("exists")
                else "ROUTE_OR_CACHE_FAULT status=%s ct=%s cf=%s" % (final.get("status"), final.get("ctype"), final.get("cf_cache")))
print("  VERDICT:", R["verdict"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2729_footprint_page_diag.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2729 COMPLETE (diagnostic — verdict drives the fix)")
