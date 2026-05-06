"""Phase Z1 — Audit current website state.
Inventory all HTML pages on S3 + cross-reference with all 17 data feeds.
Find gaps to fix."""
import io, json, os, time, base64, zipfile
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

REPORT = []
def log(m):
    print("- `" + time.strftime("%H:%M:%S") + "`   " + m)
    REPORT.append("- `" + time.strftime("%H:%M:%S") + "`   " + m)
def section(t):
    print("\n# " + t + "\n")
    REPORT.append("\n# " + t + "\n")


def main():
    code = r'''
import json, time
import boto3

S3 = boto3.client("s3")

def lambda_handler(event=None, context=None):
    paginator = S3.get_paginator("list_objects_v2")
    
    htmls = []
    data_feeds = []
    other = []
    
    for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
        for obj in page.get("Contents", []) or []:
            k = obj["Key"]
            entry = {
                "key": k,
                "size": obj["Size"],
                "lm": obj["LastModified"].isoformat(),
            }
            if k.endswith(".html") or k.endswith(".htm"):
                htmls.append(entry)
            elif k.startswith("data/") and k.endswith(".json"):
                data_feeds.append(entry)
            elif k.startswith("data/"):
                other.append(entry)
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "htmls": sorted(htmls, key=lambda x: x["key"]),
            "data_feeds": sorted(data_feeds, key=lambda x: x["lm"], reverse=True),
            "data_other": sorted(other, key=lambda x: x["key"])[:30],
            "n_html": len(htmls),
            "n_data_feeds": len(data_feeds),
        }, default=str),
    }
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-website-audit-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=120, MemorySize=512)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    log("  invoking audit...")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
        return
    
    d = json.loads(resp.get("body", "{}"))
    
    section("Website audit — current state")
    log("  Total HTML pages: " + str(d.get("n_html")))
    log("  Total data feeds: " + str(d.get("n_data_feeds")))
    log("")
    log("  ── ALL HTML PAGES ──")
    for h in d.get("htmls", []):
        sz_str = "{:>10,}b".format(h["size"])
        log("    {:<55}  {}  modified={}".format(h["key"], sz_str, h["lm"][:16]))
    
    log("")
    log("  ── ALL DATA FEEDS (sorted by recency) ──")
    for f in d.get("data_feeds", []):
        sz_str = "{:>10,}b".format(f["size"])
        log("    {:<55}  {}  modified={}".format(f["key"], sz_str, f["lm"][:16]))
    
    section("Cross-reference: data feeds → expected HTML")
    expected_pages = {
        # Old/existing
        "data/data.json": ["index.html (main dashboard)"],
        "data/screener.json": ["screener/index.html"],
        "data/options-flow.json": ["options-flow.html"],
        "data/atr-tracker.json": ["ath/index.html"],
        "data/cftc-positioning.json": ["cftc.html"],
        "data/crypto-intel.json": ["crypto/index.html"],
        # NEW — built today
        "data/themes-detected.json": ["themes/index.html (NEW)"],
        "data/nobrainers.json": ["nobrainers/index.html (NEW)"],
        "data/insider-clusters.json": ["insiders/index.html (NEW)"],
        "data/smart-money-clusters.json": ["smart-money/index.html (NEW)"],
        "data/deep-value.json": ["deep-value/index.html (NEW)"],
        "data/eps-revision-velocity.json": ["eps-velocity/index.html (NEW)"],
        "data/momentum-breakout.json": ["momentum/index.html (NEW)"],
        "data/pre-pump-signals.json": ["pre-pump/index.html (NEW)"],
        "data/sector-earnings-diffusion.json": ["sector-diffusion/index.html (NEW)"],
        "data/narrative-density.json": ["narratives/index.html (NEW)"],
        "data/activist-filings.json": ["activist/index.html (NEW)"],
        "data/cross-asset-regime.json": ["regime/index.html (NEW)"],
        "data/theme-rotation.json": ["theme-rotation/index.html (NEW)"],
        "data/volatility-squeeze.json": ["squeeze/index.html (NEW)"],
        "data/revenue-acceleration.json": ["rev-accel/index.html (NEW)"],
        "data/microcap-float-squeeze.json": ["float-squeeze/index.html (NEW)"],
        "data/earnings-pead.json": ["pead/index.html (NEW)"],
        "data/compound-signals.json": ["compound/index.html (NEW)"],
        "data/universe.json": ["universe.html (NEW)"],
    }
    
    existing_paths = {h["key"] for h in d.get("htmls", [])}
    
    log("  ── Coverage gaps to fix ──")
    for feed_key, page_hints in expected_pages.items():
        feed_exists = any(f["key"] == feed_key for f in d.get("data_feeds", []))
        if not feed_exists:
            log("    ⚠ " + feed_key + " — NO DATA FEED YET")
            continue
        # Try to find a corresponding HTML page
        feed_name = feed_key.replace("data/", "").replace(".json", "")
        # Look for any HTML page that mentions this feed
        related = [p for p in existing_paths if feed_name.split("-")[0] in p.lower()]
        marker = "✅" if related else "❌ NEEDS PAGE"
        page_str = page_hints[0] if page_hints else "?"
        log("    " + marker + " {:<48}  → {}".format(feed_key, page_str))
        for r in related[:2]:
            log("        existing: " + r)
    
    try:
        L.delete_function(FunctionName=NAME)
        log("  ✓ probe deleted")
    except Exception:
        pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_z1_audit_website.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
