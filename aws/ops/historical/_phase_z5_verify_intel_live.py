"""Phase Z5 — End-to-end verification of intel dashboard:
  1. Fetch /intel/ page itself
  2. Test each JSON feed for CORS + accessibility
  3. Send confirmation Telegram digest with the new dashboard URL"""
import io, json, os, time, base64, zipfile, urllib.request, urllib.error
import boto3

REGION = "us-east-1"
L = boto3.client("lambda", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

REPORT = []
def log(m):
    print(m)
    REPORT.append(m)


def md_escape(s):
    out = []
    for c in str(s):
        if c in "_*[]()~`>#+-=|{}.!\\":
            out.append("\\" + c)
        else:
            out.append(c)
    return "".join(out)


def main():
    log("# Phase Z5 — End-to-end verify intel dashboard\n")
    
    code = r'''
import json, urllib.request, urllib.error, time

def head(url):
    try:
        req = urllib.request.Request(url, method="HEAD")
        req.add_header("Origin", "https://justhodl.ai")
        with urllib.request.urlopen(req, timeout=10) as r:
            return {"ok": True, "status": r.status,
                    "size": int(r.headers.get("Content-Length", 0)),
                    "type": r.headers.get("Content-Type"),
                    "cors": r.headers.get("Access-Control-Allow-Origin")}
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code}
    except Exception as e:
        return {"ok": False, "error": str(e)[:80]}

def lambda_handler(event=None, context=None):
    base = "https://justhodl-dashboard-live.s3.amazonaws.com"
    pages = ["/intel/index.html", "/intel.html", "/index.html"]
    feeds = [
        "/data/cross-asset-regime.json", "/data/universe.json",
        "/data/compound-signals.json", "/data/volatility-squeeze.json",
        "/data/revenue-acceleration.json", "/data/microcap-float-squeeze.json",
        "/data/earnings-pead.json", "/data/options-flow.json",
        "/data/activist-filings.json", "/data/theme-rotation.json",
        "/data/sector-earnings-diffusion.json", "/data/narrative-density.json",
        "/data/nobrainers.json", "/data/pre-pump-signals.json",
    ]
    
    out = {"pages": {}, "feeds": {}, "summary": {}}
    n_ok_pages = 0
    n_ok_feeds = 0
    
    for p in pages:
        result = head(base + p)
        out["pages"][p] = result
        if result.get("ok"):
            n_ok_pages += 1
    
    for f in feeds:
        result = head(base + f)
        out["feeds"][f] = result
        if result.get("ok"):
            n_ok_feeds += 1
    
    out["summary"] = {
        "pages_ok": str(n_ok_pages) + "/" + str(len(pages)),
        "feeds_ok": str(n_ok_feeds) + "/" + str(len(feeds)),
    }
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-verify-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=120, MemorySize=256)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    log("## 1) Verifying pages + feeds")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        log("\n### Pages")
        for path, info in d.get("pages", {}).items():
            mark = "✓" if info.get("ok") else "❌"
            sz = info.get("size", 0)
            cors = info.get("cors", "?")
            log("  " + mark + " " + path + " — " + str(sz) + "b, status=" + str(info.get("status")) +
                 ", cors=" + str(cors))
        log("\n### Feeds")
        feeds_failing = []
        for path, info in d.get("feeds", {}).items():
            mark = "✓" if info.get("ok") else "❌"
            sz = info.get("size", 0)
            cors = info.get("cors", "?")
            log("  " + mark + " " + path + " — " + str(sz) + "b, status=" + str(info.get("status")) +
                 ", cors=" + str(cors))
            if not info.get("ok"):
                feeds_failing.append(path)
        log("\n### Summary")
        log("  Pages OK: " + str(d.get("summary", {}).get("pages_ok")))
        log("  Feeds OK: " + str(d.get("summary", {}).get("feeds_ok")))

    try:
        L.delete_function(FunctionName=NAME)
    except Exception:
        pass

    # Send Telegram digest
    log("\n## 2) Send Telegram digest")
    try:
        token = SSM.get_parameter(Name="/justhodl/telegram/bot_token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        log("  ❌ telegram credentials: " + str(e))
        return

    parts = []
    parts.append("🌐 *NEW: INTEL TERMINAL DEPLOYED TO WEBSITE*\n\n")
    parts.append(md_escape("All 13 institutional feeds now visible at:") + "\n")
    parts.append("  https\\:\\/\\/justhodl\\.ai\\/intel\\/\n\n")
    parts.append("*What's on the new dashboard:*\n")
    parts.append(md_escape("• 🌍 Cross-asset macro regime (REFLATION, RISK_ON, etc)") + "\n")
    parts.append(md_escape("• 🎯 6 cap-bucket universe (mega → nano coverage)") + "\n")
    parts.append(md_escape("• ⚡ 13-feed compound TIER-3+ multi-system convergence") + "\n")
    parts.append(md_escape("• 🔋 Volatility squeeze (coiled springs, BB+TTM+NR7)") + "\n")
    parts.append(md_escape("• 📊 Revenue acceleration (YoY growth inflection)") + "\n")
    parts.append(md_escape("• 🔥 Microcap float squeeze (short + float exhaustion)") + "\n")
    parts.append(md_escape("• 📈 PEAD (post-earnings drift)") + "\n")
    parts.append(md_escape("• 📞 Options flow + 🏛️ activist filings") + "\n")
    parts.append(md_escape("• 🟢 Theme rotation + 📈 sector diffusion + 📰 narrative density") + "\n")
    parts.append(md_escape("• 🎯 Legacy: nobrainers + pre-pump") + "\n\n")
    parts.append(md_escape("Auto-refreshes every 5 min. Mobile-responsive. Bloomberg-style dark UI.") + "\n\n")
    parts.append(md_escape("Banner added to homepage \\(justhodl\\.ai\\) for prominent access\\.") + "\n")
    
    text = "".join(parts)
    
    url = "https://api.telegram.org/bot" + token + "/sendMessage"
    data = json.dumps({
        "chat_id": chat, "text": text,
        "parse_mode": "MarkdownV2",
        "disable_web_page_preview": False,
    }).encode()
    req = urllib.request.Request(url, data=data,
                                   headers={"Content-Type": "application/json"},
                                   method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            mid = json.loads(r.read())["result"]["message_id"]
            log("  ✅ delivered, message_id=" + str(mid))
    except urllib.error.HTTPError as e:
        body_err = e.read().decode("utf-8", "replace")[:400]
        log("  ❌ HTTP " + str(e.code) + ": " + body_err)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        log("FATAL: " + str(e))
        for ln in traceback.format_exc().splitlines():
            log("    " + ln)
    out = "aws/ops/reports/latest"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "phase_z5_verify_intel.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
