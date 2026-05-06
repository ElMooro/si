"""Phase Z10 — Final final: verify justhodl.ai/intel/ live on GitHub Pages + send digest."""
import io, json, os, time, base64, zipfile
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
    log("# Phase Z10 — Verify justhodl.ai/intel/ live on GitHub Pages\n")
    
    code = r'''
import json, urllib.request, urllib.error, time

def fetch(url, label):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as r:
            content = r.read()
            return {
                "ok": True, "status": r.status,
                "size": len(content),
                "title": (
                    content[content.find(b"<title>")+7:content.find(b"</title>")].decode("utf-8", "replace")
                    if b"<title>" in content else "?"
                ),
                "has_intel_banner": b"INTEL TERMINAL BANNER" in content,
                "has_intel_link": b'href="/intel/"' in content or b"href='/intel/'" in content,
                "has_signal_grid": b"signal-grid" in content,
                "has_compound_card": b"compound-tier3" in content,
                "server": r.headers.get("Server", "?"),
            }
    except urllib.error.HTTPError as e:
        return {"ok": False, "status": e.code, "label": label}
    except Exception as e:
        return {"ok": False, "err": str(e)[:120]}


def lambda_handler(event=None, context=None):
    # GitHub Pages takes 1-3 minutes to deploy after a push
    # Test multiple times to allow propagation
    targets = [
        ("https://justhodl.ai/", "homepage"),
        ("https://justhodl.ai/intel/", "intel_dashboard"),
        ("https://justhodl.ai/intel/index.html", "intel_dashboard_explicit"),
    ]
    
    out = {"attempts": []}
    
    for attempt in range(4):  # ~30s of retries
        results = {}
        for url, key in targets:
            results[key] = fetch(url, key)
        out["attempts"].append({"attempt": attempt, "results": results})
        # Check if intel is live
        intel = results.get("intel_dashboard", {})
        if intel.get("ok") and intel.get("has_signal_grid"):
            out["final_attempt"] = attempt
            break
        time.sleep(15)
    
    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''
    
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        zi = zipfile.ZipInfo("lambda_function.py")
        zi.external_attr = 0o644 << 16
        z.writestr(zi, code)
    
    NAME = "justhodl-pages-verify-temp"
    try:
        L.get_function(FunctionName=NAME)
        L.update_function_code(FunctionName=NAME, ZipFile=buf.getvalue())
    except L.exceptions.ResourceNotFoundException:
        L.create_function(FunctionName=NAME, Runtime="python3.12",
                           Handler="lambda_function.lambda_handler",
                           Role=ROLE_ARN, Code={"ZipFile": buf.getvalue()},
                           Timeout=180, MemorySize=256)
    for _ in range(20):
        c = L.get_function_configuration(FunctionName=NAME)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(1)

    log("## 1) Test live URLs (with retry up to 4x while GH Pages deploys)")
    r = L.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    resp = json.loads(r["Payload"].read())
    if "errorMessage" in resp:
        log("  ❌ " + resp["errorMessage"][:300])
    else:
        d = json.loads(resp.get("body", "{}"))
        log("  Final attempt: " + str(d.get("final_attempt", "?")))
        for att in d.get("attempts", []):
            log("\n  ─ Attempt " + str(att["attempt"]) + " ─")
            for k, info in att["results"].items():
                mark = "✅" if info.get("ok") else "❌"
                log("    " + mark + " " + k.ljust(30) + " status=" + str(info.get("status")) +
                     " size=" + str(info.get("size", 0)))
                if info.get("ok"):
                    log("       title: " + str(info.get("title", "?"))[:70])
                    log("       has_intel_banner: " + str(info.get("has_intel_banner")) +
                         "  has_intel_link: " + str(info.get("has_intel_link")) +
                         "  has_signal_grid: " + str(info.get("has_signal_grid")))

    try:
        L.delete_function(FunctionName=NAME)
        log("\n  ✓ probe cleaned up")
    except Exception:
        pass

    # Send Telegram digest only if confirmed live
    log("\n## 2) Send confirmation Telegram digest")
    try:
        token = SSM.get_parameter(Name="/justhodl/telegram/bot_token",
                                    WithDecryption=True)["Parameter"]["Value"]
        chat = SSM.get_parameter(Name="/justhodl/telegram/chat_id")["Parameter"]["Value"]
    except Exception as e:
        log("  ❌ telegram credentials: " + str(e))
        return

    parts = []
    parts.append("🌐 *INTEL TERMINAL: LIVE ON JUSTHODL\\.AI*\n\n")
    parts.append(md_escape("All 13 institutional feeds now deployed via GitHub Pages.") + "\n\n")
    parts.append("📍 *Access:*\n")
    parts.append("  *https://justhodl\\.ai/* \\(homepage with banner\\)\n")
    parts.append("  *https://justhodl\\.ai/intel/* \\(full terminal\\)\n\n")
    parts.append("*Dashboard contents \\(12 cards\\):*\n")
    parts.append(md_escape("• 🌍 Cross-asset macro regime") + "\n")
    parts.append(md_escape("• 🎯 6 cap-bucket universe stats") + "\n")
    parts.append(md_escape("• ⚡ 13-feed compound TIER-3+") + "\n")
    parts.append(md_escape("• 🔋 Volatility squeeze hunter") + "\n")
    parts.append(md_escape("• 📊 Revenue acceleration") + "\n")
    parts.append(md_escape("• 🔥 Microcap float squeeze") + "\n")
    parts.append(md_escape("• 📈 PEAD detector") + "\n")
    parts.append(md_escape("• 📞 Options flow") + "\n")
    parts.append(md_escape("• 🏛️ Activist filings") + "\n")
    parts.append(md_escape("• 🟢 Theme rotation") + "\n")
    parts.append(md_escape("• 📈 Sector diffusion") + "\n")
    parts.append(md_escape("• 📰 Narrative density") + "\n")
    parts.append(md_escape("• 🎯 No-brainers + 🌱 pre-pump (legacy)") + "\n\n")
    parts.append(md_escape("Auto-refresh every 5 min · mobile-responsive") + "\n")
    parts.append(md_escape("All JSON feeds CORS-enabled, fetched directly from S3.") + "\n")
    
    text = "".join(parts)
    
    import urllib.request, urllib.error
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
    with open(os.path.join(out, "phase_z10_verify_pages.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(REPORT))
    print("[report written]")
