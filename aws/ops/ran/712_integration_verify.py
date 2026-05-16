"""ops/712 — verify crisis-composite integration deployed:
  #1 ai-chat + morning-intelligence Lambda code carries the new markers
  #2 /intelligence/ page serves the DEFCON tab
"""
import json, os, io, zipfile, urllib.request
import boto3
from datetime import datetime, timezone

lam = boto3.client("lambda", region_name="us-east-1")


def lambda_code_has(fn, markers):
    """Download the deployed Lambda zip, check source for markers."""
    try:
        loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
        raw = urllib.request.urlopen(loc, timeout=30).read()
        zf = zipfile.ZipFile(io.BytesIO(raw))
        src = ""
        for n in zf.namelist():
            if n.endswith(".py"):
                src += zf.read(n).decode("utf-8", "replace")
        return {"ok": all(m in src for m in markers),
                "found": {m: (m in src) for m in markers},
                "code_size": len(raw)}
    except Exception as e:
        return {"ok": False, "err": str(e)[:200]}


def page_has(url, markers):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Verify/1.0",
                                                    "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=20) as r:
            code, html = r.getcode(), r.read().decode("utf-8", "replace")
        return {"http_status": code, "ok": code == 200 and all(m in html for m in markers),
                "missing": [m for m in markers if m not in html]}
    except Exception as e:
        return {"ok": False, "err": str(e)[:200]}


def main():
    report = {"checked_at": datetime.now(timezone.utc).isoformat()}

    report["ai_chat"] = lambda_code_has(
        "justhodl-ai-chat", ["CRISIS DEFCON", "crisis-composite.json", "CAPITULATION"])
    report["morning_intelligence"] = lambda_code_has(
        "justhodl-morning-intelligence", ["crisis_composite", "CRISIS_DEFCON", "capit_signal"])

    ts = int(datetime.now(timezone.utc).timestamp())
    report["intelligence_page"] = page_has(
        f"https://justhodl.ai/intelligence/?cb={ts}",
        ['data-pane="defcon"', 'id="pane-defcon"', "renderDefcon",
         "crisis-composite.json", "Master Crisis Score"])

    report["summary"] = {
        "ai_chat_integrated": report["ai_chat"].get("ok"),
        "morning_intel_integrated": report["morning_intelligence"].get("ok"),
        "defcon_tab_live": report["intelligence_page"].get("ok"),
    }
    report["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/712_integration_verify.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    print("DONE -> 712_integration_verify.json :: " + json.dumps(report["summary"]))


if __name__ == "__main__":
    main()
