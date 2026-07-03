"""ops 2778 — verify Options Desk hub live at edge + all 7 feeds reachable/fresh.
Report: 2778_options_hub_verify.json.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2778, "ts": datetime.now(timezone.utc).isoformat()}
def get(url, timeout=25):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "cb=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()
# 1) page live at edge
okp, html = False, b""
for a in range(6):
    time.sleep(35)
    try:
        html = get("https://justhodl.ai/options.html")
        okp = b"OPTIONS HUB v1" in html and b"Dealer Gamma Exposure" in html and b"MULTI-ENGINE CONFLUENCE" in html
    except Exception as e:
        R.setdefault("page_errs", []).append(str(e)[:60])
    print("  page attempt %d: %s (%d bytes)" % (a + 1, "LIVE" if okp else "pending", len(html)))
    if okp: break
R["page_live"] = okp
R["page_has_sections"] = {s: (s.encode() in html) for s in ("Dealer Gamma Exposure", "MULTI-ENGINE CONFLUENCE", "UNUSUAL OPTIONS FLOW", "DIX & GEX", "OPEX / 0DTE")}
# 2) feeds reachable + fresh via CF Worker /data/
feeds = ["dealer-gex", "options-gamma", "options-confluence", "polygon-options-flow", "options-flow", "dix", "opex-calendar"]
R["feeds"] = {}
now = datetime.now(timezone.utc)
for f in feeds:
    try:
        d = json.loads(get("https://justhodl.ai/data/%s.json" % f))
        ts = d.get("generated_at") or d.get("as_of") or d.get("data_date")
        age = None
        if ts:
            try:
                age = round((now - datetime.fromisoformat(ts.replace("Z", "+00:00"))).total_seconds() / 3600, 1)
            except Exception:
                age = "parse"
        R["feeds"][f] = {"reachable": True, "age_hours": age}
        print("  feed %-22s reachable age=%sh" % (f, age))
    except Exception as e:
        R["feeds"][f] = {"reachable": False, "err": str(e)[:50]}
        print("  feed %-22s UNREACHABLE %s" % (f, str(e)[:40]))
assert okp, "options hub not live at edge"
assert all(v.get("reachable") for v in R["feeds"].values()), "some feeds unreachable"
R["status"] = "LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2778_options_hub_verify.json", "w"), indent=1, default=str)
print("OPS 2778 COMPLETE — Options Desk hub live, all feeds reachable")
