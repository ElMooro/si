"""ops 2785 — verify consolidated Options Desk (4 detail-page datasets) live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2785, "ts": datetime.now(timezone.utc).isoformat()}
def get(u, t=25):
    req = urllib.request.Request(u + ("&" if "?" in u else "?") + "cb=%d" % int(time.time()), headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.read()
markers = [b"GAMMA SQUEEZE SETUPS", b"MOST UNUSUAL ACTIVITY", b"OPTIONS FLOW SCANNER \xe2\x80\x94 QUALIFYING", b"TOP GAMMA STRIKES", b"OPEN INTEREST \xe2\x80\x94 calls"]
okp = False
for a in range(6):
    time.sleep(30)
    try:
        h = get("https://justhodl.ai/options.html")
        pres = {m.decode("utf-8", "ignore")[:24]: (m in h) for m in markers}
        okp = all(pres.values()); R["markers"] = pres; R["bytes"] = len(h)
        print("attempt %d: %s (%d bytes) %s" % (a + 1, "ALL LIVE" if okp else "partial", len(h), pres))
    except Exception as e:
        print("attempt %d err %s" % (a + 1, str(e)[:50]))
    if okp: break
R["consolidation_live"] = okp
# confirm the source data exists so the panels populate
try:
    of = json.loads(get("https://justhodl.ai/data/options-flow.json"))
    oa = json.loads(get("https://justhodl.ai/data/options-analytics.json"))
    R["data"] = {"flow_qualifying": len(of.get("all_qualifying") or []), "flow_summary": bool(of.get("summary")),
                 "squeeze_setups": len(oa.get("squeeze_setups") or []), "most_unusual": len(oa.get("most_unusual") or [])}
    print("data: qualifying=%d squeeze_setups=%d most_unusual=%d" % (R["data"]["flow_qualifying"], R["data"]["squeeze_setups"], R["data"]["most_unusual"]))
except Exception as e:
    R["data"] = "err " + str(e)[:60]
assert okp, ("consolidation not fully live", R.get("markers"))
R["status"] = "CONSOLIDATION LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2785_consolidation_verify.json", "w"), indent=1, default=str)
print("OPS 2785 COMPLETE")
