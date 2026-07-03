"""ops 2780 — verify Options Desk v2 live at edge with new sections."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2780, "ts": datetime.now(timezone.utc).isoformat()}
def get(url, t=25):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "cb=%d" % int(time.time()), headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()
markers = [b"OPTIONS HUB v2", b"NET GEX BY STRIKE", b"GEX BY EXPIRATION", b"DEALER GREEKS", b"POSITIONING BOARD", b"CATALYST SKEW", b"VOLATILITY SQUEEZE"]
okp = False
for a in range(7):
    time.sleep(35)
    try:
        h = get("https://justhodl.ai/options.html")
        present = {m.decode(): (m in h) for m in markers}
        okp = all(present.values())
        R["markers"] = present
        print("attempt %d: %s (%d bytes)" % (a + 1, "LIVE" if okp else "partial", len(h)))
    except Exception as e:
        print("attempt %d err %s" % (a + 1, str(e)[:50]))
    if okp:
        break
R["page_v2_live"] = okp
assert okp, ("v2 not fully live", R.get("markers"))
R["status"] = "LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2780_hubv2_verify.json", "w"), indent=1, default=str)
print("OPS 2780 COMPLETE — Options Desk v2 live")
