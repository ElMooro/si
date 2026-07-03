"""ops 2783 — verify Options Desk fix at edge (String coercion + try/catch guards)
+ inspect actual alert_level values in polygon-options-flow. Report: 2783_pagefix_verify.json.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2783, "ts": datetime.now(timezone.utc).isoformat()}
def get(url, t=25):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "cb=%d" % int(time.time()), headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.read()
okp = False
for a in range(6):
    time.sleep(30)
    try:
        h = get("https://justhodl.ai/options.html")
        has_fix = b"String(r.alert_level" in h
        n_try = h.count(b"try{ ")
        okp = has_fix and n_try >= 8
        R["page"] = {"has_string_coercion": has_fix, "n_try_blocks": n_try, "bytes": len(h)}
        print("attempt %d: fix=%s try_blocks=%d (%d bytes)" % (a + 1, has_fix, n_try, len(h)))
    except Exception as e:
        print("attempt %d err %s" % (a + 1, str(e)[:50]))
    if okp: break
R["fix_live"] = okp
# inspect actual alert_level type/values so the flow column renders sensibly
try:
    pf = json.loads(get("https://justhodl.ai/data/polygon-options-flow.json"))
    rows = (pf.get("extreme_call_flow") or []) + (pf.get("bullish_call_flow") or [])
    R["alert_level_sample"] = [{"ticker": r.get("ticker"), "alert_level": r.get("alert_level"), "type": type(r.get("alert_level")).__name__} for r in rows[:8]]
    print("alert_level values:", [(r.get("ticker"), r.get("alert_level"), type(r.get("alert_level")).__name__) for r in rows[:6]])
except Exception as e:
    R["alert_level_sample"] = "err " + str(e)[:60]
assert okp, "page fix not live at edge"
R["status"] = "FIX LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2783_pagefix_verify.json", "w"), indent=1, default=str)
print("OPS 2783 COMPLETE")
