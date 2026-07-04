"""ops 2793 — verify LLM Cost Desk page + feed live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2793, "ts": datetime.now(timezone.utc).isoformat()}
def get(u, t=25):
    req = urllib.request.Request(u + ("&" if "?" in u else "?") + "cb=%d" % int(time.time()), headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.status, r.read()
# page (retry for pages propagation)
okp = False
for a in range(5):
    time.sleep(25)
    try:
        st, b = get("https://justhodl.ai/llm-cost.html")
        marks = [x in b for x in (b"LLM Cost Desk", b"Cache hit rate", b"data/llm-cost.json", b"Controls")]
        okp = (st == 200 and all(marks)); R["page"] = {"status": st, "bytes": len(b), "markers_ok": all(marks)}
        print("page attempt %d: HTTP %d markers_ok=%s" % (a + 1, st, all(marks)))
        if okp: break
    except Exception as e:
        print("page attempt %d err %s" % (a + 1, str(e)[:60]))
# feed
try:
    st, b = get("https://justhodl.ai/data/llm-cost.json")
    d = json.loads(b)
    R["feed"] = {"status": st, "mode": d.get("mode"), "budget": d.get("daily_budget_usd"),
                 "keys": sorted(d.keys()), "per_day_len": len(d.get("per_day", []))}
    print("feed: HTTP %d mode=%s budget=%s per_day=%d" % (st, d.get("mode"), d.get("daily_budget_usd"), len(d.get("per_day", []))))
except Exception as e:
    R["feed"] = "ERR " + str(e)[:80]; print("feed err", str(e)[:80])
R["status"] = "COST DESK LIVE" if okp and isinstance(R.get("feed"), dict) else "CHECK"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2793_costdesk_verify.json", "w"), indent=1, default=str)
print("OPS 2793:", R["status"])
