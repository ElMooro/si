"""ops 2804 — verify burn-audit board + feed live at edge."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2804, "ts": datetime.now(timezone.utc).isoformat()}
def get(u, t=25):
    req = urllib.request.Request(u + ("&" if "?" in u else "?") + "cb=%d" % int(time.time()), headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.status, r.read()
okp = False
for a in range(5):
    time.sleep(25)
    try:
        st, b = get("https://justhodl.ai/llm-cost.html")
        okp = (st == 200 and b"Burn-risk audit" in b)
        R["page"] = {"status": st, "has_burn_section": b"Burn-risk audit" in b}
        if okp: break
    except Exception as e:
        R["page"] = "err " + str(e)[:50]
try:
    st, b = get("https://justhodl.ai/data/llm-cost-audit.json")
    d = json.loads(b)
    R["feed"] = {"status": st, "engines": len(d.get("engines", [])),
                 "total_uncapped": d.get("est_total_daily_cost_uncapped"),
                 "capped_count": sum(1 for e in d.get("engines", []) if e.get("capped"))}
except Exception as e:
    R["feed"] = "err " + str(e)[:60]
R["status"] = "AUDIT BOARD LIVE" if okp and isinstance(R.get("feed"), dict) else "CHECK"
print("page:", json.dumps(R.get("page")))
print("feed:", json.dumps(R.get("feed")))
print("STATUS:", R["status"])
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2804_audit_verify.json", "w"), indent=1, default=str)
print("OPS 2804 COMPLETE")
