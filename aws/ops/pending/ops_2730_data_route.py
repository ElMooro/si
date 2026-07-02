"""ops 2730 — /data/* ZONE ROUTE: class-level fix for the domain feed 404s.

Diagnosis (2729): justhodl.ai never had a /data/* route — S3 + workers.dev
serve fine, domain 404s (even gfd). Fix shipped: wrangler zone routes bind
justhodl.ai/data/* to justhodl-data-proxy (edge-cached, CORS, TTL rules),
plus a Pages _redirects 302 fallback that guarantees service even if the CF
token lacks route scope. This ops proves it end-to-end from the runner.
Report: aws/ops/reports/2730_data_route.json.
"""
import os, json, time, urllib.request, urllib.error
from datetime import datetime, timezone

R = {"ops": 2730, "ts": datetime.now(timezone.utc).isoformat()}
UA = {"User-Agent": "Mozilla/5.0 jh-verify", "Cache-Control": "no-cache"}

def get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=25) as r:
            b = r.read()
            return r.status, r.headers.get("Content-Type", ""), b
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("Content-Type", ""), (e.read() or b"")
    except Exception as e:
        return None, str(e)[:60], b""

print("waiting 75s for deploy-workers + Pages builds…"); time.sleep(75)
targets = {"footprint": "https://justhodl.ai/data/institutional-footprint.json",
           "gfd": "https://justhodl.ai/data/global-flow-desk.json",
           "capex": "https://justhodl.ai/data/capex-pulse.json"}
final = {}
for attempt in range(7):
    ok_all = True
    for name, url in targets.items():
        st, ct, body = get(url + "?cb=%d" % int(time.time()))
        good = st == 200 and "json" in ct and body[:1] == b"{"
        final[name] = {"status": st, "ctype": ct[:40], "ok": good,
                       "head": body[:90].decode("utf-8", "ignore")}
        ok_all = ok_all and good
    print("  attempt %d: %s" % (attempt + 1, {k: v["status"] for k, v in final.items()}))
    if ok_all: break
    time.sleep(30)
R["feeds"] = final
assert all(v["ok"] for v in final.values()), "domain feeds still failing: %s" % final
fp = json.loads(get(targets["footprint"])[2])
R["footprint_live"] = {"version": fp.get("version"), "has_posture": bool(fp.get("posture")),
                       "risk_now": (fp.get("posture") or {}).get("risk_now"),
                       "ledger_classes": len(fp.get("asset_ledger") or {})}
print("  footprint live:", json.dumps(R["footprint_live"]))
assert R["footprint_live"]["has_posture"] and R["footprint_live"]["ledger_classes"] >= 9

st, ct, body = get("https://justhodl.ai/institutional-footprint.html?cb=%d" % int(time.time()))
html = body.decode("utf-8", "ignore")
R["page"] = {"status": st, "marker": "ASSET LEDGER" in html, "fetch_rel": "data/institutional-footprint.json" in html}
print("  page:", json.dumps(R["page"]))
assert st == 200 and R["page"]["marker"]
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2730_data_route.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2730 COMPLETE — /data/* lives on the domain; the class is fixed")
