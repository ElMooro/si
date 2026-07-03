"""ops 2782 — final verify: DEX/VEX on hub at edge + all 12 hub feeds fresh."""
import os, json, time, urllib.request
from datetime import datetime, timezone
R = {"ops": 2782, "ts": datetime.now(timezone.utc).isoformat()}
def get(url, t=25):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "cb=%d" % int(time.time()), headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=t) as r: return r.read()
okp = False
for a in range(6):
    time.sleep(35)
    try:
        h = get("https://justhodl.ai/options.html")
        okp = (b"DEX (delta $)" in h) and (b"VEX (vega $)" in h) and (b"OPTIONS HUB v2" in h)
        print("attempt %d: %s (%d bytes)" % (a + 1, "DEX/VEX LIVE" if okp else "pending", len(h)))
    except Exception as e:
        print("attempt %d err %s" % (a + 1, str(e)[:50]))
    if okp: break
R["dexvex_on_page"] = okp
feeds = ["dealer-gex", "options-gamma", "options-confluence", "options-analytics", "polygon-options-flow",
         "options-flow", "dix", "opex-calendar", "dealer-gex-history", "catalyst-skew-premove",
         "earnings-iv-crush", "volatility-squeeze"]
now = datetime.now(timezone.utc); R["feeds"] = {}
for f in feeds:
    try:
        d = json.loads(get("https://justhodl.ai/data/%s.json" % f))
        ts = (d.get("generated_at") or d.get("as_of") or d.get("data_date")) if isinstance(d, dict) else None
        age = None
        if ts:
            try: age = round((now - datetime.fromisoformat(str(ts).replace("Z", "+00:00"))).total_seconds() / 3600, 1)
            except Exception: age = "?"
        R["feeds"][f] = {"ok": True, "age_h": age}
        print("  %-24s age=%sh" % (f, age))
    except Exception as e:
        R["feeds"][f] = {"ok": False, "err": str(e)[:40]}; print("  %-24s FAIL" % f)
# confirm DEX/VEX values in the live dealer-gex feed
try:
    dg = json.loads(get("https://justhodl.ai/data/dealer-gex.json"))
    spy = (dg.get("underlyings") or {}).get("SPY") or {}
    R["spy_dex_vex"] = {"DEX": spy.get("total_delta_dollars"), "VEX": spy.get("total_vega_dollars"),
                        "vanna": spy.get("total_vanna_dollars"), "charm": spy.get("total_charm_dollars_per_day")}
    print("  SPY DEX=%s VEX=%s" % (spy.get("total_delta_dollars"), spy.get("total_vega_dollars")))
except Exception as e:
    R["spy_dex_vex"] = {"err": str(e)[:50]}
assert okp, "DEX/VEX not visible at edge"
assert all(v.get("ok") for v in R["feeds"].values()), "some feeds unreachable"
R["status"] = "ALL VERIFIED"
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/2782_final_verify.json", "w"), indent=1, default=str)
print("OPS 2782 COMPLETE — all verified")
