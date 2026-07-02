"""ops 2744 — ON-CHAIN DESK PAGE (Khalid: which page has the CryptoQuant data).

Answer was NONE — so this ships onchain.html (dedicated CryptoQuant surface:
composite dial, forward-ledger card, live fusion map, 8 metric cards w/ z-bars
and 1y percentiles, method) + registers it in nav-manifest (pages.yml taught
to deploy the manifest). Read-only otherwise. Asserts: page at edge w/ marker,
nav entry at edge, all 4 consumed feeds strict at domain.
Report: aws/ops/reports/2744_onchain_page.json.
"""
import os, json, time, urllib.request
from datetime import datetime, timezone

A = {"ops": 2744, "ts": datetime.now(timezone.utc).isoformat(), "checks": {}}
FAILS = []
def check(name, ok, detail=""):
    A["checks"][name] = {"ok": bool(ok), "detail": str(detail)[:160]}
    print("  %s %-34s %s" % ("PASS" if ok else "FAIL", name, str(detail)[:100]))
    if not ok: FAILS.append(name)
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return r.read()

print("waiting for pages deploy (first-party, ~90s)…")
okp = False
for a in range(6):
    time.sleep(42)
    try:
        b = pub("onchain.html")
        okp = b"ONCHAIN DESK v1" in b
    except Exception as e:
        print("  attempt %d: %s" % (a + 1, str(e)[:60]))
    print("  attempt %d: %s" % (a + 1, "LIVE" if okp else "pending"))
    if okp: break
check("page.onchain_at_edge", okp)
try:
    nv = pub("nav-manifest.json").decode()
    check("nav.entry_at_edge", "/onchain.html" in nv, "manifest %d bytes" % len(nv))
except Exception as e:
    check("nav.entry_at_edge", False, e)
for f in ("data/cryptoquant-onchain.json", "data/onchain-ratios.json",
          "data/crypto-exchange-flows.json", "data/crypto-miners.json"):
    try:
        json.loads(pub(f).decode(), parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        check("feed.%s" % f.split("/")[-1], True, "strict")
    except Exception as e:
        check("feed.%s" % f.split("/")[-1], False, e)
A["fusion_map"] = {
    "direct_2742": ["crypto-exchange-flows", "crypto-miners", "onchain-ratios", "signal-logger"],
    "transitive_auto": {"crypto-intel": ["xf", "miners", "ratios"],
                        "crypto-confluence": ["xf", "miners", "ratios"],
                        "cycle-clock": ["xf", "miners", "ratios"],
                        "morning-intelligence": ["ratios"], "asymmetric-scorer": ["ratios"]},
    "deferred_by_doctrine": "crypto_risk_score direct weighting — pending scorecard verdicts ~Jul-23"}
A["verdict"] = "ALL PASS (%d)" % len(A["checks"]) if not FAILS else "FAILURES: %s" % FAILS
print("VERDICT:", A["verdict"])
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2744_onchain_page.json", "w") as f:
    json.dump(A, f, indent=1)
assert not FAILS, A["verdict"]
print("OPS 2744 COMPLETE — the chain has a face")
