#!/usr/bin/env python3
"""ops 2931 — recon: history-index registry entry (retire vs rebuild decision)
+ registry schema sample (drives the right-rail provenance/interpretation data)."""
import json, sys, time, urllib.request
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u):
    r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=15)
    return r.read().decode("utf-8", "replace")

with report("2931") as r:
    reg = json.loads(get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}"))
    raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
    entries = ({k: {**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()}
               if isinstance(raw, dict) else {e.get("name", str(i)): e for i, e in enumerate(raw)})
    hi = entries.get("justhodl-history-index")
    out = {"history_index_entry": hi}
    r.ok(f"history-index registry entry: {json.dumps(hi)}")

    sample = list(entries.items())[:4]
    out["schema_sample"] = {k: v for k, v in sample}
    r.ok("schema sample:\n" + "\n".join(f"  {k} -> {json.dumps(v)[:220]}" for k, v in sample))

    # which manifest pages have NO matching registry engine at all (candidates for "hub/document", i.e. skip right-rail)
    man = json.loads(get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}"))
    pages = [p["href"].lstrip("/") for c in man["categories"] for p in c["pages"]]
    def norm(s): return s.replace("justhodl-", "").replace("-", "").replace("_", "").replace(".html", "").lower()
    ekeys = [norm(k) for k in entries]
    unmatched_pages = [p for p in pages if not any(norm(p) in ek or ek in norm(p) for ek in ekeys)]
    out["pages_total"] = len(pages)
    out["pages_no_engine_match"] = sorted(unmatched_pages)
    r.ok(f"pages with NO engine match (rail-skip candidates): {len(unmatched_pages)}/{len(pages)}")
    json.dump(out, open("aws/ops/reports/recon_2931.json", "w"), indent=2, default=str)
print("DONE 2931 PASS"); sys.exit(0)
