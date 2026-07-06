#!/usr/bin/env python3
"""ops 2916 — FULL coverage audit: every page + every engine reachable from
the new Command Center. (1) live nav-manifest == repo pages, (2) HTTP-sweep
all 366 entries (threaded), (3) shell hooks on live index, (4) engine counts
repo-vs-registry (post 2876 regen), (5) KA + COT slots now resolve live."""
import json, sys, time, glob, os, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh-ops"}), timeout=to)
        return r.getcode(), r.read()
    except Exception:
        return None, b""

out = {}
with report("2916") as r:
    r.section("(1) manifest vs repo")
    repo = sorted(os.path.basename(p) for p in glob.glob("*.html"))
    c, b = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    m = json.loads(b.decode())
    live_pages = sorted(pp["href"].lstrip("/") for cat in m["categories"] for pp in cat["pages"])
    out["repo_pages"] = len(repo); out["manifest_pages"] = len(live_pages)
    miss = [p for p in repo if p not in live_pages]
    dead = [p for p in live_pages if p not in repo]
    out["missing_from_manifest"] = miss; out["dead_entries"] = dead
    (r.ok if not miss and not dead else r.fail)(f"repo {len(repo)} vs manifest {len(live_pages)} | missing={miss} dead={dead}")

    r.section("(2) HTTP sweep — every page")
    def chk(p):
        c2, b2 = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        return p, c2, len(b2)
    bad = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for p, c2, n in ex.map(chk, live_pages):
            if c2 != 200 or n < 300: bad.append((p, c2, n))
    out["sweep_total"] = len(live_pages); out["sweep_bad"] = bad
    (r.ok if not bad else r.fail)(f"{len(live_pages)-len(bad)}/{len(live_pages)} pages return 200; bad={bad[:5]}")

    r.section("(3) shell + SW on live index")
    c, b = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
    idx = b.decode("utf-8","replace")
    hooks = {h: (h in idx) for h in ["jh-nav-drawer.js","jh-page-ai.js","wss-client.js",'JH_V="v1.2.1"',"AMBER TERMINAL","data/ka-metrics.json","cot/extremes/current.json"]}
    out["index_hooks"] = hooks
    (r.ok if all(hooks.values()) else r.fail)(f"hooks: {hooks}")

    r.section("(4) engines: repo vs registry")
    n_dirs = len(glob.glob("aws/lambdas/*/"))
    c, b = get(f"https://justhodl.ai/data/engine-registry.json?t={int(time.time())}")
    reg = json.loads(b.decode()) if c == 200 else {}
    n_reg = reg.get("count") or len(reg.get("engines", {}))
    out["repo_lambda_dirs"] = n_dirs; out["registry_engines"] = n_reg
    out["registry_generated"] = reg.get("generated_at") or reg.get("generated")
    r.ok(f"repo lambda dirs {n_dirs} | registry engines {n_reg} (gen {out['registry_generated']})")
    for fn in ("justhodl-market-tape","justhodl-investor-lenses","justhodl-technical-overlays"):
        inreg = fn.replace("justhodl-","") in json.dumps(reg.get("engines", reg))[:400000]
        (r.ok if inreg else r.fail)(f"  new engine in registry: {fn} = {inreg}")
        out.setdefault("new_in_registry", {})[fn] = inreg

    r.section("(5) KA + COT slots resolve")
    c1, b1 = get(f"https://justhodl.ai/data/ka-metrics.json?t={int(time.time())}")
    ka = json.loads(b1.decode()) if c1 == 200 else {}
    out["ka"] = {"http": c1, "khalid_index": ka.get("khalid_index")}
    (r.ok if c1 == 200 and ka.get("khalid_index") is not None else r.fail)(f"ka-metrics {c1} khalid_index={ka.get('khalid_index')}")
    c2, b2 = get("https://justhodl-dashboard-live.s3.amazonaws.com/cot/extremes/current.json")
    cot = json.loads(b2.decode()) if c2 == 200 else {}
    ncrowd = len(cot.get("crowded", cot.get("extremes", []))) if isinstance(cot, dict) else None
    out["cot"] = {"http": c2, "n_crowded": ncrowd}
    (r.ok if c2 == 200 else r.fail)(f"cot extremes {c2} crowded={ncrowd}")

    json.dump(out, open("aws/ops/reports/2916.json","w"), indent=2, default=str)
    r.ok("report -> aws/ops/reports/2916.json")
print("DONE 2916"); sys.exit(0)
