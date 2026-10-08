#!/usr/bin/env python3
"""ops 2938 — live verification: pricing.html edits + sitewide theme-color
self-heal, full population."""
import json, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
sys.path.insert(0, "aws/ops")
from ops_report import report

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent":"Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read().decode("utf-8", "replace")
    except Exception:
        return None, ""

ok = True
with report("2938") as r:
    for att in range(12):
        c, idx = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if 'JH_V="v1.2.22"' in idx: break
        time.sleep(18)
    r.ok(f"v1.2.22 deploy live attempt {att+1}")

    c, pr = get(f"https://justhodl.ai/pricing.html?t={int(time.time())}")
    pr_ok = c == 200 and "🚀" not in pr and "lock in" in pr.lower() and "linear-gradient" not in pr
    ok &= pr_ok
    (r.ok if pr_ok else r.fail)(f"pricing.html: emoji removed / lock-in present / gradient removed = {pr_ok}")

    c, mj = get(f"https://justhodl.ai/nav-manifest.json?t={int(time.time())}")
    pages = [p["href"].lstrip("/") for cat in json.loads(mj)["categories"] for p in cat["pages"]]
    def chk(p):
        c2, b = get(f"https://justhodl.ai/{p}?t={int(time.time())}")
        if c2 != 200: return p, None
        import re
        tags = re.findall(r'<meta[^>]*name="theme-color"[^>]*content="([^"]*)"', b)
        return p, tags
    missing, wrong, dup = [], [], []
    with ThreadPoolExecutor(max_workers=12) as ex:
        for p, tags in ex.map(chk, pages):
            if tags is None: continue
            if not tags: missing.append(p)
            elif any(t != "#F0B429" for t in tags): wrong.append(p)
            if tags and len(tags) > 1: dup.append(p)
    clean = not missing and not wrong and not dup
    ok &= clean
    (r.ok if clean else r.fail)(f"theme-color full population: missing={len(missing)} wrong={len(wrong)} dup={len(dup)}"
                                + ("" if clean else f" | missing_sample={missing[:5]} wrong_sample={wrong[:5]}"))
print("DONE 2938", "PASS" if ok else "FAIL"); sys.exit(0 if ok else 1)
