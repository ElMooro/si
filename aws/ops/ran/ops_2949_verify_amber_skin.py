#!/usr/bin/env python3
"""ops 2949 — verify the colors-only amber reskin of the restored homepage, LIVE.
Proves: (1) live / still the Operator Console with all structure intact,
(2) the jh-amber-skin block is being served with the exact morning palette,
(3) no page regressions anywhere else (sample sweep + nav manifest).
Read-only."""
import json, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from ops_report import report

BASE = "https://justhodl.ai"

def get(path, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"{BASE}/{path}?_={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=to)
        return r.getcode(), r.read()
    except Exception:
        return None, b""

def main():
    with report("2949_verify_amber_skin") as rep:
        fails = []
        home = ""
        for _ in range(5):
            c, b = get("")
            home = b.decode("utf-8", "replace")
            if c == 200 and "jh-amber-skin" in home:
                break
            time.sleep(25)
        ok_skin = ("jh-amber-skin" in home and "--bg-base:#0C0B09" in home
                   and "--cyan:#F0B429" in home)
        ok_page = ("Operator Console" in home and "JH COMMAND CENTER v2.0" not in home)
        scripts = home.count("<script")
        rep.kv(skin_served=ok_skin, operator_console=ok_page, script_tags=scripts,
               skin_blocks=home.count("jh-amber-skin"))
        if not ok_skin: fails.append("amber skin block not live")
        if not ok_page: fails.append("homepage identity wrong")
        # source has 30; pages.yml bake steps legitimately inject (footer/rail) -> allow small delta
        if not (30 <= scripts <= 33): fails.append(f"script count drift ({scripts} outside 30-33)")

        sample = ["today.html", "options.html", "onchain.html", "why.html",
                  "flows.html", "engines.html", "desk-v2.html", "ai_predictions.html"]
        with ThreadPoolExecutor(max_workers=8) as ex:
            res = dict(zip(sample, ex.map(get, sample)))
        bad = [p for p, (c, b) in res.items() if c != 200 or len(b) < 3000]
        rep.kv(sample_pages_ok=f"{len(sample)-len(bad)}/{len(sample)}",
               bad_pages=",".join(bad) or "none")
        if bad: fails.append(f"pages failing: {bad}")

        c, b = get("nav-manifest.json")
        navn = b.count(b".html") if c == 200 else 0
        rep.kv(nav_manifest_html_refs=navn)
        if navn < 300: fails.append(f"nav manifest thin ({navn})")

        line = f"skin={ok_skin} console={ok_page} scripts={scripts} pages={len(sample)-len(bad)}/{len(sample)} nav={navn}"
        print(line); rep.kv(summary=line)
        if fails:
            for f in fails: rep.fail(f)
            print("FAILURES: " + " | ".join(fails)); sys.exit(1)
        rep.ok("amber skin live, structure intact, site healthy")

if __name__ == "__main__":
    main()
