"""ops 3653 — dedicated pages + sidebar for the geo-intel stack: geo-risk.html
(country board + headlines + GSSI-cross), portwatch.html (chokepoints|ports
boards + IMF disruptions), bis-crossborder.html (total/offshore/EM-Asia +
counterparty growth table). FORCE-pinned into nav categories; gates on served
pages (key markers) + SERVED nav-manifest containing all three hrefs."""
import json, sys, time, urllib.request
from pathlib import Path
from ops_report import report

with report("3653_geo_pages") as rep:
    rep.heading("ops 3653 — geo-stack dedicated pages + sidebar")
    out = {"gates": {}}
    fails = []

    def gate(n, ok, d):
        out["gates"][n] = {"ok": bool(ok), "detail": str(d)[:640]}
        print(("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]); rep.log(n + " " + str(ok))
        if not ok:
            fails.append(n)

    def get(u):
        return urllib.request.urlopen(urllib.request.Request(
            u + ("&" if "?" in u else "?") + "cb=" + str(int(time.time())),
            headers={"User-Agent": "Mozilla/5.0"}), timeout=30).read().decode("utf-8", "replace")

    PAGES = {
        "geo-risk.html": ["Geopolitical Risk Monitor", "NEWS LEADS",
                           "geopolitical-risk.json", "jh-nav-drawer.js"],
        "portwatch.html": ["PortWatch", "Chokepoints", "portwatch.json",
                            "jh-nav-drawer.js"],
        "bis-crossborder.html": ["BIS Cross-Border", "OFFSHORE CENTRES",
                                  "bis-crossborder.json", "jh-nav-drawer.js"],
    }
    ok1 = False; det = ""; dl = time.time() + 480
    while time.time() < dl:
        try:
            res = {}
            for pg, marks in PAGES.items():
                h = get("https://justhodl.ai/" + pg)
                res[pg] = all(m in h for m in marks)
            det = str(res)
            if all(res.values()):
                ok1 = True; break
        except Exception as e:
            det = str(e)[:160]
        time.sleep(20)
    gate("G1_pages", ok1, det)

    ok2 = False; det2 = ""
    for _ in range(10):
        try:
            mf = json.loads(get("https://justhodl.ai/nav-manifest.json"))
            hrefs = []
            for cat in (mf.get("categories") or []):
                for p in (cat.get("pages") or []):
                    hrefs.append(p.get("href"))
            need = ["/geo-risk.html", "/portwatch.html", "/bis-crossborder.html"]
            found = {n: (n in hrefs) for n in need}
            det2 = str(found) + f" total={len(hrefs)}"
            cats = {}
            for cat in (mf.get("categories") or []):
                for p in (cat.get("pages") or []):
                    if p.get("href") in need:
                        cats[p["href"]] = cat.get("name")
            det2 += f" cats={cats}"
            if all(found.values()):
                ok2 = True; break
        except Exception as e:
            det2 = str(e)[:160]
        time.sleep(25)
    gate("G2_sidebar", ok2, det2)

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("VERDICT:", out["verdict"]); rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3653.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)
