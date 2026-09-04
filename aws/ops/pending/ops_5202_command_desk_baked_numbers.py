"""ops_5202 -- Command desk v3: clean-key registry (full re-crawl) + baked numbers gate.

ops 5201 crawled 429/429 pages (10,735 sections) and exposed two things the first crawl must not freeze:
  * seven list pages numbered every list item (brain 6,831 notes, tradingview 504, news 154, brain-compiler
    130, compound-signals 117, equity-chokepoint 95, tv-workbench 74) — registry 2.6MB;
  * heading keys swallowed their counter spans ("japan-jgbs-series-red-amber"), so a key would CHANGE
    whenever the flag count changed and the number would drift — the opposite of the point.
jh-sections now treats a container with >40 big children as ONE section (MAX_FANOUT), caps sub-panels at 16
and derives titles/keys from a heading's own text (badges/counters excluded); the homepage loads the registry
lazily; bake_sections ships a compact copy and never bakes a >200-entry map. The registry was reset to empty
in this commit (nothing referenced its numbers yet) so the FIRST frozen numbering is the clean one.

  S1  wait for the Pages deploy carrying the new jh-sections.js (MAX_FANOUT) and an empty baked map
  S2  full re-crawl (429 pages) -> config/section-registry.json (clean keys) + S3 mirror
  S3  wait for the Pages cron to bake the maps (bonds.html carries "wr":"2")
  S4  gates: fresh-load numbers are the baked ones (§2 = wr, §2.4 = Japan), brain is one list section,
      the desk palette resolves 'jgb' -> bonds#2.4 from the registry
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
B = "justhodl-dashboard-live"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
LIST_PAGES = ["brain", "tradingview", "news", "brain-compiler", "compound-signals", "equity-chokepoint", "tv-workbench"]
FAILS = []


def http(url, timeout=25):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh-ops-5202", "Cache-Control": "no-cache"}), timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


with report("ops_5202_command_desk_baked_numbers") as R:
    R.heading("ops 5202 -- command desk v3: clean-key registry + baked numbers")

    R.section("S1 Pages deploy wait (new jh-sections.js, empty baked map)")
    t0 = time.time()
    live = False
    while time.time() - t0 < 1080:
        st, js = http("https://justhodl.ai/jh-sections.js?nocache=" + str(int(time.time())))
        st2, body = http("https://justhodl.ai/bonds.html?nocache=" + str(int(time.time())))
        live = st == 200 and "MAX_FANOUT" in js and st2 == 200 and "jh-sections.js" in body and "JH_SECTION_MAP" not in body
        R.log(f"   t+{time.time() - t0:.0f}s new_module={st == 200 and 'MAX_FANOUT' in js} bonds_map_cleared={st2 == 200 and 'JH_SECTION_MAP' not in body}")
        if live:
            break
        time.sleep(20)
    if not live:
        FAILS.append("Pages deploy with the new module / cleared map not observed within 18 min")
    else:
        R.ok(f"   live after {time.time() - t0:.0f}s")

    if not FAILS:
        R.section("S2 full re-crawl -> clean-key registry")
        import build_section_registry as bsr  # noqa: E402
        reg_path = ROOT / "config" / "section-registry.json"
        registry = {"version": 1, "pages": {}}
        pages = bsr.load_pages(str(ROOT))
        t1 = time.time()
        lines = []
        crawled = bsr.crawl("https://justhodl.ai", pages, workers=5, settle_ms=3500, log=lambda m: lines.append(m))
        stats = bsr.merge(registry, crawled)
        reg_path.write_text(json.dumps(registry, indent=1, ensure_ascii=False), encoding="utf-8")
        s3.put_object(Bucket=B, Key="data/site/section-registry.json", Body=json.dumps(registry, ensure_ascii=False, separators=(",", ":")).encode("utf-8"), ContentType="application/json", CacheControl="max-age=300")
        ok = sum(1 for r in crawled.values() if r["ok"])
        R.kv(crawled=len(crawled), ok=ok, seconds=int(time.time() - t1), **stats)
        big = sorted(((len(v["sections"]), k) for k, v in crawled.items() if v["ok"]), reverse=True)[:8]
        R.log(f"   registry {registry['n_pages']} pages / {registry['n_sections']} sections / {registry['n_panels']} panels; most sections: {big}")
        bad = [l.strip()[:90] for l in lines if l.strip().startswith("ERR")]
        R.log(f"   failures ({len(bad)}): {' | '.join(bad[:10])}")
        for k in LIST_PAGES:
            rec = crawled.get(k) or {}
            R.log(f"   list page {k:20s} sections={len(rec.get('sections', []))}")
            if rec.get("ok") and len(rec["sections"]) > 60:
                FAILS.append("%s still has %d sections" % (k, len(rec["sections"])))
        if ok < len(pages) * 0.8:
            FAILS.append("crawl covered only %d/%d pages" % (ok, len(pages)))
        bonds = registry["pages"].get("bonds", {})
        wr = next((s for s in bonds.get("sections", []) if s["key"] == "wr"), None)
        R.log("   bonds keys: %s" % [(s["n"], s["key"]) for s in bonds.get("sections", [])][:8])
        R.log("   war-room panels: %s" % [(x["n"], x["key"]) for x in (wr or {}).get("sub", [])])
        if not wr or wr["n"] != "2" or not any(x["key"] == "wr/japan-jgbs" and x["n"] == "2.4" for x in wr["sub"]):
            FAILS.append("bonds registry: expected wr=2 and wr/japan-jgbs=2.4, got %s" % [(x["n"], x["key"]) for x in (wr or {}).get("sub", [])])
        # the runner commits config/ at the end of the run; bake it into a commit now so the Pages cron can pick it up
        subprocess.run(["git", "config", "user.name", "github-actions[bot]"], cwd=ROOT, check=False)
        subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], cwd=ROOT, check=False)
        subprocess.run(["git", "add", "config/section-registry.json"], cwd=ROOT, check=False)
        cm = subprocess.run(["git", "commit", "-q", "-m", "section registry: first clean-key crawl (ops 5202) [skip-deploy]"], cwd=ROOT, check=False)
        ps = subprocess.run(["git", "push", "-q", "origin", "HEAD:main"], cwd=ROOT, check=False, capture_output=True, text=True)
        if ps.returncode != 0:
            subprocess.run(["git", "pull", "-q", "--rebase", "origin", "main"], cwd=ROOT, check=False)
            ps = subprocess.run(["git", "push", "-q", "origin", "HEAD:main"], cwd=ROOT, check=False, capture_output=True, text=True)
        R.log(f"   registry committed+pushed: commit={cm.returncode == 0} push={ps.returncode == 0} {ps.stderr[-160:]}")

    if not FAILS:
        R.section("S3 wait for the Pages cron to bake the maps")
        t2 = time.time()
        baked = False
        while time.time() - t2 < 1500:
            st, body = http("https://justhodl.ai/bonds.html?nocache=" + str(int(time.time())))
            baked = st == 200 and "window.JH_SECTION_MAP=" in body and '"wr":"2"' in body and '"wr/japan-jgbs":"2.4"' in body
            R.log(f"   t+{time.time() - t2:.0f}s bonds baked={baked}")
            if baked:
                break
            time.sleep(30)
        if not baked:
            FAILS.append("bonds.html did not receive its baked map within 25 min (Pages cron)")
        else:
            st, brain = http("https://justhodl.ai/brain.html?nocache=" + str(int(time.time())))
            st2, reg = http("https://justhodl.ai/config/section-registry.json?nocache=" + str(int(time.time())))
            R.log(f"   brain.html {len(brain)//1024}KB · site registry {len(reg)//1024}KB compact={reg[:2] == '{\"'}")
            if st2 != 200 or len(reg) > 900000:
                FAILS.append("site registry copy missing or > 900KB (%d)" % len(reg))

    if not FAILS:
        try:
            import playwright  # noqa: F401
        except Exception:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
        SHOTS.mkdir(parents=True, exist_ok=True)
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True)
            except Exception:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1440, "height": 1100})

            R.section("S4 gates: baked numbers on a fresh load, brain cap, desk palette")
            pg = ctx.new_page()
            pg.goto("https://justhodl.ai/bonds.html?nocache=" + str(int(time.time())), wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(9000)
            secs = pg.evaluate("() => (window.JH_SECTIONS || []).map(s => [s.n, s.key, s.title.slice(0, 40), s.sub.map(x => x.n + ' ' + x.title.slice(0, 22))])")
            bykey = {s[1]: s for s in secs}
            for s in secs[:6]:
                R.log(f"      §{s[0]:<5} {s[1]:<30} {s[2]}" + (f"  panels: {s[3][:5]}" if s[3] else ""))
            japan = [x for x in bykey.get("wr", [None, None, None, []])[3] if "Japan" in x]
            R.log(f"   bonds fresh load: wr -> §{bykey.get('wr', ['?'])[0]} · Japan panel {japan}")
            if bykey.get("wr", ["?"])[0] != "2" or not japan or not japan[0].startswith("2.4"):
                FAILS.append("baked numbering mismatch on fresh load: wr=%s japan=%s" % (bykey.get("wr", ["?"])[0], japan))
            pg.screenshot(path=str(SHOTS / "ops5202_bonds_baked.png"))
            pg.close()
            pg = ctx.new_page()
            pg.goto("https://justhodl.ai/brain.html?nocache=" + str(int(time.time())), wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(9000)
            bs = pg.evaluate("() => (window.JH_SECTIONS || []).map(s => [s.n, s.key, s.title.slice(0, 40), s.sub.length])")
            R.log(f"   brain: {len(bs)} sections {bs[:8]}")
            if len(bs) > 60:
                FAILS.append("brain still explodes into %d sections" % len(bs))
            pg.close()
            pg = ctx.new_page()
            errors = []
            pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
            pg.goto("https://justhodl.ai/?nocache=" + str(int(time.time())), wait_until="domcontentloaded", timeout=60000)
            pg.wait_for_timeout(12000)
            pal = pg.evaluate("""async () => { const inp = document.getElementById('hd-input'); inp.focus();
                await new Promise(r => setTimeout(r, 7000)); const out = {};
                for (const q of ['jgb', 'auction grade', 'bonds#2.', 'sector heatmap']) { inp.value = q; inp.dispatchEvent(new Event('input')); await new Promise(r => setTimeout(r, 400)); out[q] = [...document.querySelectorAll('.hd-results .it')].map(x => x.querySelector('.ref').textContent + ' · ' + x.querySelector('.ti').textContent.slice(0, 30)).slice(0, 5); }
                inp.value = ''; inp.dispatchEvent(new Event('input'));
                return { pal: out, catalog: document.getElementById('hd-catalog').textContent, titles: [...document.querySelectorAll('.hd-block .t a')].map(x => x.textContent), rows: document.querySelectorAll('.hd-block tbody tr').length, embedH: Math.round((document.querySelector('.hd-block iframe') || {getBoundingClientRect: () => ({height: 0})}).getBoundingClientRect().height) }; }""")
            R.log(f"   {json.dumps(pal)[:1200]}")
            R.log(f"   page errors {errors[:2]}")
            if not any(x.startswith("bonds#2.4") for x in pal["pal"].get("jgb", [])) or pal["rows"] < 30 or errors:
                FAILS.append("desk palette/registry: %s errors=%s" % (json.dumps(pal["pal"].get("jgb"))[:160], errors[:1]))
            pg.screenshot(path=str(SHOTS / "ops5202_home.png"), full_page=True)
            pg.close()
            ctx.close()
            browser.close()

    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: clean-key registry frozen and baked; list pages are one section; the desk resolves words to numbered sections")
