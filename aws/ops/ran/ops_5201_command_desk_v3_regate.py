"""ops_5201 -- Command desk v3 (re-gate after ops 5200) (numbered sections fleet-wide + composable homepage) -- deploy gate + registry crawl.

Khalid (2026-09-04): "index.html is designed so I can add as many engines as I want, also the part of the
engines or page that I want, either from the engine itself or the display. I don't like the design as is
now… number every section in pages in my system so I can add the engine or the page and then within that
add the sections by number… the main page design should be like bonds."

Shipped in the same push (pages.yml deploys them):
  jh-sections.js            every page numbers its real blocks §1, §1.1… after render; ?embed=n isolates a block
  scripts/bake_sections.py  injects the module + each page's baked key→number map (append-only registry)
  index.html/home.js/.css   the command desk: page#n / engine/panel / page addresses, war-room table renderer
  workspace.html            yesterday's engine-workspace homepage, preserved
  config/home-layout.json   default desk; config/section-registry.json  (this op fills it)

  ops 5200 found: engine blocks empty because the manifest saw no output feed for bond-warroom /
  auction-desk (they write through a _put_json wrapper — gen_engine_manifest now follows wrappers:
  662 → 737 engines with feeds; home.js also falls back to the data/<engine>.json convention);
  bonds#1 is the 124px title strip, the war room is §2 (default layout now bonds#2); provisional
  numbers were first-pass order, the crawl now numbers canonically in document order.

  S1  wait for the Pages deploy (home.js on /, jh-sections.js on bonds.html)
  S2  headless Chrome gate on the desk: blocks + war-room rows + live embed height + 0 page errors + 390px overflow hunter
  S3  bonds.html / auctions.html numbered: print their §sections (the badges are the user-visible proof)
  S4  crawl every page → config/section-registry.json (append-only merge) + S3 data/site/section-registry.json
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
FAILS = []


def http(url, timeout=25):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh-ops-5201", "Cache-Control": "no-cache"}), timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return 0, str(e)


with report("ops_5201_command_desk_v3_regate") as R:
    R.heading("ops 5201 -- command desk v3 re-gate: numbered sections fleet-wide + composable homepage")

    # ---------------------------------------------------------------- S1
    R.section("S1 Pages deploy wait")
    t0 = time.time()
    live_home = live_bonds = False
    while time.time() - t0 < 1080:
        st, body = http("https://justhodl.ai/?nocache=" + str(int(time.time())))
        live_home = st == 200 and "home.js" in body and "hd-grid" in body and '"sec": "2"' in http("https://justhodl.ai/config/home-layout.json?nocache=" + str(int(time.time())))[1]
        st2, body2 = http("https://justhodl.ai/bonds.html?nocache=" + str(int(time.time())))
        live_bonds = st2 == 200 and "jh-sections.js" in body2
        R.log(f"   t+{time.time() - t0:.0f}s home={live_home} bonds_numbered={live_bonds}")
        if live_home and live_bonds:
            break
        time.sleep(20)
    if not (live_home and live_bonds):
        FAILS.append("Pages deploy not observed within 18 min (home=%s bonds=%s)" % (live_home, live_bonds))
    else:
        R.ok(f"   Pages deploy live after {time.time() - t0:.0f}s")

    # ---------------------------------------------------------------- S2 / S3
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

            R.section("S2 command desk render gate")
            for width, height in ((1440, 1200), (390, 844)):
                ctx = browser.new_context(viewport={"width": width, "height": height}, is_mobile=width < 700)
                pg = ctx.new_page()
                errors, csp = [], []
                pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                pg.on("console", lambda m: csp.append(m.text[:160]) if "Content Security Policy" in m.text else None)
                pg.goto("https://justhodl.ai/?nocache=" + str(int(time.time())), wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(14000)
                facts = pg.evaluate("""() => ({
                    blocks: document.querySelectorAll('.hd-block').length,
                    refs: [...document.querySelectorAll('.hd-block .ref')].map(x => x.textContent),
                    rows: document.querySelectorAll('.hd-block tbody tr').length,
                    flags: document.querySelectorAll('.hd-block .hd-flag').length,
                    reds: document.querySelectorAll('.hd-block .hd-flag.RED').length,
                    kpis: document.querySelectorAll('.hd-kpi').length,
                    embeds: [...document.querySelectorAll('.hd-block iframe')].map(f => [f.getAttribute('src'), Math.round(f.getBoundingClientRect().height)]),
                    fresh: [...document.querySelectorAll('.hd-block .fresh')].map(x => x.textContent),
                    score: document.getElementById('hd-score').textContent, regime: document.getElementById('hd-regime').textContent,
                    headline: document.getElementById('hd-headline').textContent.slice(0, 120),
                    badges: [...document.querySelectorAll('#hd-grid .jh-secbadge')].map(x => x.textContent),
                    directory: document.querySelectorAll('#hd-dir-cats li').length,
                    catalog: document.getElementById('hd-catalog').textContent.slice(0, 120),
                    chrome: !!document.getElementById('jh-chrome'),
                    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth })""")
                pg.screenshot(path=str(SHOTS / f"ops5201_home_{width}.png"), full_page=width > 700)
                R.log(f"   {width}px: {json.dumps(facts)[:900]}")
                R.log(f"   {width}px: page errors {errors[:3]} csp {csp[:2]}")
                embed_h = max([h for _, h in facts["embeds"]] or [0])
                if facts["blocks"] < 6 or facts["rows"] < 30 or facts["flags"] < 20 or facts["directory"] < 300 or errors or csp or not facts["chrome"]:
                    FAILS.append("%dpx desk render: blocks=%s rows=%s dir=%s errors=%s csp=%s chrome=%s" % (width, facts["blocks"], facts["rows"], facts["directory"], errors[:2], csp[:1], facts["chrome"]))
                if width > 700 and embed_h < 250:
                    FAILS.append("desktop: the bonds#1 live embed never reported a height (%s)" % facts["embeds"])
                if width == 390 and facts["overflow"] > 0:
                    offenders = pg.evaluate("""() => { const cw = document.documentElement.clientWidth; const out = [];
                        const clipped = el => { for (let a = el.parentElement; a; a = a.parentElement) { const ox = getComputedStyle(a).overflowX; if (ox === 'auto' || ox === 'hidden' || ox === 'scroll' || ox === 'clip') return true; } return false; };
                        for (const el of document.querySelectorAll('body *')) { const r = el.getBoundingClientRect(); if (r.right > cw + 1 && r.width > 20 && !clipped(el)) { const cs = getComputedStyle(el); out.push([el.tagName + (el.id ? '#' + el.id : '') + (el.className && typeof el.className === 'string' ? '.' + el.className.split(' ').slice(0,2).join('.') : ''), Math.round(r.right - cw), Math.round(r.width), cs.position, Math.round(r.left)]); } }
                        return { body: document.body.scrollWidth, html: document.documentElement.scrollWidth, cw, top: out.sort((a, b) => b[1] - a[1]).slice(0, 10) }; }""")
                    R.log("   390px overflow offenders: %s" % json.dumps(offenders))
                    FAILS.append("390px overflow %dpx" % facts["overflow"])
                # add-bar grammar, live: type an address and let the palette resolve it
                if width > 700:
                    pal = pg.evaluate("""async () => { const H = window.JustHodlHome; const out = {};
                        const inp = document.getElementById('hd-input');
                        for (const q of ['bonds#1', 'bond-warroom/japan', 'auctions', 'jgb']) { inp.value = q; inp.dispatchEvent(new Event('input')); await new Promise(r => setTimeout(r, 300)); out[q] = [...document.querySelectorAll('.hd-results .it .ref')].map(x => x.textContent).slice(0, 5); }
                        inp.value = ''; inp.dispatchEvent(new Event('input')); return out; }""")
                    R.log("   palette: %s" % json.dumps(pal)[:600])
                    if not pal.get("bonds#1") or not pal.get("bond-warroom/japan"):
                        FAILS.append("palette did not resolve bonds#1 / bond-warroom/japan: %s" % json.dumps(pal)[:200])
                ctx.close()

            R.section("S3 numbered pages (bonds, auctions) + embed isolation")
            ctx = browser.new_context(viewport={"width": 1440, "height": 1100})
            for key in ("bonds", "auctions", "global-cycle", "sectors"):
                pg = ctx.new_page()
                errs = []
                pg.on("pageerror", lambda e: errs.append(str(e)[:120]))
                pg.goto(f"https://justhodl.ai/{key}.html?nocache={int(time.time())}", wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(9000)
                secs = pg.evaluate("() => { try { window.JustHodlSections.canonical(); } catch (e) {} return (window.JH_SECTIONS || []).map(s => [s.n, s.key, s.title.slice(0, 50), s.sub.map(x => x.n + ' ' + x.title.slice(0, 28))]); }")
                badges = pg.evaluate("() => document.querySelectorAll('.jh-secbadge').length")
                R.log(f"   {key}: {len(secs)} sections, {badges} badges, errors {errs[:2]}")
                for s in secs[:14]:
                    R.log(f"      §{s[0]:<5} {s[1]:<34} {s[2]}" + (f"  panels: {s[3][:6]}" if s[3] else ""))
                if key == "bonds":
                    pg.screenshot(path=str(SHOTS / "ops5201_bonds_numbered.png"), full_page=False)
                    if len(secs) < 4 or badges < 4:
                        FAILS.append("bonds.html numbering: %d sections %d badges" % (len(secs), badges))
                    # embed isolation of the first section
                    first = next((x[0] for x in secs if x[1] == "wr"), secs[0][0] if secs else "1")
                    pe = ctx.new_page()
                    pe.goto(f"https://justhodl.ai/bonds.html?embed={first}&nocache={int(time.time())}", wait_until="domcontentloaded", timeout=60000)
                    pe.wait_for_timeout(8000)
                    iso = pe.evaluate("""() => ({ embed: document.documentElement.classList.contains('jh-embed'), target: !!document.querySelector('[data-jh-embed-target]'),
                        visibleTop: [...document.body.children].filter(c => getComputedStyle(c).display !== 'none').map(c => c.tagName + (c.id ? '#' + c.id : '')),
                        chromeHidden: !document.getElementById('jh-chrome') || getComputedStyle(document.getElementById('jh-chrome')).display === 'none',
                        h: Math.round(document.querySelector('[data-jh-embed-target]') ? document.querySelector('[data-jh-embed-target]').getBoundingClientRect().height : 0) })""")
                    pe.screenshot(path=str(SHOTS / "ops5201_bonds_embed_s1.png"), full_page=True)
                    R.log(f"   bonds?embed={first}: {json.dumps(iso)}")
                    if not (iso["embed"] and iso["target"] and iso["chromeHidden"] and iso["h"] > 200):
                        FAILS.append("embed isolation failed: %s" % json.dumps(iso)[:200])
                    pe.close()
                pg.close()
            ctx.close()
            browser.close()

    # ---------------------------------------------------------------- S4
    if not FAILS:
        R.section("S4 fleet crawl -> section registry (append-only)")
        import build_section_registry as bsr  # noqa: E402
        reg_path = ROOT / "config" / "section-registry.json"
        try:
            registry = json.loads(reg_path.read_text(encoding="utf-8"))
        except Exception:
            registry = {"version": 1, "pages": {}}
        pages = bsr.load_pages(str(ROOT))
        R.log(f"   {len(pages)} pages from nav-manifest (+index)")
        t1 = time.time()
        lines = []
        crawled = bsr.crawl("https://justhodl.ai", pages, workers=5, settle_ms=3500, log=lambda m: lines.append(m))
        ok = sum(1 for r in crawled.values() if r["ok"])
        stats = bsr.merge(registry, crawled)
        reg_path.write_text(json.dumps(registry, indent=1, ensure_ascii=False), encoding="utf-8")
        s3.put_object(Bucket=B, Key="data/site/section-registry.json", Body=json.dumps(registry, ensure_ascii=False).encode("utf-8"), ContentType="application/json", CacheControl="max-age=300")
        R.kv(crawled=len(crawled), ok=ok, seconds=int(time.time() - t1), **stats)
        R.log("   registry: %d pages, %d sections, %d panels -> config/section-registry.json + s3://%s/data/site/section-registry.json" % (registry["n_pages"], registry["n_sections"], registry["n_panels"], B))
        bad = [l for l in lines if l.strip().startswith("ERR")]
        R.log("   failures (%d): %s" % (len(bad), " | ".join(x.strip()[:90] for x in bad[:12])))
        top = sorted(((len(v["sections"]), k) for k, v in crawled.items() if v["ok"]), reverse=True)[:8]
        R.log("   most sections: %s" % top)
        zero = [k for k, v in crawled.items() if v["ok"] and not v["sections"]]
        R.log("   rendered but 0 sections (%d): %s" % (len(zero), zero[:25]))
        if ok < len(pages) * 0.8:
            FAILS.append("crawl covered only %d/%d pages" % (ok, len(pages)))
        (ROOT / "aws" / "ops" / "reports" / "latest" / "ops5201_crawl.json").write_text(json.dumps({"stats": stats, "failures": bad, "zero": zero}, indent=1), encoding="utf-8")

    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: command desk v3 live, every page numbered, registry written (the next Pages bake freezes the numbers)")
