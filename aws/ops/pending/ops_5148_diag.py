"""ops_5148 -- two diagnoses: (1) FRED 400 for DGS2MO (which id is the 2-month CMT?), (2) why the
dropdown never gets facets/sources on the live page although the endpoint returns them.
Read-only except the report. Never RED.
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

KEY = "2f057499936072679d8843d7fce99989"


def get(url, timeout=60):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5148"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")


def main():
    with report("5148-diag") as r:
        r.heading("ops 5148 -- FRED 2-month id + dropdown facets diagnosis")
        r.section("A. FRED")
        for sid in ("DGS2MO", "DGS4MO", "DGS1MO", "DGS3MO"):
            st, body = get(f"https://api.stlouisfed.org/fred/series?series_id={sid}&api_key={KEY}&file_type=json")
            r.log(f"  series {sid}: HTTP {st} {body[:160]}")
            time.sleep(0.5)
        st, body = get(f"https://api.stlouisfed.org/fred/series/search?search_text={urllib.parse.quote('2-Month Treasury Constant Maturity')}&api_key={KEY}&file_type=json&limit=8")
        try:
            hits = [(x["id"], x["title"][:70], x.get("frequency_short")) for x in json.loads(body).get("seriess", [])]
        except Exception:  # noqa: BLE001
            hits = body[:200]
        r.log(f"  search '2-Month Treasury Constant Maturity': {hits}")
        st, body = get(f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS2MO&api_key={KEY}&file_type=json&observation_start=1776-07-04&limit=100000")
        r.log(f"  observations DGS2MO full-history URL: HTTP {st} {body[:160]}")
        st, body = get(f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS2MO&api_key={KEY}&file_type=json&limit=5")
        r.log(f"  observations DGS2MO minimal URL: HTTP {st} {body[:160]}")

        r.section("B. dropdown runtime")
        try:
            import playwright  # noqa: F401
        except Exception:  # noqa: BLE001
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True)
            except Exception:  # noqa: BLE001
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1920, "height": 1000})
            page = ctx.new_page()
            logs = []
            page.on("console", lambda m: logs.append(m.type + ": " + m.text[:200]))
            page.on("pageerror", lambda e: logs.append("pageerror: " + str(e)[:300]))
            page.goto("https://justhodl.ai/chart-pro.html?v=" + str(int(time.time())), wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(9000)
            has = page.evaluate("() => ({ marker: /this\\._facets = d\\.facets/.test(HeaderSearch.onInput.toString()), src: HeaderSearch.onInput.toString().slice(0, 200) })")
            r.log(f"  served page has the facets code: {json.dumps(has)[:400]}")
            page.click("#search-input")
            page.type("#search-input", "US02MY", delay=40)
            page.wait_for_timeout(7000)
            ev = page.evaluate("""async () => { const keys = Array.from(SymDir._cache.keys()); const out = { seq: HeaderSearch._seq, facets: HeaderSearch._facets, keys, groups: Object.fromEntries(Object.entries(HeaderSearch._groups || {}).map(([k, v]) => [k, (v || []).length])) };
                const k = keys.find(x => x.startsWith('us02my|40')); if (k) { try { const d = await SymDir._cache.get(k); out.cached = { facets: d.facets, failed: d.failed, rows: (d.rows || []).map(x => x.id).slice(0, 3), err: d.error }; } catch (e) { out.cachedErr = String(e); } }
                out.instr = (HeaderSearch._groups.instruments || []).map(x => ({ id: x.id, sources: x.sources })); return out; }""")
            r.log(f"  runtime: {json.dumps(ev)[:900]}")
            r.log(f"  console: {json.dumps(logs[-8:])[:600]}")
            page.close()
            ctx.close()
            browser.close()
        r.ok("diagnosis complete")


if __name__ == "__main__":
    main()
