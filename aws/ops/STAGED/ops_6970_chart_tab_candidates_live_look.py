"""ops 6970 - read-only: the candidate pages for a chart tab, each opened once in a real browser on justhodl.ai.

Khalid 2026-11-26: "Scan my entire system and give me list of engines i should add a tab to chart.html suggestions
only". ops 6969 found 392 pages the chart does not open whose engine wrote in the last 30 hours. A fresh document is
not a working page, so this op opens the ones worth considering (and the 17 the chart's side rail already opens but
the top bar does not) and records what a visitor gets: errors thrown, documents that fail to load, how much is on the
page, and words such as "unavailable". A small picture is kept for the likeliest.

Nothing is invoked, clicked or written. Safe to run again.
"""
from pathlib import Path
import asyncio
import base64
import gzip
import importlib.util
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops")]
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
NAME = "ops_6970_chart_tab_candidates_live_look"
RAIL = "options earnings earnings-pead vix-curve cot-extremes dark-pool econ-calendar sec-filings flow sp500 activist-13d spinoff-desk merger-arb supply-chain portfolio global-cycle bb-go".split()
PAGES = """why dossier fundamentals valuations insiders insider-clusters insider-buys squeeze short-interest-research short-pressure options-analytics rv-iv-scanner opex-calendar
earnings-whisper analyst-actions starmine eps-velocity catalyst-calendar institutional-footprint political ark chart-patterns patterns trend-reversal accumulation phases tape-reader momentum
highs-lows ma-crosses consolidation breadth-thrust market-internals katlin fortress bottom baggers upside-radar pre-pump-radar ignition bottleneck-boom capture-gap industry-rotation master-rank
opportunities resilience dislocations liquidity plumbing global-liquidity liquidity-capacity yield-curve fedwatch fed-speak vol vol-regime vix-capitulation vol-target-unwind market-map groups
rotation-dashboard factor-regime canaries regime cross-asset risk-radar leverage aaii hot-stocks news eurodollar yen-carry term-premium btc-cycle crypto-opportunities stablecoin-flow crypto-liquidity
altseason ath corpbonds eia metals-miners ipo-pipeline index-recon deal-scanner macro-leads nowcast-desk us-cycle 0dte/index capital-flow-radar etf-holdings flow-lookthrough sector-emergence
compounders deep-value gf-value beneish magic-formula smart-money attention nobrainers treasury-desk bond-desk credit-desk systemic-stress""".split()
SHOTS = set("why insiders squeeze options-analytics earnings-whisper catalyst-calendar political chart-patterns trend-reversal breadth-thrust katlin fortress baggers upside-radar liquidity plumbing yield-curve vol "
            "market-map rotation-dashboard canaries regime news btc-cycle crypto-opportunities corpbonds opex-calendar valuations industry-rotation 0dte/index options earnings vix-curve cot-extremes econ-calendar".split())
LOOK = r"""() => { const raw = (document.body && document.body.innerText) || '', all = (s) => document.querySelectorAll(s).length;
  const banner = /Market observations unavailable( or stale)?/.test(raw), t = raw.replace(/Market observations unavailable( or stale)?/g, '');   // the site's own top bar, on every page
  const hits = []; const re = /unavailable|not available|no data|stale|failed to load|could not be (read|loaded)|error loading|coming soon|withheld|not published|unqualified|no rows|loading…|loading\.\.\./ig; let m;
  while ((m = re.exec(t)) && hits.length < 4) hits.push(t.slice(Math.max(0, m.index - 30), m.index + 50).replace(/\s+/g, ' '));
  const h1 = document.querySelector('h1');
  return { title: document.title.slice(0, 70), h1: h1 ? h1.innerText.replace(/\s+/g, ' ').slice(0, 60) : null, chars: t.length, rows: all('tr'), svg: all('svg'), canvas: all('canvas'), cards: all('section, .card, .panel, article').valueOf(),
    height: document.documentElement.scrollHeight, hits: hits, tickers: (t.match(/\b[A-Z]{2,5}\b/g) || []).length, numbers: (t.match(/[-+]?\$?\d[\d,]*\.?\d*[%BMK]?/g) || []).length,
    frame: !!document.querySelector('iframe'), banner: banner, wide: document.documentElement.scrollWidth - document.documentElement.clientWidth }; }"""


async def look(browser, name, stamp, sem, out, shots):
    async with sem:
        ctx = await browser.new_context(viewport={"width": 1280, "height": 800}, user_agent=UA, device_scale_factor=0.5)
        page = await ctx.new_page()
        errs, bad = [], []
        page.on("pageerror", lambda e: errs.append(str(e)[:120]))
        page.on("response", lambda resp: bad.append("%d %s" % (resp.status, resp.url.split("justhodl.ai")[-1].split("?")[0][:60])) if resp.status >= 400 and ("/data/" in resp.url or ".json" in resp.url or "/api/" in resp.url) else None)
        rec = {"page": name}
        t0 = time.time()
        try:
            path = "/%s.html" % name if not name.endswith("/index") else "/%s.html" % name
            resp = await page.goto("%s%s?nocache=%d" % (BASE, path, stamp), wait_until="domcontentloaded", timeout=45000)
            rec["status"] = resp.status if resp else None
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:  # noqa: BLE001
                rec["busy"] = True
            await page.wait_for_timeout(1200)
            rec.update(await page.evaluate(LOOK))
            if name in SHOTS:
                shots[name] = await page.screenshot(type="jpeg", quality=45)
        except Exception as exc:  # noqa: BLE001
            rec["failed"] = "%s: %s" % (type(exc).__name__, str(exc)[:100])
        rec.update(errors=len(errs), first_error=errs[0] if errs else None, bad=sorted(set(bad))[:5], n_bad=len(set(bad)), seconds=round(time.time() - t0, 1))
        out[name] = rec
        await ctx.close()


async def run(log, stamp):
    from playwright.async_api import async_playwright
    out, shots = {}, {}
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(channel="chrome", headless=True)
        except Exception:  # noqa: BLE001
            browser = await p.chromium.launch(headless=True)
        sem = asyncio.Semaphore(4)
        await asyncio.gather(*(look(browser, n, stamp, sem, out, shots) for n in PAGES + RAIL))
        await browser.close()
    return out, shots


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    with report(NAME) as r:
        r.heading("ops 6970 - candidate pages for a chart tab, each opened once in a real browser (read-only)")
        A.ensure_playwright(r.log)
        with A.ensure_playwright(r.log)() as p:          # makes sure a browser can start (installs one if the runner has none)
            A.launch(p, r.log).close()
        out, shots = asyncio.run(run(r.log, stamp))
        r.section("what a visitor gets (page | status | errors thrown | documents failing | characters | table rows | charts | page height | words of doubt)")
        for group, names in (("not opened by the chart", PAGES), ("opened by the side rail, not on the top bar", RAIL)):
            r.log("-- %s (%d)" % (group, len(names)))
            for n in names:
                x = out.get(n) or {}
                r.log("%-26s %s err %d bad %d | %6s ch %4s tr %3s svg %2s cv %6s px %4.1fs | %s%s%s" % (
                    n[:26], x.get("status"), x.get("errors") or 0, x.get("n_bad") or 0, x.get("chars"), x.get("rows"), x.get("svg"), x.get("canvas"), x.get("height"), x.get("seconds") or 0,
                    (x.get("failed") or ""), (" E: " + x["first_error"][:70]) if x.get("first_error") else "", (" | " + " ~ ".join(x.get("hits") or [])[:230]) if x.get("hits") else ""))
        for n in PAGES + RAIL:
            if (out.get(n) or {}).get("bad"):
                r.log("failing documents on %s: %s" % (n, ", ".join(out[n]["bad"])))
        body = gzip.compress(json.dumps(out, separators=(",", ":")).encode(), 9)
        b64 = base64.b64encode(body).decode("ascii")
        r.section("attachments")
        r.lines.append("<!-- FIXTURE look %d bytes" % len(body))
        r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
        r.lines.append("FIXTURE-END -->")
        for k, img in sorted(shots.items()):
            b = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k.replace("/", "_"), len(img)))
            r.lines.extend(b[i:i + 16000] for i in range(0, len(b), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(pages=len(out), with_errors=sum(1 for x in out.values() if x.get("errors")), failed=sum(1 for x in out.values() if x.get("failed")), pictures=len(shots))
        r.ok("looked at %d pages; nothing clicked, nothing written" % len(out))


if __name__ == "__main__":
    main()
