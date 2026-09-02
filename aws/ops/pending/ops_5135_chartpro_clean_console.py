"""ops_5135 -- chart-pro: dispose previous chart instances (the real "Value is null"), DataService absolute-URL fix (signal feeds were 400ing), clean console gate.

The bake step name I added in ops 5129 carried an unquoted colon; GitHub rejected the
workflow YAML and every site deploy since 15:14 UTC failed silently (5130's page probe
timed out on that, not on the client). Now that pages.yml parses again this op verifies
what the last three pushes were supposed to put on justhodl.ai:

  * data.html carries the static provider inventory (table + JSON blob) for non-JS readers
  * plumbing.html publishes the engine's layer weights (25/30/15/10/20)
  * chart-pro: AAPL / NVDA / SSE:000001 / TVC:VIX / XETR:DAX chart natively from the
    warehouse with daily bars, no TradingView iframe, no paywall text, no page errors
Gates: all three pages live within 15 min; probes as stated.
"""
import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402


def http(url, timeout=60):
    req = urllib.request.Request(url + ("&" if "?" in url else "?") + "v=" + str(int(time.time())), headers={"User-Agent": "justhodl-ops-5135", "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def main():
    with report("5135-chartpro-clean-console") as r:
        r.heading("ops 5135 -- chart-pro: no leaked chart instances, signal feeds loading, clean console")
        fails = []
        r.section("S1 static content live?")
        t0 = time.time()
        got = {"data": False, "plumbing": False, "chart": False}
        while time.time() - t0 < 900 and not all(got.values()):
            try:
                d = http("https://justhodl.ai/data.html")
                got["data"] = b'id="data-plane-inventory"' in d and b"inventory-static:begin" in d
                p = http("https://justhodl.ai/plumbing.html")
                got["plumbing"] = b'<span class="weight">30%</span>' in p and b'<span class="weight">35%</span>' not in p
                c = http("https://justhodl.ai/chart-pro.html")
                got["chart"] = b"ops 5135: every signal feed was fetched as PROXY" in c and b"static reapDetached()" in c
            except Exception as e:  # noqa: BLE001
                r.log(f"  fetch failed: {str(e)[:80]}")
            r.log(f"  live: {json.dumps(got)}")
            if all(got.values()):
                break
            time.sleep(30)
        for k, v in got.items():
            if not v:
                fails.append(f"{k}.html not live with the expected content")
        if got["data"]:
            i0 = d.index(b'id="data-plane-inventory"')
            blob = d[i0:].split(b">", 1)[1].split(b"</script>", 1)[0]
            inv = json.loads(blob.decode("utf-8", "replace"))
            r.ok(f"  data.html static inventory: {len(inv.get('providers') or [])} providers, as_of {inv.get('as_of')}, totals {inv.get('totals')}")
            if len(inv.get("providers") or []) < 40:
                fails.append("static inventory carries fewer than 40 providers")

        r.section("S2 chart-pro native charts (headless Chrome)")
        if got["chart"]:
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
                ctx = browser.new_context(viewport={"width": 1500, "height": 1000})
                page = ctx.new_page()
                errors = []
                page.on("pageerror", lambda e: errors.append({"msg": str(e)[:120], "stack": (getattr(e, "stack", "") or "")[:600], "sym": page.evaluate("() => State.activeTicker")}))
                bad_resp = []
                page.on("response", lambda resp: bad_resp.append((resp.status, resp.url[:140])) if resp.status >= 400 else None)
                page.goto("https://justhodl.ai/chart-pro.html", wait_until="domcontentloaded", timeout=90000)
                page.wait_for_timeout(9000)
                r.log(f"  errors after initial load ({page.evaluate('() => State.activeTicker')}): {json.dumps(errors[:3])[:600]}")
                for sym in ("AAPL", "NVDA", "SSE:000001", "TVC:VIX", "XETR:DAX", "X:BTCUSD"):
                    page.evaluate("(s) => ChartController.loadTicker(s)", sym)
                    page.wait_for_timeout(9000)
                    pr = page.evaluate("""() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent,
                        iframe: !!document.querySelector('#tv-container-0 iframe'), paywall: document.body.innerText.includes('available on TradingView'), nativeSrc: State.nativeSrc || null })""")
                    r.log(f"  {sym}: {json.dumps(pr)[:300]}")
                    meta = str(pr.get("meta") or "")
                    if pr.get("iframe") or pr.get("paywall"):
                        fails.append(f"{sym}: TradingView widget/paywall")
                    if "warehouse" not in meta and "warehouse" not in str(pr.get("nativeSrc") or ""):
                        fails.append(f"{sym}: not from the warehouse: {meta[:120]}")
                    import re as _re
                    # the meta shows the 1Y WINDOW for equities (250 bars); history depth is the "since YYYY" in nativeSrc
                    m = _re.search(r"since (\d{4})", str(pr.get("nativeSrc") or "") + " " + meta)
                    if sym in ("AAPL", "NVDA") and (not m or int(m.group(1)) > 2000):
                        fails.append(f"{sym}: warehouse history does not reach back ({pr.get('nativeSrc')})")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5135-chartpro-native.jpg"), type="jpeg", quality=60)
                errs = [e for e in errors if "TradingView" not in json.dumps(e)]
                for e in errs[:8]:
                    r.log(f"  page error: {json.dumps(e)[:700]}")
                if errs:
                    fails.append(f"page errors {len(errs)}: {json.dumps(errs[:1])[:300]}")
                from collections import Counter
                cnt = Counter((st, u.split("?")[0]) for st, u in bad_resp)
                for (st, u), n in cnt.most_common(12):
                    r.log(f"  {n:>3}x HTTP {st} {u}")
                bad_sym = [u for st, u in bad_resp if ("/fundamentals?" in u or "/news?" in u) and "%3A" in u]
                if bad_sym:
                    fails.append(f"fundamentals/news still fetched for non-equity ids: {bad_sym[:2]}")
                doubled = [u for st, u in bad_resp if "workers.dev/https://" in u]
                if doubled:
                    fails.append(f"doubled PROXY urls still requested: {doubled[:2]}")
                sig = page.evaluate("() => ({ cascade: !!(State.cascade && Object.keys(State.cascade).length), insider: !!State.insider, options: !!State.options, live: (NativeChart._live || []).length })")
                r.log(f"  signal feeds loaded: {json.dumps(sig)}")
                if not sig.get("cascade"):
                    fails.append("cascade feed still empty after the absolute-URL fix")
                if (sig.get("live") or 0) > 6:
                    fails.append(f"chart instances leaking: {sig.get('live')} live")
                page.close()
                ctx.close()
                browser.close()

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok("PASS_ALL: data.html inventory static, plumbing weights corrected, chart-pro charts every symbol from the warehouse with daily history")


if __name__ == "__main__":
    main()
