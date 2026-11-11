"""ops 6781 - READ-ONLY audit before building the ICI browser lane (Khalid 2026-11-11 00:04 CT: "Do both"): can real
Chrome on a GitHub runner get past ici.org's Akamai wall, and do the pages it gets parse with the engines' own parsers?

For each of the three ICI weekly release pages - /research/stats/flows (Estimated Long-Term Mutual Fund Flows, the
seeded desk feed's series), /research/stats/combined_flows and /research/stats/mmf (the native engine's two
sources) - this op: (1) requests the page with urllib and the Lambda's browser headers (the 403 control); (2) opens
it in headless Chrome through Playwright and records the HTTP status, final URL, title, bot-wall markers, the
"Washington, DC; <date>" release line, every <table>'s header and first-column labels; (3) runs the native parser
ici_research.parse_release on combined_flows / mmf and the seeded parser parse_ici_table on /flows, straight from
page.content(); (4) reloads each page once more in the same context to see whether clearance sticks. A screenshot of
/flows is embedded. Nothing is written anywhere.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import importlib.util
import json
import re
import subprocess
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
ICI_SRC = ROOT / "aws" / "lambdas" / "justhodl-ici-flows" / "source"
sys.path.insert(0, str(ICI_SRC))

PAGES = [("flows", "https://www.ici.org/research/stats/flows"),
         ("combined_flows", "https://www.ici.org/research/stats/combined_flows"),
         ("mmf", "https://www.ici.org/research/stats/mmf")]
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
      "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.9"}
WALL = re.compile(r"Access Denied|Reference #\d|akamai|Pardon Our Interruption|bot detection|captcha", re.I)
RELEASE = re.compile(r"Washington,?\s+DC;?\s+([A-Z][a-z]+ \d{1,2}, \d{4})")


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except ImportError:
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as exc:  # noqa: BLE001
        log("chrome channel unavailable (%s); installing chromium" % str(exc)[:80])
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def seed_parser():
    spec = importlib.util.spec_from_file_location("ici_seed_lambda_6781", ICI_SRC / "lambda_function.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.parse_ici_table


def table_outline(html, limit=8):
    """header + first-column labels of every <table>, through the native Tables parser (lenient copy: spans allowed)"""
    import ici_research
    out = []
    try:
        parser = ici_research.Tables()
        parser.feed(html)
        for t in parser.tables:
            rows = t["rows"]
            out.append({"index": t["index"], "rows": len(rows), "header": [c[:14] for c in (rows[0] if rows else [])][:8],
                        "labels": [r[0][:22] if r else "" for r in rows[1:limit + 1]]})
    except Exception as exc:  # noqa: BLE001
        out.append({"error": "%s: %s" % (type(exc).__name__, str(exc)[:100])})
        for m in re.finditer(r"<table[^>]*>(.*?)</table>", html, re.S | re.I):
            cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", m.group(1), re.S | re.I)
            out.append({"regex_cells": [re.sub(r"<[^>]+>", "", c).strip()[:16] for c in cells[:16]]})
    return out


def main():
    from ops_report import report
    import ici_research

    now = datetime.now(timezone.utc)
    facts = {}
    with report("ops_6781_ici_chrome_probe") as r:
        r.heading("ops 6781 - ici.org through headless Chrome on the runner %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("1. the control: urllib with the Lambda's browser headers")
        for kind, url in PAGES:
            try:
                req = urllib.request.Request(url, headers=UA)
                with urllib.request.urlopen(req, timeout=30) as resp:
                    body = resp.read()
                r.log("%s: HTTP %s, %d bytes, wall markers %s" % (kind, resp.status, len(body), bool(WALL.search(body.decode("utf-8", "ignore")))))
                facts["urllib_" + kind] = "HTTP %s" % resp.status
            except urllib.error.HTTPError as exc:
                r.log("%s: HTTP %s %s" % (kind, exc.code, exc.reason))
                facts["urllib_" + kind] = "HTTP %s" % exc.code
            except Exception as exc:  # noqa: BLE001
                r.warn("%s: %s: %s" % (kind, type(exc).__name__, str(exc)[:120]))

        r.section("2. headless Chrome (Playwright), then the engines' own parsers on page.content()")
        sync_playwright = ensure_playwright(r.log)
        parse_seed = seed_parser()
        shot = None
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1366, "height": 900}, locale="en-US")
            page = ctx.new_page()
            for attempt in (1, 2):
                r.log("-- pass %d%s" % (attempt, "" if attempt == 1 else " (same context: does clearance stick?)"))
                for kind, url in PAGES:
                    t0 = time.time()
                    try:
                        resp = page.goto(url, wait_until="domcontentloaded", timeout=60000)
                        status = resp.status if resp else None
                        try:
                            page.wait_for_load_state("networkidle", timeout=20000)
                        except Exception:  # noqa: BLE001
                            pass
                        try:
                            page.wait_for_selector("table", timeout=15000)
                        except Exception:  # noqa: BLE001
                            pass
                        html = page.content()
                        acquired = datetime.now(timezone.utc).isoformat()
                        text = re.sub(r"<[^>]+>", " ", html)
                        wall = WALL.search(text)
                        rel = RELEASE.findall(re.sub(r"\s+", " ", text))
                        r.log("%s: HTTP %s in %.1fs | final %s | title %r | %d bytes | wall %s | release line %s" % (
                            kind, status, time.time() - t0, page.url[:70], (page.title() or "")[:60], len(html), wall.group(0)[:30] if wall else None, rel[:2]))
                        outline = table_outline(html)
                        r.log("  tables: %s" % json.dumps(outline, default=str)[:1500])
                        if attempt == 1:
                            facts["chrome_" + kind] = "HTTP %s, %d tables, wall %s" % (status, len(outline), bool(wall))
                        raw = html.encode("utf-8")
                        if kind in ("combined_flows", "mmf"):
                            try:
                                parsed = ici_research.parse_release(raw, kind, acquired)
                                obs = parsed["observations"]
                                latest = [o for o in obs if o["date"] == parsed["observation_dates"][-1]]
                                r.log("  native parse_release: OK release %s, dates %s, %d observations; latest %s" % (
                                    parsed["release_date"], parsed["observation_dates"], len(obs), {o["series"]: o["value"] for o in latest}))
                                if attempt == 1:
                                    facts["native_" + kind] = "OK release %s latest %s" % (parsed["release_date"], parsed["observation_dates"][-1])
                                checks = ici_research.reconcile(parsed, kind)
                                bad = [c for c in checks if c["status"] != "within_reported_rounding"]
                                r.log("  reconcile: %d identities, %d outside rounding / missing: %s" % (len(checks), len(bad), json.dumps(bad[:3])[:300]))
                            except Exception as exc:  # noqa: BLE001
                                r.log("  native parse_release: %s: %s" % (type(exc).__name__, str(exc)[:160]))
                                if attempt == 1:
                                    facts["native_" + kind] = "%s: %s" % (type(exc).__name__, str(exc)[:80])
                        if kind == "flows":
                            try:
                                tbl = parse_seed(html)
                                r.log("  seeded parse_ici_table: %d weeks %s; latest %s" % (len(tbl), sorted(tbl)[-6:], json.dumps(tbl[sorted(tbl)[-1]])[:300] if tbl else None))
                                if attempt == 1:
                                    facts["seed_flows"] = "%d weeks, latest %s" % (len(tbl), sorted(tbl)[-1] if tbl else None)
                            except Exception as exc:  # noqa: BLE001
                                r.log("  seeded parse_ici_table: %s: %s" % (type(exc).__name__, str(exc)[:160]))
                                if attempt == 1:
                                    facts["seed_flows"] = "%s" % type(exc).__name__
                            if attempt == 1:
                                shot = page.screenshot(type="jpeg", quality=55, full_page=False)
                    except Exception as exc:  # noqa: BLE001
                        r.warn("%s: %s: %s" % (kind, type(exc).__name__, str(exc)[:200]))
                        if attempt == 1:
                            facts["chrome_" + kind] = "%s" % type(exc).__name__
            ctx.close()
            browser.close()
        if shot:
            r.section("screenshot: /research/stats/flows in Chrome")
            r.log("<!-- SCREENSHOT_flows_JPEG_BASE64 -->\n%s\n<!-- /SCREENSHOT_flows_JPEG_BASE64 -->" % base64.b64encode(shot).decode())
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing written")
    sys.exit(0)


if __name__ == "__main__":
    main()
