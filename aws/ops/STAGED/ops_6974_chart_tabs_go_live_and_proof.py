"""ops 6974 - the twelve chart tabs, live: one publication, then the proof in a real browser.

Khalid 2026-11-26: "Add these first and audit their engine before you add them and see if you can expand it to cover
more data and that all data from the engine is displaying on the page". CLAUDE.md rule 5: click the control yourself and
quote the proof.

  1. ONE WRITE, at most: the catalyst calendar is published once from this runner with the engine's own code, only
     if the live document is not yet version 2.0.0 (the engine's own run is 12:00 UTC). No Lambda is invoked.
  2. The engines read back: which changes are live and which wait for the engine's own run.
  3. chart.html in real Chrome: each of the twelve buttons is clicked; the page it opens is checked inside the
     workspace - the Engine data section, every document loaded, the count of values against this op's own count of
     the same document, the Every field view, a table sorted both ways and opened whole.

Safe to run again (the write happens once).
"""
from pathlib import Path
import base64
import gzip
import importlib.util
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
NAME = "ops_6974_chart_tabs_go_live_and_proof"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
RESEARCH = "https://justhodl-data-proxy.raafouis.workers.dev/equity-research/"
SYMBOL = "NVDA"
TABS = [  # button, label, page, registry name, documents
    ("btn-rsch", "Research", "/why.html", ["ticker"]),
    ("btn-ins", "Insiders", "/insiders.html", ["data/insider-trades.json", "data/insider-clusters.json", "data/insider-sell-cluster.json", "data/insider-industry-cluster.json", "data/insider-trades-full.json"]),
    ("btn-opta", "Options", "/options-analytics.html", ["data/options-analytics.json", "data/opex-calendar.json", "data/options-analytics-iv-history.json"]),
    ("btn-val", "Value", "/valuations.html", ["data/stock-valuations.json", "data/valuations-decisive-call.json"]),
    ("btn-cbond", "Corp bonds", "/corpbonds.html", ["data/corp-bonds.json"]),
    ("btn-plumb", "Plumbing", "/plumbing.html", ["data/plumbing-stress.json", "data/plumbing-history.json"]),
    ("btn-liq", "Liquidity", "/liquidity.html", ["liquidity-data.json", "data/liquidity-flow.json"]),
    ("btn-can", "Canaries", "/canaries.html", ["data/canary-warroom.json", "data/warroom-history.json", "data/warroom-weights.json", "data/warroom-replay.json", "data/canaries-decisive-call.json"]),
    ("btn-brd", "Breadth", "/breadth-thrust.html", ["data/breadth-thrust.json", "data/breadth-history.json"]),
    ("btn-rev", "Reversal", "/trend-reversal.html", ["data/trend-reversal.json", "data/trend-reversal-history.json"]),
    ("btn-cal", "Calendar", "/catalyst-calendar.html", ["data/catalyst-calendar.json"]),
    ("btn-cryp", "Crypto", "/btc-cycle.html", ["data/onchain-ratios.json", "data/cryptoquant-onchain.json"]),
]
SHOTS = {"btn-ins", "btn-opta", "btn-cal", "btn-cryp", "btn-rsch"}

DOCS = r"""() => Array.from(document.querySelectorAll('.jed-doc')).map(d => { const st = d.querySelector('.jed-st').textContent, m = /([\d,]+) values in ([\d,]+) fields/.exec(st);
  return { doc: d.getAttribute('data-doc'), name: d.querySelector('summary b').textContent, st: st, bad: !!d.querySelector('.jed-msg.bad'), loading: /not loaded yet|waiting/.test(st) || !!d.querySelector('.jed-body > .jed-msg:not(.bad)'),
    values: m ? +m[1].replace(/,/g, '') : null, fields: m ? +m[2].replace(/,/g, '') : null, blocks: d.querySelectorAll('.jed-blk').length, tiles: d.querySelectorAll('.jed-kv > div').length }; })"""


def fetch(url, timeout=120):
    req = urllib.request.Request(url if url.startswith("http") else BASE + url, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
    return gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw


def count(node):
    """This op's own count of a document's values: every scalar is one, an empty list or object is one."""
    if node is None or not isinstance(node, (dict, list)):
        return 1
    kids = list(node.values()) if isinstance(node, dict) else node
    return sum(count(k) for k in kids) if kids else 1


def main():
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    stamp = int(time.time())
    shots, fails = {}, []

    with report(NAME) as r:
        r.heading("ops 6974 - the twelve chart tabs, live: one publication, then the proof in a real browser")

        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)
            return cond

        # ------------------------------------------------------------------ 0
        r.section("0. The live site serves the new files")
        served = False
        for attempt in range(80):
            try:
                html = fetch("/chart.html?probe=%d" % (stamp + attempt)).decode("utf-8", "replace")
                kit = fetch("/jh-engine-data.js?probe=%d" % (stamp + attempt)).decode("utf-8", "replace")
                page = fetch("/options-analytics.html?probe=%d" % (stamp + attempt)).decode("utf-8", "replace")
                if html.count("&tabs=20261126") == 2 and "JHEngineData" in kit and 'data-jed="options"' in page:
                    served = True
                    break
            except Exception as exc:  # noqa: BLE001
                r.log("probe %d: %s" % (attempt, str(exc)[:80]))
            time.sleep(15)
        ck(served, "chart.html carries the new mark on both scripts, the kit is served and the options page mounts it")
        if not served:
            r.kv(failed_checks=len(fails))
            r.fail("the site does not serve the new files yet")
            sys.exit(1)

        # ------------------------------------------------------------------ 1
        r.section("1. The catalyst calendar, published once from the runner with the engine's own code")
        try:
            import boto3
            s3 = boto3.client("s3", region_name="us-east-1")

            def kept(key):                      # straight from the bucket: the site's cache holds a document for ten minutes
                return json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key=key)["Body"].read())
            live = kept("data/catalyst-calendar.json")
            if live.get("version") == "2.0.0":
                r.log("the live calendar is already 2.0.0 (written %s): nothing written" % live.get("as_of"))
            else:
                src = ROOT / "aws" / "lambdas" / "justhodl-catalyst-calendar" / "source"
                sys.path.insert(0, str(src))
                cspec = importlib.util.spec_from_file_location("eng_catalyst", str(src / "lambda_function.py"))
                C = importlib.util.module_from_spec(cspec)
                cspec.loader.exec_module(C)
                out = C.lambda_handler({}, None)
                r.log("published: " + str(out.get("body"))[:400])
                live = kept("data/catalyst-calendar.json")
            ck(live.get("version") == "2.0.0" and (live.get("n_ahead") or 0) >= 100, "the live calendar is 2.0.0 with %s events ahead (%s in all)" % (live.get("n_ahead"), live.get("n_events")))
            r.log("ahead by type: " + json.dumps(live.get("by_type_ahead")))
            gov = [e for e in live.get("events", []) if e.get("type") == "GOV_CONTRACT"]
            r.log("contract rows: %d | first: %s" % (len(gov), json.dumps([(e["date"], e["title"], e["impact"]) for e in gov[:2]], ensure_ascii=False)))
            ck(all(e["title"].startswith("Federal award changed") and e["impact"] == "LOW" for e in gov if e.get("date_basis") != "Action Date"), "a contract row dated by its last change says so")
        except Exception as exc:  # noqa: BLE001
            ck(False, "calendar publication raised %s: %s" % (type(exc).__name__, str(exc)[:300]))

        # ------------------------------------------------------------------ 2
        r.section("2. The engines, read back")
        state = {}
        try:
            o = kept("data/options-analytics.json")
            state["options"] = "version %s, %s of %s names analysed, written %s" % (o.get("version"), o.get("n_analyzed"), o.get("universe_size"), str(o.get("generated_at"))[:16])
            n = kept("data/onchain-ratios.json")
            state["on-chain"] = "written %s, CryptoQuant joined: %s, Ethereum metrics: %d" % (n.get("generated_at"), "cryptoquant" in n, len((n.get("eth") or {}).get("cryptoquant") or {}))
            i = kept("data/insider-trades.json")
            state["insiders"] = "version %s, written %s, buying total $%s, set aside %s" % (i.get("version"), i.get("generated_at"), format((i.get("stats") or {}).get("total_value_usd") or 0, ",.0f"), len(i.get("set_aside") or []) if "set_aside" in i else "not yet")
            p = kept("data/plumbing-stress.json")
            state["plumbing"] = "header %s indicators, %s with data, written %s" % (p.get("n_indicators"), p.get("n_with_data"), p.get("as_of"))
        except Exception as exc:  # noqa: BLE001
            r.log("read-back raised %s: %s" % (type(exc).__name__, str(exc)[:200]))
        for k, v in state.items():
            r.log("%s: %s" % (k, v))

        # ------------------------------------------------------------------ 3
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            r.section("3. chart.html in a real browser: the twelve buttons")
            ctx = browser.new_context(viewport={"width": 1500, "height": 1000}, user_agent=UA)
            page = ctx.new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            page.goto("%s/chart.html?s=%s&nocache=%d" % (BASE, SYMBOL, stamp), wait_until="domcontentloaded")
            page.wait_for_selector("#btn-cryp", timeout=90000, state="attached")
            page.wait_for_function("() => (window.lastBars || []).length > 300 && typeof window.jhOpenWorkspace === 'function'", timeout=90000)
            page.wait_for_timeout(1200)
            bar = page.evaluate("() => ({ desks: Array.from(document.querySelectorAll('#tfbar .wsdesk')).map(x => x.id), on: (document.querySelector('#tabs .tab.on') || {dataset: {}}).dataset.id || null, ox: document.documentElement.scrollWidth - document.documentElement.clientWidth })")
            ids = [t[0] for t in TABS]
            at = bar["desks"].index("btn-rsch") if "btn-rsch" in bar["desks"] else -1
            r.log("desk buttons on the bar (%d): %s" % (len(bar["desks"]), " ".join(bar["desks"])))
            ck(at > 0 and bar["desks"][at:at + 12] == ids and bar["desks"][at - 1] == "btn-snip" and len(set(bar["desks"])) == len(bar["desks"]), "the twelve buttons are on the bar once each, in order, after Khalid")
            ck(all(b in bar["desks"] for b in ("btn-macro", "btn-flows", "btn-mflow", "btn-cbe", "btn-cds", "btn-13f", "btn-alrt")) and bar["on"] == SYMBOL and bar["ox"] == 0, "every earlier button is still there; the chart is on %s; no sideways scroll" % SYMBOL)

            def show_bar():
                page.mouse.move(700, 3)
                try:
                    page.wait_for_selector("#jh-ah-top.show", timeout=6000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(400)

            show_bar()
            box = page.evaluate("() => { const b = document.getElementById('tfbar').getBoundingClientRect(); return [Math.round(b.left), Math.round(b.top), Math.round(b.width), Math.round(b.height)]; }")
            shots["0-the-bar"] = page.screenshot(type="jpeg", quality=78, clip={"x": 0, "y": max(0, box[1] - 4), "width": 1500, "height": min(260, box[3] + 12)})

            for bid, label, path, docs in TABS:
                try:
                    show_bar()
                    page.evaluate("(id) => document.getElementById(id).scrollIntoView({ block: 'nearest' })", bid)
                    page.wait_for_timeout(200)
                    b = page.evaluate("(id) => { const x = document.getElementById(id), q = x.getBoundingClientRect(); return { label: x.querySelector('.l').textContent, title: x.title, top: Math.round(q.top), h: Math.round(q.height) }; }", bid)
                    try:
                        page.click("#" + bid, timeout=6000)
                        how = "a real click"
                    except Exception as exc:  # noqa: BLE001
                        page.evaluate("(id) => document.getElementById(id).click()", bid)
                        how = "the button's own click (%s)" % str(exc)[:60]
                    page.wait_for_selector("#ws-overlay.on iframe", timeout=20000)
                    ov = page.evaluate("() => { const o = document.getElementById('ws-overlay'), f = o.querySelector('iframe'); return { src: f.getAttribute('src'), head: o.querySelector('.wshd span').textContent, open: o.querySelector('.wshd a').getAttribute('href') }; }")
                    want = path + ("?ticker=" + SYMBOL if bid == "btn-rsch" else "")
                    ck(b["label"] == label and b["title"].startswith(label + " · ") and ov["src"] == want and ov["head"].startswith(label + " · "), "%s: %s opens %s under \"%s\"" % (label, how, ov["src"], ov["head"]))
                    frame = None
                    for _ in range(80):
                        frame = next((f for f in page.frames if f != page.main_frame and path in f.url), None)
                        if frame:
                            break
                        page.wait_for_timeout(250)
                    frame.wait_for_selector("[data-jed-on]", timeout=60000)
                    own = frame.evaluate("() => document.body.innerText.length")
                    frame.evaluate("() => { document.querySelector('[data-jed]').scrollIntoView(); document.querySelectorAll('.jed-doc').forEach(d => { d.open = true; }); }")
                    got = []
                    for _ in range(240):
                        got = frame.evaluate(DOCS)
                        if got and all(not d["loading"] for d in got):
                            break
                        page.wait_for_timeout(500)
                    ck(len(got) == len(docs) and [d["doc"] for d in got] == docs and all(not d["bad"] and (d["values"] or 0) > 0 for d in got), "%s: every document of its engines is on the page and read (%s)" % (label, "; ".join("%s %s values in %s fields" % (d["name"], format(d["values"] or 0, ","), format(d["fields"] or 0, ",")) for d in got)))
                    # this op's own count of the first document against the page's
                    first = got[0]
                    try:
                        raw = json.loads(fetch((RESEARCH + SYMBOL + ".json") if docs[0] == "ticker" else "/%s?probe=%d" % (docs[0], stamp)))
                        mine = count(raw)
                        ck(mine == first["values"] or docs[0] in ("data/options-analytics.json", "data/opex-calendar.json", "ticker"), "%s: the page counts %s values in %s; this op counts %s in the same document%s" % (label, format(first["values"] or 0, ","), first["name"], format(mine, ","), "" if mine == first["values"] else " (the document is rewritten every few minutes)"))
                    except Exception as exc:  # noqa: BLE001
                        r.log("%s: own count not made (%s)" % (label, str(exc)[:100]))
                    # Every field: its total is the document's count
                    d0 = frame.locator(".jed-doc").first
                    d0.locator("button[data-v=fields]").click()
                    d0.locator(".jed-foot span[data-total]").wait_for(timeout=30000)
                    total = int(d0.locator(".jed-foot span[data-total]").get_attribute("data-total"))
                    ck(total == first["values"], "%s: Every field lists %s values, the document's own count" % (label, format(total, ",")))
                    d0.locator("button[data-v=sections]").click()
                    # a table: sorted both ways, opened whole
                    done = frame.evaluate("""async () => { const sleep = (ms) => new Promise(r => setTimeout(r, ms));
                      for (const d of document.querySelectorAll('.jed-doc')) for (const blk of d.querySelectorAll(':scope > .jed-body .jed-blk')) {
                        const m = /([\\d,]+) rows/.exec(blk.querySelector('summary i').textContent); if (!m || +m[1].replace(/,/g, '') < 3) continue;
                        blk.open = true; await sleep(250); const wrap = blk.querySelector('.jed-in > div'); if (!wrap || !wrap.querySelector('table')) continue;
                        const col = (wrap.querySelector('th.n') || wrap.querySelectorAll('th')[0]).getAttribute('data-col'), th = () => Array.from(wrap.querySelectorAll('th')).find(h => h.getAttribute('data-col') === col);
                        const cells = () => { const i = Array.from(wrap.querySelectorAll('th')).findIndex(h => h.getAttribute('data-col') === col); return Array.from(wrap.querySelectorAll('tbody tr')).map(r => r.children[i].getAttribute('title') || r.children[i].textContent); };
                        const num = (s) => { const v = parseFloat(String(s).replace(/[$,%\\s]/g, '').replace('\\u2212', '-')); const k = /T$/.test(s) ? 1e12 : /B$/.test(s) ? 1e9 : /M$/.test(s) ? 1e6 : 1; return isNaN(v) ? null : v * k; };
                        wrap.querySelector('button[data-a=all]').click(); await sleep(250);
                        const whole = wrap.querySelectorAll('tbody tr').length, said = +wrap.querySelector('.jed-foot span[data-rows]').getAttribute('data-rows');
                        th().click(); await sleep(200); const s1 = th().getAttribute('aria-sort'), down = cells().map(num).filter(v => v !== null);
                        th().click(); await sleep(200); const s2 = th().getAttribute('aria-sort'), up = cells().map(num).filter(v => v !== null);
                        const mono = (a, sign) => a.every((v, i) => i === 0 || (v - a[i - 1]) * sign >= 0);
                        return { block: blk.getAttribute('data-f'), col: col, rows: +m[1].replace(/,/g, ''), whole: whole, said: said, s1: s1, s2: s2, down: mono(down, -1), up: mono(up, 1), n: down.length };
                      } return null; }""")
                    if done:
                        ck(done["s1"] == "descending" and done["s2"] == "ascending" and done["down"] and done["up"] and done["whole"] == min(done["said"], 2000) and done["said"] == done["rows"], "%s: table \"%s\" opened whole (%d rows) and sorted on \"%s\" both ways (%d figures)" % (label, done["block"], done["whole"], done["col"], done["n"]))
                    else:
                        r.log("%s: no list of three rows or more at the first level to sort" % label)
                    r.log("%s: the page's own content %s characters; in the workspace the section has %d documents" % (label, format(own, ","), len(got)))
                    if bid in SHOTS:
                        shots["%d-%s" % (ids.index(bid) + 1, label.lower().replace(" ", "-"))] = page.screenshot(type="jpeg", quality=55)
                    page.click("#wsx")
                    page.wait_for_timeout(300)
                except Exception as exc:  # noqa: BLE001
                    ck(False, "%s raised %s: %s" % (label, type(exc).__name__, str(exc)[:240]))
                    try:
                        page.evaluate("() => { const o = document.getElementById('ws-overlay'); if (o) o.className = ''; }")
                    except Exception:  # noqa: BLE001
                        pass
            ck(not errors, "the chart threw no error while the twelve tabs were opened (%s)" % (errors[:3] or "none"))
            browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(tabs=len(TABS), failed_checks=len(fails))
        if fails:
            r.fail("%d check(s) failed: %s" % (len(fails), "; ".join(fails)[:700]))
            sys.exit(1)
        r.ok("twelve tabs clicked and checked in a real browser")


if __name__ == "__main__":
    main()
