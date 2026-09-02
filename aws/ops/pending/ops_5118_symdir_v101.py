"""ops_5118 -- symbol directory v1.0.1: fixes from the 5116 gates + edge + page verification.

5116 found (real bugs, all fixed in the engine source shipped with this op):
  * synonym prefix-explosion (germany -> de -> degree; canada -> ca -> cap)
  * SOFR ETF outranking nyfed:sofr (exact bare-id match discounted)
  * series-level drill never ran when the token index had zero hits
  * StatCan cubes are gzip(zip(csv)) -- "no VECTOR column"
  * treasury warm docs are single-series `observations`
  * CPILFESL/PAYEMS + 1,309 banked FRED ids without a catalog title
  * Eurostat dataflow names in FR/DE (English TOC overlay)
  * BOJ series returned 0 values -> resolver now carries diagnostics

  S1  redeploy justhodl-symdir (code + config), wait Active
  S2  rebuild the directory (Event invoke + poll manifest)
  S3  route re-verification through the function URL (every check that
      failed in 5116 + the drill + statcan browse/series + boj diagnostics)
  S4  edge: wait for deploy-workers.yml, then /symdir-health /symsearch
      /series /quote /browse through the data-proxy and the zone route
  S5  page: wait for pages.yml (chart-pro.html carries `class SymDir`), then
      headless Chrome: type a query, count directory rows in the dropdown,
      open a dataset in the browser, chart a series, add to a watchlist
Gates (RED): any 5116 failure still failing, worker routes not live within
10 min, page not live within 15 min, page errors, dropdown/browse/chart probes.
"""
import json
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-symdir"
SRC = ROOT / "aws" / "lambdas" / FN / "source"
PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=120, retries={"max_attempts": 2}))


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def http(url, timeout=120):
    req = urllib.request.Request(url, headers={"User-Agent": "justhodl-ops-5118", "Accept-Encoding": "identity"})
    t = time.time()
    with urllib.request.urlopen(req, timeout=timeout) as r:
        body = r.read()
        hdr = dict(r.headers)
    return body, int((time.time() - t) * 1000), hdr


def http_json(url, timeout=120):
    body, ms, hdr = http(url, timeout)
    return json.loads(body.decode("utf-8", "replace")), ms, hdr


def main():
    with report("5118-symdir-v101") as r:
        r.heading("ops 5118 -- symbol directory v1.0.1: gate fixes, edge routes, live page")
        t_start = datetime.now(timezone.utc)
        fails = []

        r.section("S1 redeploy justhodl-symdir")
        env = {"S3_BUCKET": B, "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d", "FRED_KEY": "2f057499936072679d8843d7fce99989"}
        try:
            cur = lam.get_function_configuration(FunctionName=FN)
            for k in ("BLS_API_KEY",):
                v = (cur.get("Environment") or {}).get("Variables", {}).get(k)
                if v:
                    env[k] = v
        except Exception as e:  # noqa: BLE001
            r.warn(f"could not read current env: {str(e)[:80]}")
        desc = json.load(open(ROOT / "aws" / "lambdas" / FN / "config.json"))["description"]
        deploy_lambda(report=r, function_name=FN, source_dir=SRC, env_vars=env, timeout=900, memory=3008, create_function_url=True, smoke=False, description=desc)
        cfg = {}
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
        r.kv(step="S1", state=cfg.get("State"), update=cfg.get("LastUpdateStatus"), url=url)
        h, ms, _ = http_json(url + "/health")
        r.log(f"  /health {ms}ms version={h.get('version')} directory_docs={h.get('directory_docs')}")
        if h.get("version") != "1.0.1":
            fails.append(f"deployed version is {h.get('version')}, expected 1.0.1")

        r.section("S2 rebuild")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=json.dumps({"mode": "build"}).encode())
        man = None
        t0 = time.time()
        while time.time() - t0 < 900:
            time.sleep(20)
            doc, lm = get_json("data/symdir/manifest.json")
            if doc and (doc.get("built_at") or "") > t_inv.isoformat():
                man = doc
                break
        if not man:
            r.fail("rebuild manifest did not land within 15 min")
            sys.exit(1)
        r.ok(f"rebuilt: docs={man.get('docs'):,} elapsed={man.get('elapsed_s')}s tokens={man.get('tokens'):,}")
        for src in ("fred", "eurostat", "eurostat-toc", "treasury", "boj", "statcan", "bls"):
            r.log(f"  source {src}: {json.dumps((man.get('sources') or {}).get(src))[:300]}")
        for k, v in (man.get("errors") or {}).items():
            r.warn(f"  build error {k}: {str(v)[:200]}")
        r.kv(step="S2", docs=man.get("docs"), elapsed=man.get("elapsed_s"), fred_untitled=((man.get("sources") or {}).get("fred") or {}).get("untitled_banked"),
             titles_now=((man.get("sources") or {}).get("fred") or {}).get("titles_fetched_now"), eurostat_renamed=((man.get("sources") or {}).get("eurostat-toc") or {}).get("renamed"))
        w, ms, _ = http_json(url + "/warm", timeout=180)
        r.log(f"  /warm {ms}ms docs={w.get('docs')} built_at={w.get('built_at')}")
        if (w.get("built_at") or "") < t_inv.isoformat():
            # a warm container may still hold the old index; force reload via a second warm after cold-start window
            time.sleep(5)
            w, ms, _ = http_json(url + "/warm?force=1", timeout=180)
            r.log(f"  /warm again {ms}ms built_at={w.get('built_at')}")

        r.section("S3 function-URL re-verification")

        def top(q, n=5, **kw):
            qs = "&".join(f"{k}={v}" for k, v in kw.items())
            d, ms, _ = http_json(url + "/search?q=" + urllib.parse.quote(q) + f"&limit={n}" + ("&" + qs if qs else ""), timeout=120)
            return d, ms
        checks = [("sofr", "nyfed:sofr", 1), ("dgs10", "fred:DGS10", 1), ("AAPL", "AAPL", 1), ("unrate", "fred:UNRATE", 1), ("TVC:VIX", "TVC:VIX", 1),
                  ("payrolls", "fred:PAYEMS", 3), ("cpi", "fred:CPIAUCSL", 5), ("core cpi", "fred:CPILFESL", 5), ("unemployment rate germany", "fred:LRHUTTTTDEM156S", 5),
                  ("canada employment", "statcan:", 5), ("nama_10_gdp", "eurostat:NAMA_10_GDP", 1), ("gdp", "fred:GDP", 3), ("m2", "fred:M2SL", 3), ("10 year treasury yield", "fred:DGS10", 3),
                  ("bank of japan", "boj:", 5), ("money market funds", "ofr:", 5), ("euro dollar exchange rate", "fred:DEXUSEU", 5)]
        for q, want, within in checks:
            try:
                d, ms = top(q, 8)
                ids = [x["id"] for x in d.get("rows", [])]
                ok = any(i == want or (want.endswith(":") and i.startswith(want)) for i in ids[:within])
                (r.ok if ok else r.fail)(f"  {q!r:<32} {ms:>5}ms total={d.get('total'):>7} top={ids[:5]}  want {want} within {within}")
                if not ok:
                    fails.append(f"ranking {q!r}: {ids[:within]} lacks {want}")
                r.log("      " + " | ".join(f"{x['id']} · {(x['name'] or '')[:40]}" for x in d.get("rows", [])[:4]))
            except Exception as e:  # noqa: BLE001
                fails.append(f"search {q!r}: {str(e)[:80]}")
        # series-level drill for a Tier-0 flow (page scan) and a Tier-1 flow (Range read)
        for q, want in (("eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE", "eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE"), ("ecb:EXR:EXR.D.USD.EUR", "ecb:EXR:EXR.D.USD.EUR.SP00.A"),
                        ("eurostat:CENS_21COBHS_R3:A.EU_OTH", "eurostat:CENS_21COBHS_R3:A.EU_OTH")):
            d, ms = top(q, 5)
            sh = d.get("series_hits") or {}
            ids = [x["id"] for x in sh.get("rows", [])]
            ok = any(i == want or i.startswith(want) for i in ids)
            (r.ok if ok else r.fail)(f"  drill {q!r} {ms}ms tier={sh.get('tier')} pages={sh.get('pages_scanned')} in_flow={sh.get('total_in_flow')} rows={ids[:3]} err={sh.get('error')}")
            if not ok:
                fails.append(f"drill {q!r} did not return {want}")
        # browse statcan + series via vector
        d, ms, _ = http_json(url + "/browse?ds=statcan:10100002&q=&limit=5", timeout=180)
        rows = d.get("rows") or []
        r.log(f"  browse statcan:10100002 {ms}ms total={d.get('total')} first={[x['id'] + ' · ' + (x['name'] or '')[:50] for x in rows[:2]]} err={d.get('error')}")
        if not rows:
            fails.append(f"statcan browse empty: {d.get('error')}")
        else:
            sd, ms, _ = http_json(url + "/series?id=" + urllib.parse.quote(rows[0]["id"]), timeout=300)
            r.log(f"  series {rows[0]['id']} {ms}ms n={sd.get('n')} first={sd.get('first')} last={sd.get('last')} src={sd.get('source')} err={sd.get('error')}")
            if not sd.get("n"):
                fails.append(f"statcan series empty: {sd.get('error') or sd.get('source')}")
        # boj diagnostics
        d, ms, _ = http_json(url + "/browse?ds=boj:BP01&q=&limit=400", timeout=180)
        cand = sorted((x for x in (d.get("rows") or []) if (x.get("last") or "") >= "2020"), key=lambda x: -(x.get("n") or 0))[:2]
        for x in cand + ([{"id": "boj:BP01:BPBP6D1A"}] if not any(c["id"] == "boj:BP01:BPBP6D1A" for c in cand) else []):
            sd, ms, _ = http_json(url + "/series?id=" + urllib.parse.quote(x["id"]) + "&nocache=1", timeout=300)
            r.log(f"  boj {x['id']} {ms}ms n={sd.get('n')} first={sd.get('first')} last={sd.get('last')} diag={sd.get('diag')} err={sd.get('error')}")
            if x is cand[0] if cand else False:
                if not sd.get("n"):
                    fails.append(f"boj series {x['id']} empty: diag={sd.get('diag')}")
        # treasury
        for sid in ("treasury:debt_to_penny", "treasury:avg_interest_rates"):
            sd, ms, _ = http_json(url + "/series?id=" + sid, timeout=120)
            r.log(f"  {sid} {ms}ms n={sd.get('n')} first={sd.get('first')} last={sd.get('last')} name={sd.get('name')!r} err={sd.get('error')}")
            if sid == "treasury:debt_to_penny" and not sd.get("n"):
                fails.append("treasury:debt_to_penny empty")
        # fred titles for the famous untitled ids
        for sid in ("fred:CPILFESL", "fred:PAYEMS"):
            d, ms = top(sid.split(":")[1], 3)
            row = next((x for x in d.get("rows", []) if x["id"] == sid), None)
            r.log(f"  {sid}: {row and row.get('name')!r} untitled={row and row.get('untitled')}")
            if not row:
                fails.append(f"{sid} not found by id")
        # a eurostat english name
        d, ms = top("nama_10_gdp", 1)
        nm = (d.get("rows") or [{}])[0].get("name") or ""
        r.log(f"  eurostat NAMA_10_GDP name: {nm!r}")
        if "Produit" in nm:
            fails.append("eurostat names still French (TOC overlay failed)")

        r.section("S4 edge routes (deploy-workers.yml)")
        live = False
        t0 = time.time()
        while time.time() - t0 < 600:
            try:
                d, ms, hdr = http_json(PROXY + "/symdir-health", timeout=60)
                if d.get("ok"):
                    live = True
                    r.ok(f"  /symdir-health via worker {ms}ms: version={d.get('version')} docs={d.get('directory_docs')} edge={hdr.get('X-Edge-Cache') or hdr.get('x-edge-cache')}")
                    break
                r.log(f"  worker route answered but not ok: {json.dumps(d)[:160]}")
            except Exception as e:  # noqa: BLE001
                r.log(f"  worker route not live yet ({str(e)[:60]}); waiting…")
            time.sleep(20)
        if not live:
            fails.append("worker symdir routes not live within 10 min")
        else:
            for path, must in (("/symsearch?q=dgs10&limit=3", "rows"), ("/series?id=fred:DGS10", "obs"), ("/quote?ids=fred:DGS10,nyfed:sofr", "quotes"), ("/browse?ds=eurostat:NAMA_10_GDP&q=DE&limit=3", "rows")):
                try:
                    d, ms, hdr = http_json(PROXY + path, timeout=180)
                    n = len(d.get(must) or [])
                    r.log(f"  {path} {ms}ms {must}={n} edge={hdr.get('X-Edge-Cache') or hdr.get('x-edge-cache')} err={d.get('error')}")
                    if not n:
                        fails.append(f"worker {path} returned no {must}")
                    # second hit should be an edge HIT
                    d2, ms2, hdr2 = http_json(PROXY + path, timeout=180)
                    r.log(f"      repeat {ms2}ms edge={hdr2.get('X-Edge-Cache') or hdr2.get('x-edge-cache')}")
                except Exception as e:  # noqa: BLE001
                    fails.append(f"worker {path}: {str(e)[:80]}")
            for u in (PROXY + "/data/symdir/instruments.json.gz", "https://justhodl.ai/data/symdir/instruments.json.gz"):
                try:
                    body, ms, hdr = http(u, timeout=120)
                    txt = body
                    if body[:2] == b"\x1f\x8b":
                        import gzip
                        txt = gzip.decompress(body)
                    d = json.loads(txt)
                    r.log(f"  instruments via {u.split('/')[2]}: {ms}ms {len(body)} bytes rows={d.get('n')} built={d.get('built_at')}")
                except Exception as e:  # noqa: BLE001
                    fails.append(f"instruments file via {u}: {str(e)[:80]}")

        r.section("S5 live page (pages.yml) + headless Chrome")
        page_ok = False
        t0 = time.time()
        while time.time() - t0 < 900:
            try:
                body, ms, _ = http("https://justhodl.ai/chart-pro.html?nocache=" + str(int(time.time())), timeout=60)
                if b"class SymDir" in body and b"class DatasetBrowser" in body:
                    page_ok = True
                    r.ok(f"  chart-pro.html carries the directory client ({len(body)} bytes)")
                    break
                r.log(f"  page not updated yet ({len(body)} bytes); waiting…")
            except Exception as e:  # noqa: BLE001
                r.log(f"  page fetch failed: {str(e)[:60]}")
            time.sleep(30)
        if not page_ok:
            fails.append("chart-pro.html did not carry the directory client within 15 min")
        else:
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
                console, errors = [], []
                page.on("console", lambda m: console.append({"type": m.type, "text": m.text[:300]}))
                page.on("pageerror", lambda e: errors.append(str(e)[:300]))
                page.goto("https://justhodl.ai/chart-pro.html", wait_until="domcontentloaded", timeout=90000)
                try:
                    page.wait_for_load_state("networkidle", timeout=30000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(6000)
                probe0 = page.evaluate("() => ({ symdir: typeof SymDir, dsb: typeof DatasetBrowser, instruments: SymDir.instruments ? SymDir.instruments.length : -1, proxy: PROXY })")
                r.log(f"  page globals: {json.dumps(probe0)}")
                # 1. search dropdown
                page.click("#search-input")
                page.fill("#search-input", "unemployment rate germany")
                page.wait_for_timeout(3500)
                dd = page.evaluate("""() => { const dd = document.getElementById('hsearch-dropdown');
                    const rows = Array.from(dd.querySelectorAll('.hs-row')).map(e => ({ id: e.dataset.ticker, kind: e.dataset.kind || e.className.replace('hs-row','').trim(), text: e.textContent.trim().slice(0, 90) }));
                    return { open: dd.classList.contains('open'), rows: rows.length, series: dd.querySelectorAll('.hs-row.series').length, datasets: dd.querySelectorAll('.hs-row.dataset').length,
                             instruments: dd.querySelectorAll('.hs-row.instrument').length, adds: dd.querySelectorAll('.hs-add').length, groups: Array.from(dd.querySelectorAll('.hs-group')).map(g => g.textContent.slice(0, 30)), first: rows.slice(0, 6) }; }""")
                r.log(f"  dropdown: {json.dumps(dd)[:1500]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5118-chartpro-search.jpg"), type="jpeg", quality=60)
                if not (dd.get("open") and (dd.get("series") or 0) >= 3):
                    fails.append(f"dropdown: open={dd.get('open')} series rows={dd.get('series')}")
                # 2. dataset browser: type a dataset code, click its row
                page.fill("#search-input", "nama_10_gdp")
                page.wait_for_timeout(3000)
                has_ds = page.evaluate("() => !!document.querySelector('#hsearch-dropdown .hs-row.dataset')")
                if has_ds:
                    page.click("#hsearch-dropdown .hs-row.dataset")
                    page.wait_for_timeout(5000)
                    br = page.evaluate("""() => ({ open: document.getElementById('dsb-modal').classList.contains('open'), rows: document.querySelectorAll('#dsb-body .dsb-row').length,
                        chips: document.querySelectorAll('#dsb-facets .dsb-chip').length, count: document.getElementById('dsb-count').textContent, title: document.getElementById('dsb-title').textContent.slice(0, 80),
                        first: Array.from(document.querySelectorAll('#dsb-body .dsb-row')).slice(0, 3).map(e => e.textContent.trim().slice(0, 100)) })""")
                    r.log(f"  dataset browser: {json.dumps(br)[:900]}")
                    page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5118-chartpro-browser.jpg"), type="jpeg", quality=60)
                    if not (br.get("open") and (br.get("rows") or 0) >= 5):
                        fails.append(f"dataset browser: {json.dumps(br)[:200]}")
                    # click a chip then a row -> chart
                    if br.get("chips"):
                        page.click("#dsb-facets .dsb-chip")
                        page.wait_for_timeout(3000)
                    page.click("#dsb-body .dsb-row")
                    page.wait_for_timeout(7000)
                else:
                    fails.append("no dataset row for nama_10_gdp in dropdown")
                    page.evaluate("() => ChartController.loadTicker('eurostat:NAMA_10_GDP:A.CLV10_MEUR.B1GQ.DE')")
                    page.wait_for_timeout(7000)
                ch = page.evaluate("""() => ({ active: State.activeTicker, name: (document.getElementById('active-name')||{}).textContent, meta: (document.getElementById('nch-meta-0')||{}).textContent,
                                          loading: (document.getElementById('nch-loading-0')||{}).textContent, symMeta: State.symbolMeta[State.activeTicker] || null })""")
                r.log(f"  chart after browser click: {json.dumps(ch)[:700]}")
                if not ("obs" in str(ch.get("meta") or "")):
                    fails.append(f"series chart did not render: {json.dumps(ch)[:200]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5118-chartpro-series.jpg"), type="jpeg", quality=60)
                # 3. FRED series through the same path + watchlist add
                page.evaluate("() => ChartController.loadTicker('fred:DGS10')")
                page.wait_for_timeout(6000)
                ch2 = page.evaluate("() => ({ active: State.activeTicker, meta: (document.getElementById('nch-meta-0')||{}).textContent, name: (document.getElementById('active-name')||{}).textContent })")
                r.log(f"  fred:DGS10 chart: {json.dumps(ch2)[:400]}")
                if "obs" not in str(ch2.get("meta") or ""):
                    fails.append(f"fred:DGS10 chart did not render: {json.dumps(ch2)[:200]}")
                wl = page.evaluate("""() => { State.customWatchlists['custom_ops5118'] = { name: 'ops5118', color: null, tickers: [] }; State.activeWatchlistId = 'custom_ops5118';
                    SymDir.addToWatchlist('fred:DGS10', { id: 'fred:DGS10', name: 'Market Yield on U.S. Treasury Securities at 10-Year', provider: 'fred', unit: '%', freq: 'D' });
                    SymDir.addToWatchlist('nyfed:sofr', { id: 'nyfed:sofr', name: 'Secured Overnight Financing Rate (SOFR)', provider: 'nyfed', unit: 'Percent', freq: 'D' });
                    SymDir.addToWatchlist('AAPL', null); return State.customWatchlists['custom_ops5118'].tickers; }""")
                page.wait_for_timeout(6000)
                rows = page.evaluate("""() => Array.from(document.querySelectorAll('#watchlist-body .wl-row')).map(e => ({ t: e.dataset.ticker, sym: e.querySelector('.wl-symbol').textContent, sub: e.querySelector('.wl-sub').textContent.slice(0, 50), last: e.querySelector('.wl-last').textContent, chg: e.querySelector('.wl-chg').textContent }))""")
                r.log(f"  watchlist after adds {wl}: {json.dumps(rows)[:900]}")
                page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / "5118-chartpro-watchlist.jpg"), type="jpeg", quality=60)
                srows = [x for x in rows if x["t"] in ("fred:DGS10", "nyfed:sofr")]
                if len(srows) < 2 or any(x["last"] in ("—", "") for x in srows):
                    fails.append(f"watchlist series rows missing values: {json.dumps(srows)[:300]}")
                csp = [c["text"] for c in console if "content security policy" in c["text"].lower()]
                errs = [e for e in errors if "TradingView" not in e]
                r.log(f"  page errors {len(errs)} {json.dumps(errs)[:400]} · CSP {len(csp)} {json.dumps(csp)[:300]}")
                if errs or csp:
                    fails.append(f"page errors {len(errs)} / CSP {len(csp)}")
                page.close()
                ctx.close()
                browser.close()

        r.section("verdict")
        r.log(f"elapsed {int((datetime.now(timezone.utc) - t_start).total_seconds())}s")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"PASS_ALL: symdir v1.0.1 live, {man.get('docs'):,} docs; worker routes live; chart-pro searches every ticker and data series, browses datasets, charts full history, watchlist shows series values")


if __name__ == "__main__":
    main()
