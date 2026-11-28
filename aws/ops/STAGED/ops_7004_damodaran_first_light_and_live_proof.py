"""ops 7004 - justhodl-damodaran: birth, first light, the EVA engine on its premium, and the pages on the live site.

Khalid, 2026-11-27: "add whatever my system needs". Dry run: ops 7003 (251 of his files read on the runner, his page's
five premiums equal to the monthly file's newest row, EVA's whole-market pass on 5.50% beside 4.41%).

  1. Guard   the bank's function is this checkout's, configured as config.json says, on its once-a-day schedule;
             a failed run is not retried by itself.
  2. Light   ONE run of the bank, by hand, only while its manifest holds no run (run again later: nothing is invoked).
  3. Read    the three documents, the tables and the kept files, read back from the bucket and checked; his files
             are not served by the site.
  4. EVA     ONE run of justhodl-eva-spread, by hand, only while its live document does not carry the bank's premium;
             then its document is read back: the premium is the bank's, and says so.
  5. Pages   cost-of-capital.html and eva.html on the live site, in a real browser: figures against the documents,
             every list sorted both ways, searched and saved, the chart read under the pointer, a phone.

Safe to run again: with the bank lit and EVA carrying its premium it invokes nothing.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import importlib.util
import io
import json
import re
import sys
import time
import urllib.error
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
NAME = "ops_7004_damodaran_first_light_and_live_proof"
REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
FN, EVA_FN, SCHED = "justhodl-damodaran", "justhodl-eva-spread", "justhodl-damodaran-daily"
BASE, UA = "https://justhodl.ai", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops"
MANIFEST, PREMIUM, COUNTRY, INDUSTRY = "data/warm/damodaran/manifest.json", "data/equity-risk-premium.json", "data/country-risk-premiums.json", "data/industry-benchmarks.json"
RAW, TABLES, EVA_KEY = "damodaran/raw/", "data/warm/damodaran/tables/", "data/eva-spread.json"
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
_MASK = [(re.compile(r"(api_?key=)[^&\s\"']*", re.I), r"\1***"), (re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+"), "***")]


def clean(text):
    text = str(text)
    for pat, rep in _MASK:
        text = pat.sub(rep, text)
    return text


def load(stem):
    path = next((ROOT / "aws" / "ops" / "STAGED").glob(stem + "_*.py"))
    spec = importlib.util.spec_from_file_location(stem, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def fetch(path, timeout=90):
    with urllib.request.urlopen(urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Cache-Control": "no-cache"}), timeout=timeout) as resp:
        return resp.read()


def outside(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 jh-ops", "Range": "bytes=0-63"}), timeout=30) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code
    except Exception as exc:                  # noqa: BLE001
        return "no answer (%s)" % type(exc).__name__


def keys_under(s3, prefix):
    out, token = {}, None
    while True:
        kw = {"Bucket": LIVE, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        for o in page.get("Contents") or []:
            out[o["Key"]] = o["Size"]
        token = page.get("NextContinuationToken")
        if not token:
            return out


def month(day):
    return "%s %s" % (MONTHS[int(day[5:7]) - 1], day[:4])


def pct(v):
    """As the page writes it: JavaScript's toFixed rounds the number's exact value half up; Python's % rounds a tie to even."""
    if v is None:
        return "–"
    from decimal import Decimal, ROUND_HALF_UP
    return "%s%%" % Decimal(v * 100).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def same_package(lam, fn, source, files):
    g = lam.get_function(FunctionName=fn)
    with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
        z = zipfile.ZipFile(io.BytesIO(resp.read()))
    names = set(z.namelist())
    return g["Configuration"], {n: (n in names and z.read(n) == (source / n).read_bytes()) for n in files}


def browse(new_page, P, K, I, E, ck, shots, log, tables):
    us = P["us"]
    page, errors = new_page()
    page.goto(BASE + "/cost-of-capital.html?probe=%d" % int(time.time()), wait_until="domcontentloaded", timeout=120000)
    page.wait_for_selector("#cc-table table", timeout=120000)
    page.wait_for_selector("#cc-premium .cc-tile", timeout=60000)
    page.wait_for_timeout(700)
    tiles = page.evaluate("() => Array.from(document.querySelectorAll('#cc-premium .cc-tile')).map(t => t.innerText.replace(/\\s*\\n\\s*/g, ' | '))")
    for t in tiles:
        log("  tile: %s" % t)
    ck(len(tiles) == 6 and ("| %.2f%% |" % us["premium_pct"]) in tiles[0] and ("start of %s" % month(us["as_of"])) in tiles[0]
       and ("| %.2f%% |" % us["a_month_before"]["pct"]) in tiles[1] and ("| %.2f%% |" % us["a_year_before"]["pct"]) in tiles[2]
       and ("| %.2f%% |" % us["tbond_pct"]) in tiles[3] and ("| %.2f%% |" % us["expected_return_pct"]) in tiles[4] and format(round(us["sp500"]), ",") in tiles[5],
       "the six tiles are the premium document's figures: %.2f%% at the start of %s, %.2f%% a month before, %.2f%% a year before" % (us["premium_pct"], month(us["as_of"]), us["a_month_before"]["pct"], us["a_year_before"]["pct"]))
    meas = page.evaluate("() => Array.from(document.querySelectorAll('#cc-measures tbody tr')).map(tr => Array.from(tr.children).map(td => td.innerText.trim()))")
    ck(len(meas) == 5 and [m[2] for m in meas] == ["%.2f%%" % m["pct"] for m in us["measures"]] and [m[3] for m in meas] == ["%.2f%%" % v for v in P["home_page"]["page_values_pct"]]
       and "the one the engines use" in meas[0][0], "the five measures, each with his page's figure beside it: %s" % [m[2] for m in meas])
    counts = dict(page.evaluate("() => Array.from(document.querySelectorAll('.cc-count')).map(c => [c.dataset.ccCount, c.innerText.trim()])"))
    log("  lists: %s" % counts)
    bench_us = I["benchmarks"]["US"]["rows"]
    ck(counts.get("monthly") == "%d months" % P["monthly"]["n"] and counts.get("annual") == "%d years" % P["annual"]["n"] and counts.get("returns") == "%d years" % P["returns"]["n"]
       and counts.get("countries") == "%d countries" % K["n"] and counts.get("frontier") == "%d markets" % len(K["frontier"]["rows"]) and counts.get("industries") == "%d industries" % len(bench_us),
       "every list is whole: %d months, %d and %d years, %d countries, %d frontier markets, %d industries" % (P["monthly"]["n"], P["annual"]["n"], P["returns"]["n"], K["n"], len(K["frontier"]["rows"]), len(bench_us)))
    rows_of = "(id) => Array.from(document.querySelectorAll('#' + id + ' tbody tr')).map(tr => Array.from(tr.children).map(td => td.innerText.trim()))"
    ck(len(page.evaluate(rows_of, "cc-countries")) == K["n"] and len(page.evaluate(rows_of, "cc-monthly")) == P["monthly"]["n"], "and every row of them is on the page (%d countries, %d months)" % (K["n"], P["monthly"]["n"]))

    # --- the chart
    page.locator("#s-history").scroll_into_view_if_needed()
    page.wait_for_timeout(300)
    n_lines = page.evaluate("() => document.querySelectorAll('#cc-chart path').length")
    legend = page.evaluate("() => Array.from(document.querySelectorAll('#cc-legend .cc-key')).map(k => k.innerText.trim())")
    ck(n_lines == 5 and legend == [m["label"] for m in us["measures"]], "the chart draws the five measures, each named in the key: %s" % legend)
    # the picture itself: a few ticks over the premiums' own range, and lines that fill it (one cell of his file off by a hundredfold once flattened every line)
    look = page.evaluate("""() => { const svg = document.querySelector('#cc-chart svg');
      return { ticks: Array.from(svg.querySelectorAll('text[text-anchor="end"]')).map(t => parseFloat(t.textContent)), heights: Array.from(svg.querySelectorAll('path')).map(p => Math.round(p.getBBox().height)), plot: svg.viewBox.baseVal.height - 38 }; }""")
    log("  the picture: ticks %s, each line's height %s of a plot %s px tall" % (look["ticks"], look["heights"], look["plot"]))
    ck(3 <= len(look["ticks"]) <= 9 and max(look["ticks"]) <= 25 and max(look["heights"]) >= 0.4 * look["plot"] and min(look["heights"]) >= 20,
       "the axis runs from %s%% to %s%% in %d ticks and the lines fill the plot" % (min(look["ticks"]), max(look["ticks"]), len(look["ticks"])))
    keys0 = [c["key"] for c in P["monthly"]["cols"]]
    off = [(m["label"], month(x[0]), x[keys0.index(P["monthly"]["measure_columns"][m["key"]])]) for m in us["measures"] for x in P["monthly"]["rows"]
           if x[keys0.index(P["monthly"]["measure_columns"][m["key"]])] is not None and not -0.05 <= x[keys0.index(P["monthly"]["measure_columns"][m["key"]])] <= 0.25]
    note = page.locator("#cc-off").inner_text().strip()
    log("  off the scale in his file: %s | the page says: %s" % (off, note[:300]))
    ck((not off and note == "") or (bool(off) and all(("%s, %s (%s in his file)" % (lab, when, pct(v))) in note for lab, when, v in off)),
       "%d figure(s) of his file too far off the scale to be a premium are left out of the picture and named under it" % len(off))
    ends = page.evaluate("() => Array.from(document.querySelectorAll('#cc-chart svg text')).map(t => t.textContent).filter(t => /^\\d+\\.\\d\\d%$/.test(t))")
    ck(sorted(ends) == sorted("%.2f%%" % m["pct"] for m in us["measures"]), "each line ends in its newest value: %s" % ends)
    box = page.locator("#cc-chart").bounding_box()
    page.mouse.move(box["x"] + box["width"] * 0.55, box["y"] + 160)
    page.wait_for_timeout(250)
    tip = page.locator("#cc-chart .cc-tip").inner_text().replace("\n", " | ")
    log("  under the pointer: %s" % tip[:260])
    mm = re.match(r"([A-Z][a-z]{2}) (\d{4})", tip)
    row = next((x for x in P["monthly"]["rows"] if mm and x[0][:7] == "%s-%02d" % (mm.group(2), MONTHS.index(mm.group(1)) + 1)), None)
    keys = [c["key"] for c in P["monthly"]["cols"]]
    want = [("%.2f%%" % (row[keys.index(P["monthly"]["measure_columns"][m["key"]])] * 100)) for m in us["measures"] if row and row[keys.index(P["monthly"]["measure_columns"][m["key"]])] is not None] if row else []
    ck(bool(row) and all(w in tip for w in want) and len(want) >= 1, "the pointer reads a month of his file: %s -> %s" % (row[0] if row else None, want))
    shots["2-the-premium-through-time"] = page.screenshot(type="jpeg", quality=70)
    page.click('#cc-legend .cc-key[data-cc-line="average_cf"]')
    ck(page.evaluate("() => document.querySelectorAll('#cc-chart path').length") == 4, "a line switched off in the key is not drawn")
    page.click('#cc-views [data-cc-view="annual"]')
    page.wait_for_timeout(200)
    an_lines = page.evaluate("() => document.querySelectorAll('#cc-chart path').length")
    ck(an_lines >= 2 and "since %s" % P["annual"]["from"] in page.locator('#cc-views [data-cc-view="annual"]').inner_text(), "the year-end view draws %d lines since %s" % (an_lines, P["annual"]["from"]))
    page.click('#cc-views [data-cc-view="monthly"]')

    # --- countries: sorted both ways, searched, saved
    page.locator("#s-countries").scroll_into_view_if_needed()
    j = [c["key"] for c in K["cols"]].index("equity_risk_premium")
    page.click('#cc-countries button[data-cc-sort="countries"][data-j="%d"]' % j)
    top = page.evaluate(rows_of, "cc-countries")[0]
    hi = max((x for x in K["rows"] if x[j] is not None), key=lambda x: x[j])
    page.click('#cc-countries button[data-cc-sort="countries"][data-j="%d"]' % j)
    low = page.evaluate(rows_of, "cc-countries")[0]
    lo = min(x[j] for x in K["rows"] if x[j] is not None)
    ck(top[j] == pct(hi[j]) and top[0] == hi[0] and low[j] == pct(lo), "countries sort by total premium both ways: %s at %s, then %s" % (top[0], top[j], low[j]))
    ck(page.evaluate("() => document.activeElement && document.activeElement.dataset.ccSort") == "countries", "the heading that was clicked keeps the focus")
    page.fill('#cc-countries input[data-cc-q="countries"]', "united")
    page.wait_for_timeout(150)
    hits = [x for x in K["rows"] if any(isinstance(v, str) and "united" in v.lower() for v in x)]
    got = page.evaluate(rows_of, "cc-countries")
    ck(len(got) == len(hits) and page.locator('[data-cc-count="countries"]').inner_text() == "%d of %d countries" % (len(hits), K["n"])
       and page.evaluate("() => document.activeElement && document.activeElement.dataset.ccQ") == "countries", "searching 'united' leaves %d of %d, and the search box keeps the focus" % (len(hits), K["n"]))
    with page.expect_download(timeout=30000) as dl:
        page.click('#cc-countries button[data-cc-csv="countries"]')
    saved = Path(dl.value.path()).read_bytes().decode("utf-8").strip().split("\r\n")
    ck(len(saved) == len(hits) + 1 and saved[0].startswith("Country,Region,Moody's rating") and dl.value.suggested_filename == "damodaran-countries.csv", "the list saves as a file of what is shown: %d rows (%s)" % (len(saved) - 1, dl.value.suggested_filename))
    page.fill('#cc-countries input[data-cc-q="countries"]', "")
    usa = next(x for x in K["rows"] if x[0] == "United States")
    facts = page.locator("#cc-country-facts").inner_text().replace("\n", " | ")
    ck(pct(K["mature_market_premium"]) in facts and pct(K["us_premium"]) in facts and pct(usa[j]) == pct(K["us_premium"]), "a mature market's premium %s and the United States' %s, as his file gives them" % (pct(K["mature_market_premium"]), pct(K["us_premium"])))

    # --- industries: a region, a sort, the picture
    page.locator("#s-industries").scroll_into_view_if_needed()
    cols = [c["key"] for c in I["benchmarks"]["US"]["cols"]]
    jc = cols.index("cost_of_capital")
    page.click('#cc-industries button[data-cc-sort="industries"][data-j="%d"]' % jc)
    first = page.evaluate(rows_of, "cc-industries")[0]
    best = max((x for x in bench_us if x[jc] is not None), key=lambda x: x[jc])
    ck(first[0] == best[0] and first[jc] == pct(best[jc]), "US industries by cost of capital: %s at %s first" % (first[0], first[jc]))
    tm = next((x for x in bench_us if x[0].lower() == "total market"), None)
    log("  the US total market: %s" % dict(zip(cols, tm)) if tm else "  no total market row")
    shots["3-industries"] = page.screenshot(type="jpeg", quality=70)
    page.click('#cc-regions [data-cc-region="Global"]')
    page.wait_for_timeout(200)
    gl = I["benchmarks"]["Global"]["rows"]
    grows = page.evaluate(rows_of, "cc-industries")
    gbest = max((x for x in gl if x[jc] is not None), key=lambda x: x[jc])
    ck(len(grows) == len(gl) and grows[0][0] == gbest[0] and grows[0][jc] == pct(gbest[jc]), "the Global switch lists %d industries, still sorted: %s at %s" % (len(gl), grows[0][0], grows[0][jc]))
    ck(len(page.evaluate("() => Array.from(document.querySelectorAll('#cc-regions .cc-chip')).map(c => c.innerText)")) == len(I["regions"]), "a switch for each of his %d regions" % len(I["regions"]))

    # --- every table
    page.locator("#s-tables").scroll_into_view_if_needed()
    opts = page.evaluate("() => Array.from(document.querySelectorAll('select[data-cc-topic] option')).map(o => o.value)")
    ck(sorted(opts) == sorted(set(t["topic"] for t in I["tables"])), "the picker offers all %d of his tables' topics" % len(opts))
    head0 = page.locator("#cc-table-head").inner_text()
    w = tables["wacc-us"]
    ck(len(page.evaluate(rows_of, "cc-table")) == w["n"] and (w["title"] or "") in head0 and "wacc.xls" in head0, "it opens on the US cost of capital table, whole: %d rows, from wacc.xls" % w["n"])
    page.select_option("select[data-cc-topic]", "pe")
    page.select_option("select[data-cc-tregion]", "Japan")
    page.wait_for_function("() => /Japan/.test(document.getElementById('cc-table-head').innerText) && document.querySelector('#cc-table table')", timeout=60000)
    pj = tables["pe-japan"]
    trows = page.evaluate(rows_of, "cc-table")
    heads = page.evaluate("() => Array.from(document.querySelectorAll('#cc-table thead th')).map(th => th.innerText.trim())")
    ck(len(trows) == pj["n"] and heads == [c["label"] for c in pj["cols"]] and trows[0][0] == pj["rows"][0][0], "another table of another region opens whole under his own headings: PE, Japan, %d rows, %d columns" % (pj["n"], len(heads)))
    shots["4-every-table"] = page.screenshot(type="jpeg", quality=70)

    # --- returns, source, nothing broken
    worked = page.locator("#cc-worked").inner_text().replace("\n", " ")
    p0 = P["returns"]["premiums_worked_here"][0]
    ck(("%d to %d (%d)" % (p0["from"], p0["to"], p0["years"])) in worked and pct(p0["stocks_minus_bonds"]["compounded"]) in worked, "stocks over bonds since %d, compounded: %s" % (p0["from"], pct(p0["stocks_minus_bonds"]["compounded"])))
    src = page.locator("#cc-source").inner_text()
    ck("Aswath Damodaran" in src and "no strings attached" in src and "ERPbymonth.xlsx" in src and "not a real-time source" in src, "the source block names him, his terms, his caution and his files")
    bad = page.evaluate("() => (document.getElementById('cc-host').innerText.match(/\\bNaN\\b|\\bundefined\\b|\\[object/g) || []).length")
    ck(bad == 0 and not errors, "nothing unreadable on the page and no script error: %s" % errors[:3])
    page.evaluate("() => window.scrollTo(0, 0)")
    page.wait_for_timeout(200)
    shots["1-cost-of-capital"] = page.screenshot(type="jpeg", quality=70)

    # --- eva.html
    ev, eerr = new_page()
    ev.goto(BASE + "/eva.html?probe=%d" % int(time.time()), wait_until="domcontentloaded", timeout=120000)
    ev.wait_for_selector("#erp-source", timeout=120000)
    card = ev.evaluate("() => Array.from(document.querySelectorAll('#regime .wacc .row')).map(r => r.innerText.replace(/\\s*\\n\\s*/g, ' | '))")
    for line in card:
        log("  eva.html: %s" % line)
    w_in = E["wacc_inputs"]
    ck(any(("Equity Risk Premium" in c and ("%.2f%%" % w_in["equity_risk_premium_pct"]) in c) for c in card) and w_in["equity_risk_premium_pct"] == us["premium_pct"],
       "eva.html shows the premium the engine used, and it is the bank's: %.2f%%" % us["premium_pct"])
    ck(any(("Premium read from" in c and ("Damodaran · start of %s" % month(us["as_of"])) in c) for c in card) and ev.locator("#erp-source a").get_attribute("href") == "/cost-of-capital.html",
       "and says where it was read from, with a link to the desk: Damodaran · start of %s" % month(us["as_of"]))
    ck(not eerr, "no script error on eva.html: %s" % eerr[:3])
    shots["5-eva-premium"] = ev.locator("#regime").screenshot(type="jpeg", quality=75)

    # --- a phone
    ph, perr = new_page(True)
    ph.goto(BASE + "/cost-of-capital.html?probe=%d" % int(time.time()), wait_until="domcontentloaded", timeout=120000)
    ph.wait_for_selector("#cc-table table", timeout=120000)
    ph.wait_for_timeout(700)
    o = ph.evaluate("() => ({wide: document.documentElement.scrollWidth - document.documentElement.clientWidth, paths: document.querySelectorAll('#cc-chart path').length, tiles: document.querySelectorAll('#cc-premium .cc-tile').length})")
    ck(o["wide"] <= 1 and o["paths"] == 5 and o["tiles"] == 6 and not perr, "on a phone the page fits the screen with the chart drawn (%s px wider than the screen)" % o["wide"])
    shots["6-phone"] = ph.screenshot(type="jpeg", quality=62)


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=640, connect_timeout=30, retries={"max_attempts": 0}))
    s3 = boto3.client("s3", region_name=REGION)
    sch = boto3.client("scheduler", region_name=REGION)
    A = load("ops_6846")
    fails, shots = [], {}
    with report(NAME) as r:
        def say(text):
            r.log(clean(text))

        def ck(ok, text):
            (r.ok if ok else r.fail)(clean(text))
            if not ok:
                fails.append(text)

        def doc_of(key):
            return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())

        r.heading("7004 - justhodl-damodaran: first light, EVA on its premium, and the pages live, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. Guard: the function, its package, its schedule")
        cfg = json.loads((ROOT / "aws/lambdas" / FN / "config.json").read_text())
        try:
            cf, same = same_package(lam, FN, ROOT / "aws/lambdas" / FN / "source", ["lambda_function.py", "dam_core.py", "xlrd/__init__.py"])
        except Exception as e:                # noqa: BLE001
            r.fail("%s is not deployed yet (%s): wait for deploy-lambdas, then run again" % (FN, type(e).__name__))
            sys.exit(1)
        env = (cf.get("Environment") or {}).get("Variables", {})
        say("%s: runtime %s, memory %s MB, timeout %s s, state %s, updated %s | files as in this checkout: %s" % (FN, cf.get("Runtime"), cf.get("MemorySize"), cf.get("Timeout"), cf.get("State"), (cf.get("LastModified") or "")[:19], same))
        diff = {k: (env.get(k), v) for k, v in cfg["env"].items() if env.get(k) != v}
        if not all(same.values()) or diff or cf.get("MemorySize") != cfg["memory"] or cf.get("Timeout") != cfg["timeout"] or cf.get("Runtime") != cfg["runtime"] or cf.get("State") != "Active":
            r.fail("the deployed function is not this checkout's or not configured as config.json says (%s); nothing was invoked" % json.dumps(diff))
            sys.exit(1)
        ck(True, "the deployed bank is this checkout's, configured as config.json says")
        sc = cfg["eventbridge_scheduler"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            ck(live.get("ScheduleExpression") == sc["cron"] and live.get("State") == "ENABLED" and (live.get("Target") or {}).get("Arn", "").endswith(":function:" + FN),
               "its schedule is %s, enabled, and starts this function and nothing else" % live.get("ScheduleExpression"))
        except Exception as e:                # noqa: BLE001
            ck(False, "the schedule %s could not be read: %s" % (SCHED, type(e).__name__))
        lam.put_function_event_invoke_config(FunctionName=FN, MaximumRetryAttempts=0, MaximumEventAgeInSeconds=900)
        ck(lam.get_function_event_invoke_config(FunctionName=FN).get("MaximumRetryAttempts") == 0, "a failed run is not retried by itself: it waits for the next day")

        r.section("2. First light")
        try:
            man = doc_of(MANIFEST)
        except Exception:                     # noqa: BLE001
            man = {}
        if man.get("runs"):
            say("the bank has %d run(s) on record, the last at %s: it is not run again" % (len(man["runs"]), man["runs"][-1].get("at")))
        else:
            say("no run on record: ONE run by hand, now (disclosed to Khalid)")
            t0 = time.time()
            resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_7004_first_light"}).encode())
            out = json.loads(resp["Payload"].read() or b"{}")
            say("answered in %.0f s: %s" % (time.time() - t0, json.dumps({k: out.get(k) for k in ("ok", "seconds", "listed", "asked", "sweep", "swept", "stopped", "unchanged", "banked_bytes", "in_bank", "groups", "wrote", "peak_memory_mb", "premium", "trouble", "errorMessage")})[:1500]))
            ck(not resp.get("FunctionError") and out.get("ok") is True, "the run finished without an error")
            man = doc_of(MANIFEST)
        last = (man.get("runs") or [{}])[-1]
        say("last run: %s" % json.dumps({k: last.get(k) for k in ("at", "seconds", "asked", "sweep", "swept", "stopped", "unchanged", "banked_bytes", "wrote", "groups", "peak_memory_mb")}))
        for t in last.get("trouble") or []:
            say("  trouble: %s" % t)

        r.section("3. Read back")
        P, K, I = doc_of(PREMIUM), doc_of(COUNTRY), doc_of(INDUSTRY)
        us, hp = P["us"], P["home_page"]
        say("premium: %s%% at the start of %s (%s), T.Bond %s%%, expected return %s%% | a month before %s, a year before %s | his page %s -> %s, agree %s" % (
            us["premium_pct"], us["as_of"], us["measure"], us["tbond_pct"], us["expected_return_pct"], us["a_month_before"], us["a_year_before"], hp.get("page_date"), hp.get("page_values_pct"), hp.get("agree")))
        ck(P["version"] == "1.0.0" and len(us["measures"]) == 5 and hp.get("agree") is True and [m["pct"] for m in us["measures"]] == hp.get("page_values_pct") and 2.0 < us["premium_pct"] < 8.0,
           "the premium document carries his page's five figures for %s: %s" % (hp.get("page_date"), [m["pct"] for m in us["measures"]]))
        ck(P["monthly"]["from"][:7] == "2008-09" and P["monthly"]["n"] >= 200 and P["annual"]["from"] == 1960 and P["returns"]["from"] == 1928 and len(P["returns"]["premiums_worked_here"]) == 3 and P["attribution"].startswith("Data: Aswath Damodaran"),
           "monthly since %s (%d rows; his file skips %s), annual since %s, returns since %s, his name on it" % (P["monthly"]["from"], P["monthly"]["n"], P["monthly"]["months_his_file_skips"], P["annual"]["from"], P["returns"]["from"]))
        usa = next((x for x in K["rows"] if x[0] == "United States"), None)
        ck(K["n"] >= 140 and len((K.get("frontier") or {}).get("rows") or []) >= 5 and usa is not None and all(x[4] is not None and 0.01 < x[4] < 0.6 for x in K["rows"]),
           "countries: %d rated and %d frontier markets from %s (as of %s); mature market %s, United States %s" % (K["n"], len(K["frontier"]["rows"]), K["file"], K["as_of"], pct(K["mature_market_premium"]), pct(K["us_premium"])))
        say("industries: as of %s, %d tables | %s" % (I["as_of"], I["n_tables"], "; ".join("%s %d tables, %d industries, missing %s" % (g["code"], g["tables"], g["industries"], g["missing"]) for g in I["regions"])))
        for x in I.get("not_tables") or []:
            say("  holds no industry table: %s (%s)" % (x["file"], x["why"]))
        ck(len(I["regions"]) == 8 and I["n_tables"] >= 200 and all(not g["missing"] and g["industries"] >= 90 for g in I["regions"]), "eight regions, %d tables, and every benchmark column found in every region" % I["n_tables"])
        stored = keys_under(s3, TABLES)
        ck(len(stored) == I["n_tables"] and all(t["key"] in stored for t in I["tables"]), "every one of the %d tables listed is stored under %s" % (I["n_tables"], TABLES))
        tables = {tid: doc_of(TABLES + tid + ".json") for tid in ("wacc-us", "pe-japan")}
        raw = {k: v for k, v in keys_under(s3, RAW).items() if "/_superseded/" not in k}
        files = man.get("files") or {}
        ck(len(raw) == len(files) and len(files) >= 200 and all(raw.get(RAW + n) == f["bytes"] for n, f in files.items()), "his %d files are kept whole under %s, each at its recorded size (%.1f MB)" % (len(files), RAW, sum(raw.values()) / 1e6))
        ck(not [k for k in keys_under(s3, "data/warm/damodaran/") if k.lower().endswith((".xls", ".xlsx"))], "none of his spreadsheets is stored where the site serves it")
        probe = RAW + "ERPbymonth.xlsx"
        site_says, bucket_says = outside("%s/%s" % (BASE, probe)), outside("https://%s.s3.amazonaws.com/%s" % (LIVE, probe))
        ck(site_says in (403, 404) and bucket_says in (403, 404), "a stranger asking for %s is refused by the site (%s) and by the bucket (%s)" % (probe, site_says, bucket_says))
        served = {}
        for attempt in range(12):
            try:
                served = {k: json.loads(fetch("/%s?probe=%d" % (k, int(time.time())))).get("generated_at") for k in (PREMIUM, COUNTRY, INDUSTRY)}
            except Exception as e:            # noqa: BLE001
                served = {"error": str(e)[:80]}
            if served == {PREMIUM: P["generated_at"], COUNTRY: K["generated_at"], INDUSTRY: I["generated_at"]}:
                break
            time.sleep(15)
        ck(served == {PREMIUM: P["generated_at"], COUNTRY: K["generated_at"], INDUSTRY: I["generated_at"]}, "the site serves the three documents as they are in the bucket (%s)" % P["generated_at"])

        r.section("4. EVA on the bank's premium")
        E = doc_of(EVA_KEY)
        w = E.get("wacc_inputs") or {}
        before = {"generated_at": E.get("generated_at"), "version": E.get("version"), "premium": w.get("equity_risk_premium_pct"), "source": w.get("source_erp"), "state": E.get("universe_state"),
                  "creators": "%s of %s" % (E.get("n_value_creators"), E.get("n_valid")), "medians": E.get("universe_medians"),
                  "market": {k: (E.get("market") or {}).get(k) for k in ("n_valid", "n_value_creators", "pct_value_creators", "regime", "medians", "n_super_compounders")}}
        say("before: %s" % json.dumps(before))
        if w.get("erp_fixed") is False and w.get("erp_as_of") == us["as_of"] and w.get("equity_risk_premium_pct") == us["premium_pct"]:
            say("the live document already carries the bank's premium: the engine is not run again")
        else:
            ecf, esame = same_package(lam, EVA_FN, ROOT / "aws/lambdas" / EVA_FN / "source", ["lambda_function.py"])
            say("%s updated %s | files as in this checkout: %s" % (EVA_FN, (ecf.get("LastModified") or "")[:19], esame))
            if not all(esame.values()):
                r.fail("the deployed EVA engine is not this checkout's; it was not invoked")
                sys.exit(1)
            say("ONE run of the EVA engine by hand, now (disclosed to Khalid; its own run is at 00:45 UTC)")
            t0 = time.time()
            resp = lam.invoke(FunctionName=EVA_FN, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_7004_premium"}).encode())
            body = json.loads(resp["Payload"].read() or b"{}")
            say("answered in %.0f s: status %s, error %s" % (time.time() - t0, body.get("statusCode"), resp.get("FunctionError")))
            ck(not resp.get("FunctionError") and body.get("statusCode") == 200, "the EVA run finished without an error")
            E = doc_of(EVA_KEY)
            w = E.get("wacc_inputs") or {}
        after = {"generated_at": E.get("generated_at"), "version": E.get("version"), "premium": w.get("equity_risk_premium_pct"), "source": w.get("source_erp"), "state": E.get("universe_state"),
                 "creators": "%s of %s" % (E.get("n_value_creators"), E.get("n_valid")), "medians": E.get("universe_medians"),
                 "market": {k: (E.get("market") or {}).get(k) for k in ("n_valid", "n_value_creators", "pct_value_creators", "regime", "medians", "n_super_compounders")}}
        say("after:  %s" % json.dumps(after))
        ck(E.get("version") == "1.2.0" and w.get("equity_risk_premium_pct") == us["premium_pct"] and w.get("erp_fixed") is False and w.get("erp_as_of") == us["as_of"] and w.get("erp_fixed_until_v1_2_pct") == 5.5,
           "the EVA document is built on the bank's premium: %s%% as of %s, not a fixed figure" % (w.get("equity_risk_premium_pct"), w.get("erp_as_of")))
        ck(str(w.get("source_erp", "")).startswith("Damodaran implied ERP at the start of %s" % us["as_of"][:7]) and ("ERP=%s%%" % us["premium_pct"]) in (E.get("methodology") or {}).get("wacc", ""),
           "and says so in its own words: %s" % w.get("source_erp"))
        for attempt in range(12):
            try:
                if json.loads(fetch("/%s?probe=%d" % (EVA_KEY, int(time.time())))).get("generated_at") == E.get("generated_at"):
                    break
            except Exception:                 # noqa: BLE001
                pass
            time.sleep(15)

        r.section("5. The pages on the live site, in a real browser")
        ready = {}
        for attempt in range(40):
            try:
                html = fetch("/cost-of-capital.html?probe=%d" % int(time.time()))
                tag = re.search(rb'<script src="(/jh-cost-of-capital\.js\?v=[0-9A-Za-z._-]+)"', html)
                ready = {"page": b'id="cc-host"' in html, "script": bool(tag) and b"offHtml" in fetch(tag.group(1).decode() + "&probe=%d" % int(time.time())),      # the script that keeps an off-scale figure out of the picture
                         "eva.html": b'id="erp-source"' in fetch("/eva.html?probe=%d" % int(time.time()))}
            except Exception as e:            # noqa: BLE001
                ready = {"error": str(e)[:80]}
            if ready and all(v is True for v in ready.values()):
                break
            time.sleep(20)
        say("served after %d tries: %s" % (attempt + 1, json.dumps(ready)))
        if not (ready and all(v is True for v in ready.values())):
            r.fail("the live site does not serve the pages yet; the browser part was not run")
        else:
            sync_playwright = A.ensure_playwright(r.log)
            with sync_playwright() as p:
                browser = A.launch(p, r.log)

                def new_page(phone=False):
                    ctx = browser.new_context(viewport={"width": 390, "height": 844}, device_scale_factor=2, is_mobile=True, has_touch=True, user_agent=UA, accept_downloads=True) if phone \
                        else browser.new_context(viewport={"width": 1500, "height": 950}, user_agent=UA, accept_downloads=True)
                    page, errors = ctx.new_page(), []
                    page.on("pageerror", lambda e: errors.append(str(e)[:200]))
                    return page, errors
                try:
                    browse(new_page, P, K, I, E, ck, shots, say, tables)
                except Exception as exc:      # noqa: BLE001
                    ck(False, "the browser part stopped: %s: %s" % (type(exc).__name__, str(exc)[:500]))
                browser.close()

        r.section("screenshots (jpeg, base64)")
        for k, img in sorted(shots.items()):
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(premium_pct=us["premium_pct"], as_of=us["as_of"], files=len(files), tables=I["n_tables"], countries=K["n"], eva_premium=w.get("equity_risk_premium_pct"), failed_checks=len(fails))
        if fails:
            r.fail("%d check(s) failed" % len(fails))
            sys.exit(1)
        r.ok("LIVE: the bank holds his %d files and publishes the premium (%s%% at the start of %s), %d countries and %d industry tables; the EVA engine is built on that premium; the pages draw it" % (
            len(files), us["premium_pct"], us["as_of"][:7], K["n"], I["n_tables"]))


if __name__ == "__main__":
    main()
