"""ops 6984 - read-only: the Corp bonds desk after the engine's OWN run with the sleeves read from the warehouse.

The ICE BofA sleeves were empty on every kept run from 2026-11-11 (ops 6982): the four tiles at the top of the Corp
bonds page read a dash and both sleeve panels were empty. The engine was changed and deployed 2026-11-26 about 20:05
UTC and not run by hand; its own run is 20:15 UTC Monday to Friday.

  1. the live document: written by the engine's own run after the deploy, sleeves present, each with the status the
     page reads as printed, the fund holdings and the TRACE aggregates as before
  2. the page on justhodl.ai in a real browser: the four tiles carry a figure equal to the document's, the sleeves
     chart and table are filled, and "did not print this run" names only what really did not
A run that has not happened yet is reported as pending, not as a failure. Nothing is written, no engine is invoked.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import importlib.util
import json
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / "aws" / "ops"))
NAME = "ops_6984_corp_bonds_sleeves_own_run"
BASE = "https://justhodl.ai"
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36 jh-ops-probe"
DEPLOYED = "2026-11-26T20:00"
READ = r"""() => { const t = (s) => (s || '').replace(/\s+/g, ' ').trim();
  const kp = {}; document.querySelectorAll('.kpi').forEach(k => { kp[t((k.querySelector('.label') || {}).textContent)] = t((k.querySelector('.val') || {}).textContent) + ' | ' + t((k.querySelector('.sub') || {}).textContent); });
  const panel = (name) => Array.from(document.querySelectorAll('.panel')).find(p => t((p.querySelector('h3') || {}).textContent).indexOf(name) === 0);
  const g = panel('Global corporate sleeves'), c = panel('Sleeve changes'), cov = panel('What this engine covers');
  const rows = c ? Array.from(c.querySelectorAll('table tr')).slice(1).map(r => Array.from(r.children).map(td => t(td.textContent))) : [];
  return { stamp: t((document.getElementById('ts') || document.querySelector('.ts') || {}).textContent), kpi: kp, bars: g ? g.querySelectorAll('.bar-row').length : -1, bar_note: g ? t((g.querySelector('.note') || {}).textContent) : null,
    rows: rows, missed: cov ? (t(cov.innerText).match(/FRED sleeves that did not print this run: [^\n]*/) || [null])[0] : null, bad: /NaN|undefined/.test(document.getElementById('root') ? document.getElementById('root').innerText.slice(0, 6000) : '') }; }"""


def main():
    import boto3
    from ops_report import report
    spec = importlib.util.spec_from_file_location("ops_6846", str(sorted(Path(__file__).resolve().parent.glob("ops_6846_*.py"))[0]))
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    s3 = boto3.client("s3", region_name="us-east-1")
    fails, shots = [], {}
    with report(NAME) as r:
        r.heading("6984 - Corp bonds after the engine's own run: the ICE sleeves (read-only), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        def ck(cond, msg):
            r.log(("PASS  " if cond else "FAIL  ") + msg)
            if not cond:
                fails.append(msg)
            return cond

        r.section("1. The live document")
        doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/corp-bonds.json")["Body"].read())
        src = doc.get("sources") or {}
        fred = src.get("fred") or {}
        sleeves = doc.get("sleeves") or []
        live = [k for k, v in fred.items() if v == "live"]
        r.log("written %s | %d sleeves | FRED ids live %d of %d | not printed: %s" % (doc.get("generated_at"), len(sleeves), len(live), len(fred), json.dumps({k: v for k, v in fred.items() if v != "live"})[:400]))
        r.log("read from: %s" % json.dumps(src.get("sleeves_via"))[:600])
        if str(doc.get("generated_at", "")) < DEPLOYED:
            r.log("PENDING  the engine has not run on its own since the deploy (its run is 20:15 UTC Monday to Friday)")
            r.kv(failed_checks=0, pending=1)
            r.ok("nothing wrong; the engine's own run is still to come")
            return
        funds = (doc.get("issues") or {}).get("funds") or []
        trace = src.get("trace") or {}
        ck(len(sleeves) >= 20 and len(live) == len(sleeves), "%d sleeves are in the document, each marked live" % len(sleeves))
        ck(all(x.get("via") for x in sleeves) and sum(1 for x in sleeves if x.get("via") == "warehouse") >= 20, "each says where it was read: %d from the warehouse" % sum(1 for x in sleeves if x.get("via") == "warehouse"))
        by = {x["id"]: x for x in sleeves}
        ck(all(k in by for k in ("us_ig", "us_hy", "em", "asia_usd")) and by["us_ig"]["level"] < by["us_hy"]["level"], "the four sleeves behind the page's tiles are there (US IG %s bp, US HY %s bp, EM %s bp, Asia %s bp)" % tuple((by.get(k) or {}).get("level") for k in ("us_ig", "us_hy", "em", "asia_usd")))
        ck(sum(1 for f in funds if f.get("n")) == len(funds) == 16 and (doc.get("issues") or {}).get("n_bonds", 0) > 20000, "the fund holdings arrived as before (%d funds, %s bonds, holdings of %s)" % (len(funds), (doc.get("issues") or {}).get("n_bonds"), (doc.get("issues") or {}).get("as_of")))
        ck(sum(1 for v in trace.values() if str(v).startswith("live")) >= 4, "the TRACE aggregates arrived as before (%s)" % json.dumps(trace))
        for x in sleeves[:24]:
            r.log("  %-20s %8s %-3s as of %s | 1d %s, 5d %s | 5y pct %s | %s" % (x["label"], x["level"], x["unit"], x["as_of"], x.get("chg_1d"), x.get("chg_5d"), x.get("pct_5y"), x.get("via")))

        r.section("2. The page, live")
        sync_playwright = A.ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = A.launch(p, r.log)
            page = browser.new_context(viewport={"width": 1400, "height": 1000}, user_agent=UA).new_page()
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            seen = None
            for attempt in range(40):       # the site keeps a document for about ten minutes
                page.goto("%s/corpbonds.html?nocache=%d" % (BASE, int(time.time())), wait_until="domcontentloaded")
                try:
                    page.wait_for_selector(".kpi .val", timeout=60000)
                except Exception:  # noqa: BLE001
                    pass
                page.wait_for_timeout(1500)
                seen = page.evaluate(READ)
                if seen["bars"] > 0:
                    break
                r.log("attempt %d: the page still shows the earlier document (%s)" % (attempt + 1, seen["stamp"][:60]))
                page.wait_for_timeout(30000)
            r.log("the page says: %s" % seen["stamp"])
            r.log("tiles: %s" % json.dumps(seen["kpi"], ensure_ascii=False))
            want = {"US IG OAS": by["us_ig"]["level"], "US HY OAS": by["us_hy"]["level"], "EM corp OAS": by["em"]["level"], "Asia USD OAS": by["asia_usd"]["level"]}
            ok = all(seen["kpi"].get(k, "").split(" |")[0] == "%d bp" % round(v) for k, v in want.items())
            ck(ok, "the four tiles carry the document's figures (%s)" % ", ".join("%s %d bp" % (k, round(v)) for k, v in want.items()))
            n_oas = sum(1 for x in sleeves if x.get("kind") == "oas" and x.get("level") is not None)
            ck(seen["bars"] == n_oas, "the sleeves chart draws every spread sleeve (%d bars for %d)" % (seen["bars"], n_oas))
            ck(len(seen["rows"]) == len(sleeves), "the changes table lists every sleeve (%d rows for %d)" % (len(seen["rows"]), len(sleeves)))
            hy = next((x for x in seen["rows"] if x and x[0] == "US HY"), None)
            ck(bool(hy) and hy[3] == "%d bp" % round(by["us_hy"]["level"]) and hy[-1] == by["us_hy"]["as_of"], "US HY on the page is the document's (%s)" % (hy and [hy[3], hy[4], hy[-1]]))
            missing = sorted(k for k, v in fred.items() if v != "live")
            said = seen["missed"] or ""
            ck(all(k in said for k in missing) and not any(k in said for k in live), "\"did not print this run\" names only what did not print (%s)" % (said[:160] or "nothing"))
            ck(not seen["bad"] and not errors, "nothing broken on the page (%s)" % (errors[:2] or "no error"))
            shots["corp-bonds-top"] = page.screenshot(type="jpeg", quality=55)
            page.evaluate("() => { const p = Array.from(document.querySelectorAll('.panel')).find(x => /Global corporate sleeves/.test(x.textContent.slice(0, 60))); if (p) p.scrollIntoView(); }")
            page.wait_for_timeout(500)
            shots["corp-bonds-sleeves"] = page.screenshot(type="jpeg", quality=55)
            browser.close()
        r.section("screenshots (jpeg, base64)")
        for k, img in shots.items():
            b64 = base64.b64encode(img).decode("ascii")
            r.lines.append("<!-- SHOT %s %d bytes" % (k, len(img)))
            r.lines.extend(b64[i:i + 16000] for i in range(0, len(b64), 16000))
            r.lines.append("SHOT-END -->")
        r.kv(failed_checks=len(fails), sleeves=len(sleeves), pending=0)
        if fails:
            r.fail("; ".join(fails)[:700])
            sys.exit(1)
        r.ok("the sleeves are back on the Corp bonds desk, from the engine's own run")


if __name__ == "__main__":
    main()
