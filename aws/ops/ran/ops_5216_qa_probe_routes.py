"""ops_5216 -- READ-ONLY production probe for the QA audit brief (2026-09-07).

Reproduces, in a live Chrome, the user-visible states of / , /khalid.html , /khalidrisk.html , /engines.html ,
/status.html (+ its consolidated destination) and /fusion.html: final URL + HTTP status, page errors, console errors,
failed network requests, fallback strings still visible after 10s ("Risk artifact loading", "loading catalogs",
"Risk artifact unavailable", "ENGINE DATA UNAVAILABLE", "0 risks/vetoes/conflicts"), key DOM facts and a screenshot;
then reads the artifacts those pages depend on (age, size, status fields) and the cadence/last-log of their producers.
Writes nothing to S3, deploys nothing, changes nothing. sys.exit(1) only if the probe itself cannot run.
"""
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=60)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
logs = boto3.client("logs", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
ROUTES = ["/", "/khalid.html", "/khalidrisk.html", "/engines.html", "/status.html", "/system.html", "/fusion.html"]
FALLBACKS = ["Risk artifact loading", "loading catalogs", "Risk artifact unavailable", "artifact unavailable", "ENGINE DATA UNAVAILABLE", "0 risks", "0 vetoes", "0 conflicts", "no opportunities", "loading…", "Loading…"]
ARTIFACTS = ["data/khalid-risk.json", "data/engine-fusion.json", "data/khalid.json", "data/khalid-candidates.json", "data/engine-registry.json", "data/site/section-registry.json", "data/jh-fusion.json", "data/risk-gate.json"]
ENGINES = ["justhodl-khalid", "justhodl-khalid-risk", "justhodl-engine-fusion"]
FACTS_JS = """() => {
  const t = (id) => (document.getElementById(id) || {}).textContent || null;
  const q = (s) => document.querySelectorAll(s).length;
  const body = document.body ? document.body.innerText : "";
  return {
    title: document.title, sections: q('section'), tables: q('table'), rows: q('tbody tr'), links: q('a[href]'), buttons: q('button'),
    hd_authority: t('hd-authority-title'), hd_catalog: t('hd-catalog'), hd_dir_count: t('hd-dir-count'), hd_cap: t('hd-risk-cap'), hd_cov: t('hd-fusion-coverage'),
    hd_blocks: q('.hd-block, [data-block], .hd-grid > *'), khalid_unavail: body.includes('ENGINE DATA UNAVAILABLE'), body_len: body.length,
    engines_rows: q('#engines tbody tr, .engine-row, .engine, .eng, [data-engine]'), errbox: t('errbox'),
    snippet: body.replace(/\\s+/g, ' ').slice(0, 700)
  };
}"""


def get_obj(key):
    r = s3.get_object(Bucket=BUCKET, Key=key)
    body = r["Body"].read()
    try:
        d = json.loads(body)
    except Exception:
        d = None
    return d, r["LastModified"], len(body)


def last_log(fn):
    try:
        st = logs.describe_log_streams(logGroupName="/aws/lambda/" + fn, orderBy="LastEventTime", descending=True, limit=1).get("logStreams") or []
        if st and st[0].get("lastEventTimestamp"):
            return datetime.fromtimestamp(st[0]["lastEventTimestamp"] / 1000, tz=timezone.utc).isoformat()[:19]
    except Exception as exc:
        return "err " + str(exc)[:60]
    return None


with report("ops_5216_qa_probe_routes") as R:
    R.heading("ops 5216 -- QA probe: live route states (read-only)")
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
        for route in ROUTES:
            R.section("route %s" % route)
            ctx = browser.new_context(viewport={"width": 1440, "height": 1100})
            pg = ctx.new_page()
            errors, console, failed, responses = [], [], [], []
            pg.on("pageerror", lambda e: errors.append(str(e)[:200]))
            pg.on("console", lambda m: console.append("%s: %s" % (m.type, m.text[:160])) if m.type in ("error", "warning") else None)
            pg.on("requestfailed", lambda rq: failed.append("%s %s" % (rq.url[:140], rq.failure)))
            pg.on("response", lambda rs: responses.append((rs.status, rs.url[:140])) if rs.status >= 400 else None)
            t0 = time.time()
            try:
                resp = pg.goto("https://justhodl.ai%s?v=%d" % (route, int(time.time())), wait_until="domcontentloaded", timeout=60000)
                status = resp.status if resp else None
            except Exception as exc:
                status = "nav-error %s" % str(exc)[:120]
            pg.wait_for_timeout(10000)
            final = pg.url
            try:
                facts = pg.evaluate(FACTS_JS)
            except Exception as exc:
                facts = {"evaluate_error": str(exc)[:160]}
            body = facts.get("snippet") or ""
            try:
                full = pg.evaluate("() => document.body ? document.body.innerText : ''")
            except Exception:
                full = ""
            present = [f for f in FALLBACKS if f in full]
            pg.screenshot(path=str(SHOTS / ("ops5216_%s_1440.png" % (route.strip("/").replace(".html", "") or "home"))), full_page=False)
            R.log("   HTTP %s -> final %s in %.1fs" % (status, final.replace("https://justhodl.ai", ""), time.time() - t0))
            R.log("   facts: %s" % json.dumps({k: v for k, v in facts.items() if k != "snippet"})[:600])
            R.log("   fallback strings still visible after 10s: %s" % (present or "none"))
            R.log("   page errors: %s" % (errors[:4] or "none"))
            R.log("   console errors/warnings: %s" % (console[:5] or "none"))
            R.log("   failed requests: %s" % (failed[:6] or "none"))
            R.log("   >=400 responses: %s" % (responses[:8] or "none"))
            R.log("   text: %s" % body[:420])
            R.kv(route=route, http=status, final=final.replace("https://justhodl.ai", "")[:40], errors=len(errors), console=len(console), failed=len(failed) + len(responses), fallbacks=",".join(present)[:80], rows=facts.get("rows"), sections=facts.get("sections"))
            ctx.close()
        browser.close()

    R.section("artifacts the pages depend on")
    now = datetime.now(timezone.utc)
    for key in ARTIFACTS:
        try:
            d, lm, size = get_obj(key)
            age_h = (now - lm).total_seconds() / 3600
            top = {}
            if isinstance(d, dict):
                for k in ("status", "as_of", "generated_at", "engine", "schema_version", "policy", "capital_decision", "exposure_cap_pct", "n_candidates", "n_opportunities", "mode", "run_id", "coverage"):
                    if k in d:
                        v = d[k]
                        top[k] = v if not isinstance(v, (dict, list)) else (json.dumps(v)[:160])
                keys = list(d.keys())[:30]
            else:
                keys = ["<%s>" % type(d).__name__]
            R.log("   %s: %.1fh old, %d bytes, keys %s" % (key, age_h, size, keys))
            R.log("      top: %s" % json.dumps(top)[:500])
            if key == "data/khalid.json" and isinstance(d, dict):
                for k in ("opportunities", "candidates", "picks", "rows", "board"):
                    if k in d:
                        v = d[k]; R.log("      %s: %s items" % (k, len(v) if hasattr(v, "__len__") else v))
            if key == "data/engine-fusion.json" and isinstance(d, dict):
                R.log("      packets %s active / inactive %s / coverage %s / status %s" % (len(d.get("packets") or []), len(d.get("inactive_packets") or []), json.dumps(d.get("coverage"))[:160], d.get("status")))
            if key == "data/engine-registry.json" and isinstance(d, dict):
                R.log("      registry entries: %s" % (len(d.get("engines") or d.get("entries") or d.get("items") or [])))
        except Exception as exc:
            R.warn("   %s: %s" % (key, str(exc)[:160]))

    R.section("producers")
    for fn in ENGINES:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            names = []
            for s in sch.list_schedules(NamePrefix=fn).get("Schedules") or []:
                d = sch.get_schedule(Name=s["Name"], GroupName=s.get("GroupName") or "default")
                names.append("%s %s %s" % (s["Name"], d.get("ScheduleExpression"), d.get("State")))
            R.log("   %s: modified %s | %sMB/%ss | schedules %s | last log %s" % (fn, cfg.get("LastModified", "")[:19], cfg.get("MemorySize"), cfg.get("Timeout"), names or "-", last_log(fn)))
            # recent errors in the last 6h
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=int((time.time() - 6 * 3600) * 1000), filterPattern="?ERROR ?Traceback ?Task timed out", limit=5)
                msgs = [e["message"][:200].strip() for e in ev.get("events") or []]
                R.log("      recent errors (6h): %s" % (msgs[:3] or "none"))
            except Exception as exc:
                R.log("      log filter: %s" % str(exc)[:100])
        except Exception as exc:
            R.warn("   %s: %s" % (fn, str(exc)[:160]))
    R.ok("probe complete (read-only)")
    sys.exit(0)
