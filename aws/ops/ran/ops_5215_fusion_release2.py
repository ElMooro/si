"""ops_5215 -- JustHodl Intelligence Network Release 2: read API v1 + Fusion Desk page + shadow ledger.

Shipped in this commit (deployed by the workflows on push):
  * cloudflare/workers/justhodl-data-proxy/src/fusion_api.js + /api/v1/* zone routes   (deploy-workers.yml)
  * fusion.html (marker JH_FUSION_DESK_V1) + nav pin                                   (pages.yml)
  * justhodl-jh-fusion v1.1.0: shadow comparison doc data/jh-fusion/shadow.json + jh_fusion rows into justhodl-signals
This op (runner): re-deploys the fusion Lambda from source (idempotent), forces one fusion run and gates shadow.json
+ the graded-ledger rows, waits for the worker (X-JH-API header on /api/v1/health) and validates every route's shape
on the live domain, waits for the page at the edge and drives it in Chrome at 1440 / 390. Hard failures -> sys.exit(1).
"""
import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

FUSION = "justhodl-jh-fusion"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
ddb = boto3.resource("dynamodb", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
FUSION_KEY = "data/jh-fusion.json"
SHADOW_KEY = "data/jh-fusion/shadow.json"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def http(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops5215", "Cache-Control": "no-cache", "Pragma": "no-cache"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), e.read()
    except Exception as e:
        return 0, {}, str(e).encode()


with report("ops_5215_fusion_release2") as R:
    R.heading("ops 5215 -- fusion Release 2: API v1 + Fusion Desk + shadow ledger")
    # ---- 1. fusion Lambda v1.1.0 (idempotent with deploy-lambdas.yml)
    R.section("fusion Lambda")
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FUSION / "config.json").read_text())
    create_or_update_lambda(report=R, function_name=FUSION, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FUSION / "source"),
                            env_vars=dict(cfg_json.get("env") or {}), timeout=int(cfg_json.get("timeout") or 300), memory=int(cfg_json.get("memory") or 1024),
                            description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    for _ in range(40):
        c = lam.get_function_configuration(FunctionName=FUSION)
        if c.get("LastUpdateStatus") in (None, "Successful") and c.get("State") == "Active":
            break
        time.sleep(5)
    prior_run = None
    try:
        prior_run = get_json(FUSION_KEY).get("run_id")
    except Exception:
        pass
    lam.invoke(FunctionName=FUSION, InvocationType="Event", Payload=json.dumps({"mode": "ops5215", "force": True}).encode())
    fdoc = None; t0 = time.time()
    while time.time() - t0 < 240:
        time.sleep(10)
        try:
            d = get_json(FUSION_KEY)
        except Exception:
            continue
        if d.get("run_id") and d.get("run_id") != prior_run:
            fdoc = d; break
    if not fdoc:
        FAILS.append("forced fusion run did not land in 240s")
    else:
        R.ok("   fusion %s v%s: %s entities, shadow=%s, shadow block %s" % (fdoc["run_id"], fdoc.get("version"), fdoc["stats"]["n_entities"], fdoc.get("shadow_mode"), json.dumps(fdoc.get("shadow"))[:300]))
        if fdoc.get("version") != "1.1.0":
            FAILS.append("fusion version %s (expected 1.1.0)" % fdoc.get("version"))
        # ---- shadow doc + graded ledger rows
        try:
            sh = get_json(SHADOW_KEY)
            R.ok("   shadow.json: %d rows, agreement %s on %s reads, logging %s" % (len(sh.get("rows") or []), sh.get("agreement_rate"), sh.get("n_compared"), json.dumps(sh.get("logging"))[:200]))
            if sh.get("fusion_run_id") != fdoc["run_id"] or len(sh.get("rows") or []) < 14:
                FAILS.append("shadow.json stale or short: run %s rows %s" % (sh.get("fusion_run_id"), len(sh.get("rows") or [])))
            lg = sh.get("logging") or {}
            if lg.get("enabled") and lg.get("logged", 0) == 0 and lg.get("skipped", 0) < 14:
                FAILS.append("shadow logging enabled but logged 0 rows: %s" % json.dumps(lg)[:300])
            for r in sh.get("rows") or []:
                R.kv(entity=r["entity_id"], fusion=r["fusion"].get("score"), direction=r["fusion"].get("direction"), capital=r["fusion"].get("capital_decision"), agree="%s/%s" % (r["n_agree"], r["n_compared"]), existing=",".join(sorted(r["existing"].keys()))[:120])
        except Exception as exc:
            FAILS.append("shadow.json: %s" % str(exc)[:160])
        today = datetime.now(timezone.utc).date().isoformat()
        table = ddb.Table("justhodl-signals")
        found = []
        for eid, r in (fdoc.get("entities") or {}).items():
            et, sym = eid.split(":", 1)
            ysym = {"crypto": sym + "-USD"}.get(et, sym)
            if et == "market":
                continue
            try:
                it = table.get_item(Key={"signal_id": "jh_fusion#%s#%s" % (ysym, today)}).get("Item")
            except Exception as exc:
                R.warn("   ddb get_item %s: %s" % (ysym, str(exc)[:120])); it = None
            if it:
                found.append("%s %s %s" % (ysym, it.get("predicted_direction"), it.get("baseline_price")))
        R.log("   jh_fusion ledger rows today: %d -> %s" % (len(found), found[:14]))
        directional = sum(1 for r in (fdoc.get("entities") or {}).values() for h in [((r.get("horizons") or {}).get(r.get("best_horizon")) or {})] if abs(h.get("fusion_score") or 0) >= 0.15 and (h.get("confidence") or 0) >= 0.3 and h.get("capital_decision") != "BLOCKED" and not r["entity_id"].startswith("market:"))
        if directional and not found:
            FAILS.append("no jh_fusion rows in justhodl-signals although %d directional reads qualified" % directional)

    # ---- 2. API on the live domain (worker deploy lands via deploy-workers.yml)
    R.section("API v1 (justhodl.ai/api/v1/*)")
    ok = False; t1 = time.time()
    while time.time() - t1 < 420:
        st, hd, body = http("https://justhodl.ai/api/v1/health?v=%d" % int(time.time()))
        if st == 200 and (hd.get("X-JH-API") or hd.get("x-jh-api")) == "v1":
            ok = True; break
        time.sleep(20)
    R.log("   /api/v1/health live at the edge: %s (%ss)" % (ok, int(time.time() - t1)))
    if not ok:
        FAILS.append("API did not come up on justhodl.ai within 7 min (worker deploy)")
    else:
        checks = [
            ("/api/v1/fusion", lambda j: j.get("n", 0) >= 14 and "rows" in j and "regime" in j and all(k in j["rows"][0] for k in ("fusion_score", "confidence", "fusion_coverage", "contradiction_score", "capital_decision"))),
            ("/api/v1/fusion/NVDA", lambda j: j.get("entity", {}).get("entity_id") == "equity:NVDA" and j["entity"]["horizons"] and "signals" not in next(iter(j["entity"]["horizons"].values()))),
            ("/api/v1/fusion/equity:NVDA?horizon=INTERMEDIATE&full=1", lambda j: "INTERMEDIATE" in j.get("entity", {}).get("horizons", {}) and "signals" in j["entity"]["horizons"]["INTERMEDIATE"]),
            ("/api/v1/fusion/NVDA/changes", lambda j: "velocity" in j and "horizons" in j),
            ("/api/v1/signals/SPY?family=RISK", lambda j: j.get("n", 0) >= 1 and all(s["family"] == "RISK" for s in j["signals"])),
            ("/api/v1/regime/current", lambda j: j.get("regime", {}).get("label") not in (None, "UNKNOWN") and "critical_dependencies" in j),
            ("/api/v1/opportunities?min_confidence=0.3&limit=5", lambda j: "rows" in j and len(j["rows"]) <= 5 and all("opportunity_rank_score" in r for r in j["rows"])),
            ("/api/v1/fusion/ZZZZ", lambda j: j.get("error") == "unknown entity"),
        ]
        for path, fn in checks:
            st, hd, body = http("https://justhodl.ai%s%sv=%d" % (path, "&" if "?" in path else "?", int(time.time())))
            try:
                j = json.loads(body or b"{}")
            except Exception:
                j = {}
            good = (st in (200, 404)) and fn(j)
            R.log("   %s -> HTTP %s, %d bytes, ok=%s" % (path, st, len(body or b""), good))
            if not good:
                FAILS.append("API %s failed: HTTP %s %s" % (path, st, (body or b"")[:200]))

    # ---- 3. page at the edge + Chrome drive
    R.section("fusion.html")
    live = False
    for _ in range(40):
        st, hd, body = http("https://justhodl.ai/fusion.html?v=%d" % int(time.time()))
        live = st == 200 and b"JH_FUSION_DESK_V1" in body
        if live:
            break
        time.sleep(15)
    R.log("   fusion.html carries JH_FUSION_DESK_V1 at the edge: %s" % live)
    if not live:
        FAILS.append("page deploy not observed at the edge")
    else:
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
            for width, height in ((1440, 1100), (390, 844)):
                ctx = browser.new_context(viewport={"width": width, "height": height}, is_mobile=width < 700)
                pg = ctx.new_page()
                errors = []; csp = []
                pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                pg.on("console", lambda m: csp.append(m.text[:160]) if "Content Security Policy" in m.text else None)
                pg.goto("https://justhodl.ai/fusion.html?v=%d" % int(time.time()), wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(9000)
                pg.evaluate("""() => { const r = document.querySelectorAll('#board tbody tr.pick')[2]; if (r) r.click(); }""")
                pg.wait_for_timeout(800)
                facts = pg.evaluate("""() => ({ score: document.getElementById('wr-score').textContent, posture: document.getElementById('wr-posture').textContent,
                    legs: document.querySelectorAll('#legs tbody tr').length, board: document.querySelectorAll('#board tbody tr.pick').length,
                    evid: (document.querySelector('#evid .why') || {}).textContent ? document.querySelector('#evid .why').textContent.length : 0,
                    evrows: document.querySelectorAll('#evid .ev').length, tabs: document.querySelectorAll('#hz-tabs button').length,
                    helps: document.querySelectorAll('#board th .help').length, defs: document.querySelectorAll('#defs dt').length,
                    opps: document.querySelectorAll('#opps tbody tr').length, shadow: document.querySelectorAll('#shadow tbody tr').length,
                    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, err: document.getElementById('errbox').textContent })""")
                pg.screenshot(path=str(SHOTS / f"ops5215_fusion_{width}.png"))
                R.log("   %4dpx: %s errors=%s csp=%s" % (width, json.dumps(facts)[:400], errors[:2], csp[:2]))
                if facts["score"] in ("—", "") or facts["legs"] < 4 or facts["board"] < 14 or facts["helps"] < 10 or facts["defs"] < 8 or facts["evid"] < 120 or facts["shadow"] < 14 or errors or csp or facts["err"]:
                    FAILS.append("%dpx render: %s errors=%s csp=%s" % (width, json.dumps(facts)[:260], errors[:2], csp[:2]))
                if width == 390 and facts["overflow"] > 0:
                    FAILS.append("390px overflow %dpx" % facts["overflow"])
                ctx.close()
            browser.close()

    if FAILS:
        R.section("FAILS")
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("GREEN -- Release 2 live: /api/v1/*, fusion.html, shadow ledger (jh_fusion in justhodl-signals) -- still shadow mode")
    sys.exit(0)
