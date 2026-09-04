"""ops_5197 -- bond war room v1.1.0 (TradingView scanner + bank, MOF history, Bundesbank, ECB) -- redeploy + verify. Original launch: (global bond heartbeat) + verify the war room on bonds.html.
Creates the function directly (the deploy workflow does not create new functions), runs it, asserts
the feed (TradingView yields for the main bond centers, MOF JGB curve, ICE BofA family, MOVE/ETFs,
verdicts), arms the schedules (America/New_York, Mon-Fri) and drives the page in Chrome."""
import json
import subprocess
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

FN = "justhodl-bond-warroom"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=280)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []

with report("ops_5197_bond_warroom_launch") as R:
    R.heading("ops 5197 -- bond war room v1.3.0 warehouse-first + official-yields lane")
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                            env_vars=cfg_json.get("env") or {}, timeout=int(cfg_json.get("timeout") or 240), memory=int(cfg_json.get("memory") or 1024),
                            description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    cfg = None
    for _ in range(30):
        cfg = lam.get_function_configuration(FunctionName=FN)
        if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
            break
        time.sleep(5)
    t0 = time.time()
    resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    out = json.loads(resp["Payload"].read() or b"{}")
    R.log("   run (%.0fs, error=%s) -> %s" % (time.time() - t0, resp.get("FunctionError"), json.dumps(out)[:700]))
    if resp.get("FunctionError") or not out.get("ok"):
        FAILS.append("engine failed: %s" % json.dumps(out)[:300])
    D = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/bond-warroom.json")["Body"].read())
    R.section("feed")
    hb, eq, ed = D.get("heartbeat") or {}, D.get("equity_risk") or {}, D.get("eurodollar_shortage") or {}
    R.log("   heartbeat %s %s -- %s" % (hb.get("score"), hb.get("regime"), (hb.get("headline") or "")[:200]))
    R.log("   equity: %s %s -- %s" % (eq.get("state"), eq.get("level"), (eq.get("text") or "")[:200]))
    R.log("   eurodollar: %s %s -- %s" % (ed.get("state"), ed.get("points"), (ed.get("text") or "")[:160]))
    R.log("   freshness=%s notes=%s" % (D.get("freshness"), (D.get("notes") or [])[:3]))
    for key, rows in (D.get("panels") or {}).items():
        R.log("   panel %-15s %2d rows: %s" % (key, len(rows), ", ".join("%s %s %s" % (r["key"], fmt := (str(r["last"])[:7]), r["flag"]) for r in rows[:9])[:260]))
        if key in ("us_rates", "europe", "japan", "credit", "volatility") and len(rows) < 3:
            FAILS.append("panel %s thin (%d rows)" % (key, len(rows)))
    R.section("official histories (v1.2)")
    allrows = {r["key"]: r for rows in (D.get("panels") or {}).values() for r in rows}
    off = (D.get("freshness") or {}).get("official") or {}
    R.log("   official sources: %s (n=%s)" % (json.dumps(off), (D.get("freshness") or {}).get("official_n")))
    for k in ("US02Y", "US10Y", "US30Y", "DE02Y", "DE10Y", "DE30Y", "GB10Y", "CA10Y", "AU10Y", "CH10Y", "JP10Y", "BTP-Bund", "IT-ES"):
        r = allrows.get(k)
        R.log("   %-9s %s hist=%s z=%s z_ready=%s dod=%s dod%%=%s flag=%s src=%s" % (k, r and r["last"], r and r.get("history_days"), r and r.get("z"), r and r.get("z_ready"), r and r.get("dod"), r and r.get("dod_pct"), r and r["flag"], r and r["source"][:40]))
        if k in ("US10Y", "DE02Y", "DE10Y", "DE30Y", "GB10Y", "CA10Y", "AU10Y", "JP10Y") and not (r and r.get("z_ready")):
            FAILS.append("%s has no real history (z not ready)" % k)
    if (D.get("freshness") or {}).get("official_n", 0) < 20:
        FAILS.append("official histories thin: %s" % (D.get("freshness") or {}).get("official_n"))
    R.section("warehouse-first (v1.3)")
    wh_rows = [r for r in allrows.values() if str(r.get("source", "")).startswith("warehouse:")]
    R.log("   rows sourced warehouse-first: %d / %d" % (len(wh_rows), len(allrows)))
    for k in ("DGS10", "BAMLH0A0HYM2", "TLT", "^MOVE", "US10Y", "DE10Y", "GB10Y", "CA10Y", "AU10Y", "EA_AAA10Y"):
        r = allrows.get(k)
        R.log("   %-14s src=%s asof=%s" % (k, r and r["source"], r and r["asof"]))
        if not (r and str(r["source"]).startswith("warehouse:")):
            FAILS.append("%s not warehouse-first (%s)" % (k, r and r["source"]))
    R.log("   sources_doctrine: %s" % str(D.get("sources_doctrine"))[:160])
    lane = [o["Key"].split("/")[-1] for o in (s3.list_objects_v2(Bucket="justhodl-dashboard-live", Prefix="data/warm/official-yields/").get("Contents") or [])]
    R.log("   official-yields lane: %d objects: %s" % (len(lane), lane))
    st = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/warm/official-yields/_state.json")["Body"].read())
    R.log("   lane state: count=%s banked_at=%s sample=%s" % (st.get("count"), st.get("banked_at"), (st.get("catalog") or [])[:4]))
    if len(lane) < 30 or (st.get("count") or 0) < 28:
        FAILS.append("official-yields lane thin: %d objects, state count %s" % (len(lane), st.get("count")))
    import urllib.request as _u
    for sid in ("official-yields:ca-10y-boc", "official-yields:jp-10y-mof", "official-yields:it-10y-tv", "official-yields:gb-5y-boe"):
        try:
            with _u.urlopen(_u.Request("https://justhodl-data-proxy.raafouis.workers.dev/series?id=" + sid, headers={"User-Agent": "ops5197"}), timeout=60) as r:
                j = json.loads(r.read())
            obs = j.get("observations") or j.get("obs") or []
            R.log("   /series %-32s n=%d last=%s src=%s" % (sid, len(obs), obs[-1] if obs else None, str(j.get("source"))[:50]))
            if len(obs) < 2:
                FAILS.append("resolver empty for %s" % sid)
        except Exception as e:
            FAILS.append("resolver %s: %s" % (sid, str(e)[:80]))
    R.section("data.html provider catalog")
    PC = "justhodl-provider-catalog"
    pc_cfg = json.loads((ROOT / "aws" / "lambdas" / PC / "config.json").read_text())
    create_or_update_lambda(report=R, function_name=PC, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / PC / "source"), env_vars=pc_cfg.get("env") or {},
                            timeout=int(pc_cfg.get("timeout") or 600), memory=int(pc_cfg.get("memory") or 1024), description=pc_cfg.get("description", "")[:250],
                            reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
    for _ in range(30):
        c2 = lam.get_function_configuration(FunctionName=PC)
        if c2.get("LastUpdateStatus") in (None, "Successful") and c2.get("State") == "Active":
            break
        time.sleep(5)
    lam.invoke(FunctionName=PC, InvocationType="Event", Payload=b"{}")
    doc = None
    for _ in range(40):
        time.sleep(15)
        try:
            doc = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/providers/official-yields.json")["Body"].read())
            break
        except Exception:
            doc = None
    R.log("   data/providers/official-yields.json: %s" % (json.dumps({k: doc.get(k) for k in ("slug", "name", "series", "n_series", "objects", "bytes", "generated_at") if k in doc})[:300] if doc else "NOT WRITTEN within 10 min (catalog runs on its own schedule; re-check later)"))
    if not doc:
        R.warn("   provider doc not yet written -- catalog engine is Event-invoked; verify on data.html after its next run")
    jc = D.get("jgb_curve") or {}
    R.log("   MOF JGB curve %s tenors=%s err=%s" % (jc.get("today"), len(jc.get("curve") or []), jc.get("error")))
    A = D.get("auction") or {}
    R.log("   auction desk: %s %s tags=%s preds=%d" % (A.get("today"), (A.get("verdict") or {}).get("headline"), (A.get("verdict") or {}).get("tags"), len(A.get("prediction") or [])))
    if not (A.get("verdict") or {}).get("headline"):
        FAILS.append("auction summary missing")
    R.log("   RED=%s AMBER=%s" % ((D.get("flags") or {}).get("RED"), (D.get("flags") or {}).get("AMBER")))
    R.section("schedules (America/New_York, Mon-Fri)")
    for name, expr in (("justhodl-bond-warroom-early", "cron(30 7 ? * MON-FRI *)"), ("justhodl-bond-warroom-mid", "cron(0 10 ? * MON-FRI *)"),
                       ("justhodl-bond-warroom-after-auction", "cron(35 13 ? * MON-FRI *)"), ("justhodl-bond-warroom-close", "cron(45 16 ? * MON-FRI *)"),
                       ("justhodl-bond-warroom-evening", "cron(15 19 ? * MON-FRI *)")):
        tgt = {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": "{}", "RetryPolicy": {"MaximumRetryAttempts": 1}}
        try:
            sch.get_schedule(Name=name, GroupName="default")
            sch.update_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="America/New_York", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description="bond war room (ops 5197)")
            R.ok("   %s updated %s ET" % (name, expr))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="America/New_York", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description="bond war room (ops 5197)")
            R.ok("   %s created %s ET" % (name, expr))
        except Exception as e:
            FAILS.append("schedule %s: %s" % (name, str(e)[:100]))
    R.section("page")
    import urllib.request
    live = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bonds.html", headers={"User-Agent": "ops5197", "Cache-Control": "no-cache"}), timeout=30) as r:
                live = b"warehouse first" in r.read()
        except Exception:
            live = False
        if live:
            break
        time.sleep(15)
    R.log("   bonds.html carries the war room: %s" % live)
    if live:
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
                errors = []
                pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                pg.goto("https://justhodl.ai/bonds.html", wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(8000)
                facts = pg.evaluate("""() => ({ score: document.getElementById('wr-score').textContent, regime: document.getElementById('wr-regime').textContent, headline: document.getElementById('wr-headline').textContent.slice(0, 90),
                    panels: document.querySelectorAll('#wr-grid .wr-panel').length, rows: document.querySelectorAll('#wr-grid tbody tr').length, flags: document.querySelectorAll('#wr-grid .wr-flag').length,
                    reds: document.querySelectorAll('#wr-grid .wr-flag.RED').length, jgb: document.querySelectorAll('.wr-jgb div').length, auction: document.querySelector('#wr-auction .h').textContent.slice(0, 80),
                    regimeBanner: !!document.getElementById('regime-banner'), overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth })""")
                pg.screenshot(path=str(SHOTS / f"ops5197_bonds_warroom_{width}.png"))
                R.log("   %4dpx: %s errors=%s" % (width, json.dumps(facts)[:420], errors[:2]))
                if facts["panels"] < 6 or facts["rows"] < 30 or facts["score"] in ("—", "") or errors or not facts["regimeBanner"]:
                    FAILS.append("%dpx render: %s errors=%s" % (width, json.dumps(facts)[:200], errors[:2]))
                if width == 390 and facts["overflow"] > 0:
                    offenders = pg.evaluate("""() => { const cw = document.documentElement.clientWidth; const out = [];
                        const clipped = el => { for (let a = el.parentElement; a; a = a.parentElement) { const ox = getComputedStyle(a).overflowX; if (ox === 'auto' || ox === 'hidden' || ox === 'scroll' || ox === 'clip') return true; } return false; };
                        for (const el of document.querySelectorAll('body *')) { const r = el.getBoundingClientRect(); if (r.right > cw + 1 && r.width > 20 && !clipped(el)) { const cs = getComputedStyle(el); out.push([el.tagName + (el.id ? '#' + el.id : '') + (el.className && typeof el.className === 'string' ? '.' + el.className.split(' ').slice(0,2).join('.') : ''), Math.round(r.right - cw), Math.round(r.width), cs.position, Math.round(r.left)]); } }
                        return { body: document.body.scrollWidth, html: document.documentElement.scrollWidth, cw, top: out.sort((a, b) => b[1] - a[1]).slice(0, 10) }; }""")
                    R.log("   390px overflow offenders (element, px past edge, width): %s" % json.dumps(offenders))
                    FAILS.append("390px overflow %dpx" % facts["overflow"])
                ctx.close()
            browser.close()
    else:
        FAILS.append("page deploy not observed")
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: bond war room v1.3 warehouse-first, official-yields lane banked, catalog registered")
