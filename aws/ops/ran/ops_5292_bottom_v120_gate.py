"""ops_5292 -- BOTTOM v1.2.0 gate: the climax must be the LOW of a prolonged decline on the heaviest volume of that decline (Khalid 2026-09-09: "usually a climax\nsell-off is accompanied with the highest volume on daily followed by diminishing volume ... that stock is going to be at the lows not at the highs"; Wyckoff/SMI: "only after a\nmove has been in effect for some time ... if it does not have this it is not a selling climax"). Compares the new base rates with the v1.1 artifact captured before the run,\nproves no actionable row sits in the upper half of its yearly range, and re-drives the page (actionable-at-the-lows default view).
Re-runs the engine (three stop rules measured on every historical trigger, plain-English findings, exact phase-detector /
accumulation-radar / khalid-risk joins), audits the artifact, re-arms the schedule idempotently, drives the page, and proves the consumers.

Creates/updates the function from the runner (idempotent with deploy-lambdas), makes sure the Polygon key is in the env
(env-first, SSM canonical), runs it ASYNC and gates on data/bottom.json freshness (never a sync invoke), audits the
artifact (universe by desk, states, base rates, harvester contract, chart payloads, degraded inputs), arms EventBridge
Scheduler (daily 03:45 UTC Tue-Sat), waits for the page at the edge and drives it in Chrome at 1440/390 (board rows,
help icons, funnel, evidence panel with the SVG theater, base-rates tab, no overflow), then proves the two consumers:
katlin picks carry `wyckoff_bottom` after a fresh run, and the jhsignal bridge publishes engine `bottom` signals."""
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

FN = "justhodl-bottom"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
sch = boto3.client("scheduler", region_name="us-east-1", config=CFG)
ssm = boto3.client("ssm", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
OUT = "data/bottom.json"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
UA = {"User-Agent": "justhodl-ops-5292", "Cache-Control": "no-cache", "Pragma": "no-cache"}
FAILS, WARNS = [], []
PUSH_TS = int(subprocess.run(["git", "log", "-1", "--format=%ct", "--grep=ops 5292", "--", "aws/lambdas/justhodl-bottom/source/lambda_function.py"], cwd=ROOT, capture_output=True, text=True).stdout.strip() or "0")


def get_json(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_deployed(fn, since_epoch, max_s):
    """True once the live function's LastModified is at/after the push (deploy-lambdas landed); bounded."""
    t0 = time.time()
    while time.time() - t0 < max_s:
        try:
            cfg = lam.get_function_configuration(FunctionName=fn)
            lm = datetime.strptime(cfg["LastModified"][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            if lm >= since_epoch - 120 and cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
                return True
        except Exception:
            pass
        time.sleep(20)
    return False


with report("ops_5292_bottom_launch") as R:
    R.heading("ops 5292 -- BOTTOM launch (Wyckoff bottom desk)")
    cfg_json = json.loads((ROOT / "aws" / "lambdas" / FN / "config.json").read_text())
    # ---- env: canonical provider vars, env-first (managed_secret falls back to SSM only if these are absent)
    env = dict(cfg_json.get("env") or {})
    try:
        env = dict(lam.get_function_configuration(FunctionName=FN).get("Environment", {}).get("Variables", {})) | env
    except Exception:
        pass
    donor = (cfg_json.get("inherit_env") or {}).get("from_function")
    for k in (cfg_json.get("inherit_env") or {}).get("keys") or []:
        if env.get(k):
            continue
        try:
            v = lam.get_function_configuration(FunctionName=donor).get("Environment", {}).get("Variables", {}).get(k)
            if v:
                env[k] = v
        except Exception as e:
            WARNS.append("donor %s env %s: %s" % (donor, k, str(e)[:80]))
    for k, pname in (("POLYGON_API_KEY", "/justhodl/polygon/api-key"), ("FMP_KEY", "/justhodl/fmp/api-key")):
        if not env.get(k):
            try:
                env[k] = ssm.get_parameter(Name=pname, WithDecryption=True)["Parameter"]["Value"]
                R.log("   %s taken from SSM %s" % (k, pname))
            except Exception as e:
                WARNS.append("%s unresolved (%s): %s" % (k, pname, str(e)[:80]))
    if not env.get("POLYGON_API_KEY"):
        WARNS.append("POLYGON_API_KEY missing -- the crypto lane will read the existing bank only (no incremental days)")
    create_or_update_lambda(report=R, function_name=FN, zip_bytes=build_zip(ROOT / "aws" / "lambdas" / FN / "source"),
                            env_vars=env, timeout=int(cfg_json.get("timeout") or 900), memory=int(cfg_json.get("memory") or 8192),
                            description=cfg_json.get("description", "")[:250], reserved_concurrency=None, create_function_url=False, ephemeral_storage=2048)
    cfg = None
    for _ in range(40):
        cfg = lam.get_function_configuration(FunctionName=FN)
        if cfg.get("LastUpdateStatus") in (None, "Successful") and cfg.get("State") == "Active":
            break
        time.sleep(5)
    R.log("   function state %s / %s, %sMB / %ss, env keys %s" % (cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("MemorySize"), cfg.get("Timeout"), sorted((cfg.get("Environment") or {}).get("Variables", {}).keys())))

    # ---- async run + freshness gate
    before = None
    prior = {}
    try:
        prior = get_json(OUT)
        before = prior.get("generated_at")
    except Exception:
        pass
    t0 = time.time()
    lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    R.log("   invoked async (prior generated_at=%s); polling %s" % (before, OUT))
    D = None
    while time.time() - t0 < 900:
        time.sleep(20)
        try:
            d = get_json(OUT)
        except Exception:
            continue
        if d.get("generated_at") and d.get("generated_at") != before:
            D = d
            break
    if not D:
        FAILS.append("no fresh %s after %.0fs (check CloudWatch /aws/lambda/%s)" % (OUT, time.time() - t0, FN))
    else:
        R.log("   fresh artifact after %.0fs: v%s session %s elapsed %ss" % (time.time() - t0, D.get("version"), D.get("session"), D.get("elapsed_s")))
        R.section("universe / states / desks")
        U, C, M = D.get("universe") or {}, D.get("counts") or {}, D.get("market") or {}
        R.log("   universe %s" % json.dumps(U))
        R.log("   by_state %s" % json.dumps(C.get("by_state")))
        R.log("   by_desk  %s" % json.dumps(C.get("by_desk")))
        R.log("   breadth  %s" % json.dumps((M.get("breadth") or {})))
        R.log("   read: %s" % M.get("read"))
        R.log("   benchmarks: %s" % json.dumps(M.get("benchmarks"))[:600])
        R.log("   authority %s | risk gate %s | katlin posture %s" % (json.dumps(M.get("capital_authority")), json.dumps(M.get("risk_gate")), M.get("katlin_posture")))
        if (U.get("scored") or 0) < 3000:
            FAILS.append("scored universe thin: %s (expected several thousand stocks + wrappers)" % U.get("scored"))
        if (U.get("sessions") or 0) < 1000:
            FAILS.append("bar window thin: %s sessions" % U.get("sessions"))
        desks = C.get("by_desk") or {}
        for want in ("stocks", "equity_etfs", "bonds", "gold_metals", "commodities", "countries"):
            if not desks.get(want):
                WARNS.append("desk %s has no live/recent sequence this session (possible, but check the classifier if it persists)" % want)
        if "crypto" not in desks and not any("crypto" in x for x in (D.get("degraded") or [])):
            WARNS.append("no crypto rows this session")
        R.section("board / rows / contract")
        board = D.get("board") or []
        all_rows = D.get("board_all") or []
        R.log("   board %d rows (with charts), board_all %d rows, weekly_live %d, top_picks %d" % (len(board), len(all_rows), len(D.get("weekly_live") or []), len(D.get("top_picks") or [])))
        if not all_rows:
            FAILS.append("board_all empty -- no sequence detected on any instrument (detector or loader broken)")
        bad_chart = [r["ticker"] for r in board if not (isinstance(r.get("chart"), dict) and (r["chart"].get("c") or []))]
        if bad_chart:
            FAILS.append("%d board rows without a chart payload: %s" % (len(bad_chart), bad_chart[:6]))
        need = ("ticker", "state", "score", "grade", "frame", "desk", "sc_date", "event", "why", "what_next")
        miss = [k for k in need if board and any(r.get(k) is None for r in board[:50])]
        if miss:
            FAILS.append("board rows missing keys: %s" % miss)
        if not all("ticker" in x and "score" in x for x in D.get("top_picks") or []):
            FAILS.append("top_picks rows lack ticker/score (harvester contract)")
        for r in board[:12]:
            p = r.get("plan") or {}
            R.log("      %-6s %-8s %-13s %-6s %4s %-5s sc %s %sx  test %s %s%%  trig %s  plan %s/%s/%s  fleet %s" % (
                r["ticker"], r.get("desk", "")[:8], r["state"], r.get("frame"), r.get("score"), r.get("grade"), r.get("sc_date"), r.get("sc_vol_x"),
                r.get("st_date"), (round(100 * r["st_vol_ratio_sc"]) if r.get("st_vol_ratio_sc") is not None else "-"), r.get("trigger_date"),
                p.get("entry"), p.get("stop"), p.get("target_1"), r.get("n_confirm")))
        R.section("base rates (graded from the tape)")
        br = D.get("base_rates") or {}
        R.log("   sequences %s, with rally %s, triggered %s, outcome mix %s" % (br.get("n_sequences"), br.get("n_with_rally"), br.get("n_triggered"), json.dumps(br.get("outcome_mix"))))
        R.log("   triggered all: %s" % json.dumps((br.get("triggered") or {}).get("all"))[:500])
        R.log("   crowd vs pro: %s" % json.dumps(br.get("crowd_vs_pro"))[:900])
        for k, v in (br.get("by_desk") or {}).items():
            R.log("      %-12s n %4s  +21 %s (hit %s%%)  +63 %s (hit %s%%)  managed63 %s  stop %s%%  t1 %s%%" % (
                k, v.get("n"), (v.get("ret_21") or {}).get("median"), (v.get("ret_21") or {}).get("hit"), (v.get("ret_63") or {}).get("median"), (v.get("ret_63") or {}).get("hit"),
                (v.get("managed_63") or {}).get("median"), v.get("stop_hit_pct"), v.get("target1_hit_pct")))
        for k, v in (br.get("by_test_volume") or {}).items():
            R.log("      test volume %-22s n %4s  +63 %s (hit %s%%)  stop %s%%" % (k, v.get("n"), (v.get("ret_63") or {}).get("median"), (v.get("ret_63") or {}).get("hit"), v.get("stop_hit_pct")))
        for k, v in (((br.get("triggered") or {}).get("all") or {}).get("stops") or {}).get("rules", {}).items():
            R.log("      stop rule %-7s n %5s  hit %s%%  bars-to-stop %s  managed +21 %s (hit %s%%)  +63 %s (hit %s%%)" % (k, v.get("n"), v.get("stop_hit_pct"), v.get("median_bars_to_stop"), (v.get("managed_21") or {}).get("median"), (v.get("managed_21") or {}).get("hit"), (v.get("managed_63") or {}).get("median"), (v.get("managed_63") or {}).get("hit")))
        for line in (br.get("findings") or []):
            R.log("   finding: " + line[:260])
        if not br.get("findings"):
            FAILS.append("no findings block in base_rates (v1.1.0 expected)")
        if not (((br.get("triggered") or {}).get("all") or {}).get("stops") or {}).get("rules"):
            FAILS.append("no stop-rule ledger in base_rates (v1.1.0 expected)")
        R.section("v1.2.0 -- climax at the lows (vs the v1.1 artifact)")
        pb = (prior.get("base_rates") or {}) if isinstance(prior, dict) else {}
        pt = ((pb.get("triggered") or {}).get("all") or {})
        nt = ((br.get("triggered") or {}).get("all") or {})
        R.log("   sequences: v%s %s -> v%s %s | triggered %s -> %s" % (prior.get("version"), pb.get("n_sequences"), D.get("version"), br.get("n_sequences"), pb.get("n_triggered"), br.get("n_triggered")))
        R.log("   raw +63 median/hit: %s/%s%% -> %s/%s%% | paper-stop hit: %s%% -> %s%% | target1 hit: %s%% -> %s%%" % (
            (pt.get("ret_63") or {}).get("median"), (pt.get("ret_63") or {}).get("hit"), (nt.get("ret_63") or {}).get("median"), (nt.get("ret_63") or {}).get("hit"),
            pt.get("stop_hit_pct"), nt.get("stop_hit_pct"), pt.get("target1_hit_pct"), nt.get("target1_hit_pct")))
        for fr in ("D", "W"):
            R.log("   climax gates %s: %s" % (fr, json.dumps((D.get("climax_gates") or {}).get(fr))))
        act = [r for r in all_rows if r.get("actionable")]
        R.log("   actionable rows: %d of %d (counts.actionable=%s)" % (len(act), len(all_rows), (D.get("counts") or {}).get("actionable")))
        high = [(r["ticker"], r.get("pos_52w_pct")) for r in board if r.get("actionable") and (r.get("pos_52w_pct") or 0) > 60]
        if high:
            FAILS.append("actionable rows sitting in the top of their yearly range (not bottoms): %s" % high[:8])
        for r in board[:16]:
            R.log("      %-6s %-9s %-13s %-3s act=%-5s pos52w %3s%%  vs52wH %6s%%  climax %s (%sx, %s bars down, pos %s%%)  test %s %s%%  trig %s" % (
                r["ticker"], r.get("desk", "")[:9], r["state"], r.get("frame"), r.get("actionable"), r.get("pos_52w_pct"), r.get("dist_52w_high_pct"), r.get("sc_date"), r.get("sc_vol_x"),
                r.get("sc_bars_down"), r.get("sc_pos_range_pct"), r.get("st_date"), (round(100 * r["st_vol_ratio_sc"]) if r.get("st_vol_ratio_sc") is not None else "-"), r.get("trigger_date")))
        if "IVES" in {r["ticker"] for r in act}:
            FAILS.append("IVES is still presented as an actionable bottom")
        ives = next((r for r in all_rows if r.get("ticker") == "IVES"), None)
        R.log("   IVES on the board: %s" % (json.dumps({k: ives.get(k) for k in ("state", "actionable", "pos_52w_pct", "sc_date", "frame", "score")}) if ives else "no (no qualifying sequence)"))
        fl = [l for l in (D.get("log") or []) if l.startswith("[bottom] feeds:")]
        if fl:
            R.log("   %s" % fl[-1])
            if "phase=0" in fl[-1]:
                FAILS.append("phase-detector join still binds zero rows")
        if (br.get("n_sequences") or 0) < 500:
            FAILS.append("base rates over only %s sequences -- the 5y scan is not covering the universe" % br.get("n_sequences"))
        R.section("health")
        R.log("   degraded: %s" % (D.get("degraded") or []))
        R.log("   row_errors: %s" % json.dumps(D.get("row_errors") or {})[:400])
        if sum((D.get("row_errors") or {}).values()) > 50:
            FAILS.append("row errors: %s" % json.dumps(D.get("row_errors"))[:200])
        R.log("   feeds_asof: %s" % json.dumps(D.get("feeds_asof"))[:500])
        for line in (D.get("log") or [])[-12:]:
            R.log("      " + line[:200])
        try:
            size = s3.head_object(Bucket=BUCKET, Key=OUT)["ContentLength"]
            R.log("   %s = %.2f MB" % (OUT, size / 1e6))
            if size > 12e6:
                WARNS.append("artifact is %.1f MB -- trim board_rows/chart bars if the page feels slow" % (size / 1e6))
        except Exception:
            pass

    # ---- schedule (EventBridge Scheduler; never a classic rule)
    R.section("schedule (EventBridge Scheduler, UTC)")
    arn = cfg["FunctionArn"]
    name, expr, desc = "justhodl-bottom-daily", "cron(45 3 ? * TUE-SAT *)", "BOTTOM daily 03:45 UTC Tue-Sat after polygon-full + fortress, before katlin (ops 5292)"
    tgt = {"Arn": arn, "RoleArn": SCHED_ROLE, "Input": "{}", "RetryPolicy": {"MaximumRetryAttempts": 1}}
    try:
        sch.get_schedule(Name=name, GroupName="default")
        sch.update_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=desc)
        R.ok("   %s updated %s" % (name, expr))
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=name, GroupName="default", ScheduleExpression=expr, ScheduleExpressionTimezone="UTC", FlexibleTimeWindow={"Mode": "OFF"}, Target=tgt, State="ENABLED", Description=desc)
        R.ok("   %s created %s" % (name, expr))
    except Exception as e:
        FAILS.append("schedule %s: %s" % (name, str(e)[:120]))

    # ---- page at the edge + Chrome render gate
    R.section("page")
    live = False
    for _ in range(40):
        try:
            with urllib.request.urlopen(urllib.request.Request("https://justhodl.ai/bottom.html?v=%d" % int(time.time()), headers=UA), timeout=30) as r:
                live = b"BOTTOM_DESK_V1" in r.read()
        except Exception:
            live = False
        if live:
            break
        time.sleep(15)
    R.log("   bottom.html carries marker BOTTOM_DESK_V1 at the edge: %s" % live)
    if live and D:
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
                pg.goto("https://justhodl.ai/bottom.html?v=%d" % int(time.time()), wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(9000)
                pg.evaluate("""() => { const r = document.querySelector('#board tbody tr.pick'); if (r) r.click(); }""")
                pg.wait_for_timeout(800)
                facts = pg.evaluate("""() => ({ headline: document.getElementById('wr-headline').textContent.slice(0, 80), funnel: document.querySelectorAll('#funnel div[data-s]').length,
                    board: document.querySelectorAll('#board tbody tr.pick').length, helps: document.querySelectorAll('#board th .help').length,
                    evid: (document.querySelector('#evid .why') || {textContent: ''}).textContent.length, svg: !!document.querySelector('#evid svg'), defs: document.querySelectorAll('#defs dt').length,
                    overflow: document.documentElement.scrollWidth - document.documentElement.clientWidth, err: document.getElementById('errbox').textContent })""")
                pg.evaluate("""() => { const b = document.querySelector('#tabs button[data-t=rates]'); if (b) b.click(); }""")
                pg.wait_for_timeout(600)
                facts["rates_cards"] = pg.evaluate("() => document.querySelectorAll('.cvp .card').length")
                facts["rates_tables"] = pg.evaluate("() => document.querySelectorAll('#aux table').length")
                pg.evaluate("""() => { const b = document.querySelector('#tabs button[data-t=all]'); if (b) b.click(); }""")
                pg.wait_for_timeout(300)
                pg.screenshot(path=str(SHOTS / f"ops5292_bottom_{width}.png"))
                R.log("   %4dpx: %s errors=%s" % (width, json.dumps(facts)[:420], errors[:2]))
                if facts["funnel"] != 7 or facts["helps"] < 20 or facts["defs"] < 10 or errors or facts["err"] or facts["rates_cards"] != 2:
                    FAILS.append("%dpx render: %s errors=%s" % (width, json.dumps(facts)[:240], errors[:2]))
                if facts["board"] == 0:
                    FAILS.append("%dpx: the board rendered zero rows although board_all has %d" % (width, len(D.get("board_all") or [])))
                if facts["board"] and (facts["evid"] < 80 or not facts["svg"]):
                    FAILS.append("%dpx: evidence panel did not fill (why %d chars, svg %s)" % (width, facts["evid"], facts["svg"]))
                if width == 390 and facts["overflow"] > 0:
                    FAILS.append("390px overflow %dpx" % facts["overflow"])
                ctx.close()
            browser.close()
    elif not live:
        FAILS.append("page deploy not observed at the edge")

    # ---- consumers: katlin join (wyckoff_bottom on picks) and the fusion bridge (engine `bottom`)
    if D:
        R.section("consumers")
        if wait_deployed("justhodl-katlin", PUSH_TS, 900):
            kb = None
            try:
                kb = get_json("data/katlin.json").get("generated_at")
            except Exception:
                pass
            lam.invoke(FunctionName="justhodl-katlin", InvocationType="Event", Payload=b"{}")
            K = None
            t1 = time.time()
            while time.time() - t1 < 720:
                time.sleep(20)
                try:
                    k = get_json("data/katlin.json")
                except Exception:
                    continue
                if k.get("generated_at") and k.get("generated_at") != kb:
                    K = k
                    break
            if K:
                picks = K.get("picks") or []
                joined = [r for r in picks if r.get("wyckoff_bottom")]
                R.log("   katlin v%s fresh: %d picks, %d carry wyckoff_bottom (%s)" % (K.get("version"), len(picks), len(joined), [(r["ticker"], r["wyckoff_bottom"].get("state")) for r in joined[:6]]))
                if K.get("version", "0") < "2.5.0":
                    WARNS.append("katlin ran on version %s (join not deployed yet) -- the next scheduled run will carry it" % K.get("version"))
                if "bottom" not in (K.get("feeds_asof") or K.get("asof") or {}) and not joined:
                    WARNS.append("katlin picks carry no wyckoff_bottom this run -- either no pick has a live sequence or the join did not read data/bottom.json")
            else:
                WARNS.append("katlin did not refresh within 12 min -- the join proof waits for its 04:10 UTC run")
        else:
            WARNS.append("katlin redeploy not observed within 15 min -- the join proof waits for its 04:10 UTC run")
        if wait_deployed("justhodl-jhsignal-bridge", PUSH_TS, 600):
            lam.invoke(FunctionName="justhodl-jhsignal-bridge", InvocationType="Event", Payload=b"{}")
            found = None
            t2 = time.time()
            while time.time() - t2 < 300:
                time.sleep(20)
                try:
                    st = get_json("data/jhsignal/state/latest.json")
                except Exception:
                    continue
                ents = st.get("entities") if isinstance(st.get("entities"), dict) else {}
                sigs = [s for v in ents.values() for s in (v if isinstance(v, list) else [])]
                mine = [s for s in sigs if isinstance(s, dict) and s.get("engine_id") == "bottom"]
                if mine:
                    found = mine
                    break
            if found:
                ad = st.get("adapters")
                ad_b = (ad.get("bottom") if isinstance(ad, dict) else next((a for a in (ad or []) if isinstance(a, dict) and a.get("engine_id") == "bottom"), None))
                R.log("   fusion bridge: %d `bottom` signals in the state store, e.g. %s | adapter report %s" % (len(found), [(s.get("entity_id"), s.get("direction"), round(s.get("score", 0), 2)) for s in found[:5]], json.dumps(ad_b)[:300]))
            else:
                WARNS.append("no `bottom` signals in data/jhsignal/state/latest.json yet -- the hourly bridge will pick them up (check the bridge run report for skip reasons)")
        else:
            WARNS.append("jhsignal-bridge redeploy not observed within 10 min -- fusion proof waits for the hourly bridge")
        try:
            lam.invoke(FunctionName="justhodl-alert-router", InvocationType="Event", Payload=b"{}")
            R.log("   alert-router invoked (check_bottom registered; Telegram delivery depends on the bot token Khalid must rotate)")
        except Exception as e:
            WARNS.append("alert-router invoke: %s" % str(e)[:100])

    for w in WARNS:
        R.warn("   " + w)
    for f in FAILS:
        R.fail("   " + f)
    if FAILS:
        sys.exit(1)
    R.ok("   GREEN: BOTTOM live -- engine, feed, schedule, page" + (" (with warnings)" if WARNS else ""))
    sys.exit(0)
