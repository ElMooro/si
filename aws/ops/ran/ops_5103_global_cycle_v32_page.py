"""ops_5103 -- global cycle v3.0.2: freshness-first picker (features 1.1.1), equity-pillar diagnostics, page v3 render gate.
warehouses, then run justhodl-global-business-cycle v3.0.0 and verify.

  S1  deploy justhodl-cycle-features (create-or-update; role/lambda-execution-role,
      2048MB / 600s, S3_BUCKET) and settle Active
  S2  EventBridge Scheduler justhodl-cycle-features-daily cron(30 10 * * ? *)
      -- 90 minutes before the GBC engine's 12:00 UTC run
  S3  first real run (async) -> poll data/cycle/features-manifest.json;
      report coverage per feature and per country, source status, errors
      gate: >= 30 countries with features, cli_oecd >= 12, bci >= 25,
      ip_yoy >= 25, credit_impulse >= 25
  S4  wait for deploy-lambdas.yml to land GBC v3.0.0 (Description), invoke,
      poll S3 for engine_version 3.0.0
  S5  verify: composite.available, countries_multi_pillar >= 25, pillar
      counts, downturn_probability_6m ok, composite-history doc written
      (34 countries, global series), v2 keys intact for consumers
      (phase/cli_level/gdp_weight/three_month_change/latest_date), per-country
      table with pillar z's; CloudWatch tail of [gbc-v3]
Gate (sys.exit(1)) on any of S3/S5.
"""
import json
import sys
import time
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
FN_FEAT = "justhodl-cycle-features"
FN_GBC = "justhodl-global-business-cycle"
SRC_FEAT = ROOT / "aws" / "lambdas" / FN_FEAT / "source"
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
MANIFEST_KEY = "data/cycle/features-manifest.json"
LIVE_KEY = "data/global-business-cycle.json"
COMP_KEY = "data/global-business-cycle-composite-history.json"
WANT = "3.0.2"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=60, retries={"max_attempts": 1}))
sch = boto3.client("scheduler", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def get_json(key):
    try:
        o = s3.get_object(Bucket=B, Key=key)
        return json.loads(o["Body"].read()), o["LastModified"]
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:120]


def main():
    with report("5103-global-cycle-v32-page") as r:
        r.heading("ops 5103 -- global cycle v3.0.2 + page v3 (pillars, downturn card) render gate")
        t_start = datetime.now(timezone.utc)
        r.log(f"started {t_start.isoformat(timespec='seconds')}")
        fails = []

        r.section("S1 deploy justhodl-cycle-features")
        deploy_lambda(report=r, function_name=FN_FEAT, source_dir=SRC_FEAT, env_vars={"S3_BUCKET": B},
                      timeout=600, memory=3008, create_function_url=False, smoke=False,
                      description=("Cycle feature store v1.1.1: per-country monthly business-cycle features from OECD "
                                   "(live CLI + KEI/labour warehouse), BIS (credit/property/REER/gap/DSR), Eurostat "
                                   "(surveys/unemployment), fleet sovereign desk -> data/cycle/features.json.gz. Daily 10:30 UTC."))
        cfg = {}
        for _ in range(40):
            cfg = lam.get_function_configuration(FunctionName=FN_FEAT)
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(3)
        r.kv(step="S1", state=cfg.get("State"), last_update=cfg.get("LastUpdateStatus"), memory=cfg.get("MemorySize"), timeout=cfg.get("Timeout"))
        if cfg.get("State") != "Active":
            r.fail("feature-store function not Active")
            sys.exit(1)

        r.section("S2 schedule")
        sched = {"Name": FN_FEAT + "-daily", "ScheduleExpression": "cron(30 10 * * ? *)", "ScheduleExpressionTimezone": "UTC",
                 "FlexibleTimeWindow": {"Mode": "OFF"},
                 "Target": {"Arn": cfg["FunctionArn"], "RoleArn": SCHED_ROLE, "Input": "{}",
                            "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}},
                 "State": "ENABLED", "Description": "Cycle feature store daily 10:30 UTC (before global-business-cycle at 12:00 UTC)"}
        try:
            sch.create_schedule(**sched)
            r.ok("schedule created")
        except sch.exceptions.ConflictException:
            sch.update_schedule(**sched)
            r.ok("schedule updated")
        got = sch.get_schedule(Name=FN_FEAT + "-daily")
        r.kv(step="S2", expr=got.get("ScheduleExpression"), state=got.get("State"))

        r.section("S3 first real run")
        t_inv = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN_FEAT, InvocationType="Event", Payload=b"{}")
        man = None
        t0 = time.time()
        while time.time() - t0 < 660:
            time.sleep(20)
            doc, lm = get_json(MANIFEST_KEY)
            if doc and (doc.get("generated_at") or "") > t_inv.isoformat():
                man = doc
                break
            r.log(f"  polling manifest… {doc.get('generated_at') if doc else lm}")
        if not man:
            fails.append("feature-store manifest did not land within 11 min")
            r.fail(fails[-1])
        else:
            cnt = man.get("feature_count_by_name") or {}
            r.ok(f"manifest generated_at={man.get('generated_at')} elapsed={man.get('elapsed_s')}s n_countries={man.get('n_countries')}")
            r.log(f"  feature counts: {json.dumps(cnt)}")
            for k, v in (man.get("sources") or {}).items():
                r.log(f"  source {k}: {json.dumps(v)[:300]}")
                r.kv(source=k, ok=v.get("ok"), countries=v.get("countries"), latest=v.get("latest"), error=(v.get("error") or "")[:80])
            cov = man.get("coverage") or {}
            for iso, c in sorted(cov.items()):
                r.log(f"  {iso}: {c.get('n_features')} features · pillars {c.get('pillars')} · fresh {c.get('fresh_features')} · "
                      + ", ".join(f"{k}@{v[0]}({v[1]}mo)" for k, v in (c.get("features") or {}).items()))
            if (man.get("n_countries") or 0) < 30:
                fails.append(f"only {man.get('n_countries')} countries carry features")
            for k, need in (("cli_oecd", 12), ("bci", 25), ("ip_yoy", 25), ("credit_impulse", 25), ("exports_yoy", 25), ("curve", 25), ("cci", 25), ("reer_12m", 25)):
                if cnt.get(k, 0) < need:
                    fails.append(f"feature {k}: {cnt.get(k, 0)} countries < {need}")
            # CloudWatch tail
            try:
                lg = f"/aws/lambda/{FN_FEAT}"
                st = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=1)
                for s in st.get("logStreams") or []:
                    ev = logs.get_log_events(logGroupName=lg, logStreamName=s["logStreamName"], startTime=int(t_inv.timestamp() * 1000) - 30000, limit=200)
                    for e in ev.get("events") or []:
                        m = e["message"].rstrip()
                        if "[cycle-features]" in m or "Traceback" in m or "Error" in m or "REPORT" in m:
                            r.log("  log: " + m[:220])
            except Exception as e:  # noqa: BLE001
                r.warn(f"log tail failed: {str(e)[:100]}")

        r.section("S4 GBC v3.0.0 deploy wait + run")
        t0 = time.time()
        deployed = False
        while time.time() - t0 < 900:
            c = lam.get_function_configuration(FunctionName=FN_GBC)
            if "v3.0.2" in (c.get("Description") or "") and c.get("LastUpdateStatus") == "Successful":
                deployed = True
                r.ok(f"GBC v3.0.2 deployed {c.get('LastModified')} after {time.time() - t0:.0f}s")
                break
            r.log(f"  waiting for deploy-lambdas.yml… has_v3={'v3.0.2' in (c.get('Description') or '')} status={c.get('LastUpdateStatus')}")
            time.sleep(25)
        if not deployed:
            fails.append("GBC v3.0.2 not deployed within 15 min")
        t_inv2 = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN_GBC, InvocationType="Event", Payload=b"{}")
        after = None
        t0 = time.time()
        while time.time() - t0 < 900:
            time.sleep(20)
            doc, lm = get_json(LIVE_KEY)
            if doc and doc.get("engine_version") == WANT and (doc.get("generated_at") or "") > t_inv2.isoformat():
                after = doc
                r.ok(f"fresh v{WANT} feed generated_at={doc.get('generated_at')} elapsed={doc.get('elapsed_sec')}s")
                break
            r.log(f"  polling… engine_version={doc.get('engine_version') if doc else lm}")
        if after is None:
            fails.append("no v3.0.2 feed landed within 15 min")
            after = get_json(LIVE_KEY)[0] or {}

        r.section("S5 verify v3 output")
        comp = after.get("composite") or {}
        cal = after.get("downturn_probability_6m") or {}
        agg = after.get("aggregate") or {}
        r.log(f"composite: {json.dumps(comp)[:600]}")
        r.log(f"downturn_probability_6m: {json.dumps(cal)[:700]}")
        r.log(f"global_composite_latest: {json.dumps(after.get('global_composite_latest'))}")
        r.log(f"aggregate: phase={agg.get('global_phase')} avg_cli={agg.get('global_avg_cli')} mix={json.dumps(agg.get('global_phase_mix_pct'))} "
              f"coverage={agg.get('classification_coverage_pct')}")
        bc = after.get("by_country") or {}
        n_multi = 0
        for iso in sorted(bc):
            c = bc[iso]
            nc = c.get("composite") or {}
            pil = nc.get("pillars") or {}
            r.log(f"  {iso} {str(c.get('phase')):<10} cli={c.get('cli_level')} basis={c.get('phase_basis')} eq_cli={c.get('equity_cli_level')} "
                  f"eq_phase={c.get('equity_phase')} conf={nc.get('confidence')} pillars=" +
                  " ".join(f"{p}:{v.get('z')}/{v.get('n')}" for p, v in pil.items()))
            r.kv(iso=iso, phase=c.get("phase"), cli=c.get("cli_level"), basis=c.get("phase_basis"), eq_cli=c.get("equity_cli_level"),
                 conf=nc.get("confidence"), n_feat=nc.get("n_features"), survey=(pil.get("survey") or {}).get("z"),
                 financial=(pil.get("financial") or {}).get("z"), activity=(pil.get("activity") or {}).get("z"),
                 trade=(pil.get("trade") or {}).get("z"), equity=(pil.get("equity") or {}).get("z"))
            if c.get("phase_basis") == "multi-pillar":
                n_multi += 1
            for k in ("phase", "cli_level", "gdp_weight", "three_month_change", "yoy_change", "latest_date", "source", "physical"):
                if k not in c:
                    fails.append(f"{iso}: consumer key {k} missing")
                    break
        r.log(f"multi-pillar countries: {n_multi}/{len(bc)}")
        trade_n = (comp.get("pillar_counts") or {}).get("trade", 0)
        r.log(f"trade pillar countries: {trade_n}")
        if trade_n < 20:
            fails.append(f"trade pillar only {trade_n} countries")
        gl = after.get("global_composite_latest") or {}
        if (gl.get("n") or 0) < 28:
            fails.append(f"global composite latest month covers only {gl.get('n')} countries (history not carried forward?)")
        if not comp.get("available"):
            fails.append(f"composite not available: {comp.get('reason')}")
        if n_multi < 25:
            fails.append(f"only {n_multi} countries multi-pillar")
        if not cal.get("ok"):
            r.warn(f"downturn calibration not ok: {cal.get('reason')}")
        ch, lm = get_json(COMP_KEY)
        if ch:
            g = ch.get("global") or []
            r.ok(f"composite history: {len(ch.get('by_country') or {})} countries, global {len(g)} points "
                 f"{g[0]['period'] if g else None}..{g[-1]['period'] if g else None}, size via S3 LastModified {lm}")
            usa = (ch.get("by_country") or {}).get("USA") or {}
            hist = usa.get("history") or []
            r.log(f"  USA history: {len(hist)} points; last 6: {json.dumps([{k: h[k] for k in ('period', 'cli', 'phase', 'pillars')} for h in hist[-6:]])[:900]}")
            if (ch.get("generated_at") or "") < t_inv2.isoformat():
                fails.append("composite history not regenerated by this run")
        else:
            fails.append(f"composite history doc missing: {lm}")
        try:
            lg = f"/aws/lambda/{FN_GBC}"
            st = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2)
            for s in st.get("logStreams") or []:
                ev = logs.get_log_events(logGroupName=lg, logStreamName=s["logStreamName"], startTime=int(t_inv2.timestamp() * 1000) - 30000, limit=300)
                for e in ev.get("events") or []:
                    m = e["message"].rstrip()
                    if "[gbc-v3]" in m or "Traceback" in m or "[ERROR]" in m or "REPORT" in m or "feature store" in m:
                        r.log("  log: " + m[:220])
                    if "Traceback" in m:
                        fails.append("traceback in the GBC run log")
        except Exception as e:  # noqa: BLE001
            r.warn(f"log tail failed: {str(e)[:100]}")

        r.section("S6 equity pillar diagnostics from the run log")
        try:
            lg = f"/aws/lambda/{FN_GBC}"
            st = logs.describe_log_streams(logGroupName=lg, orderBy="LastEventTime", descending=True, limit=2)
            eq_lines = []
            for s in st.get("logStreams") or []:
                ev = logs.get_log_events(logGroupName=lg, logStreamName=s["logStreamName"], startTime=int(t_inv2.timestamp() * 1000) - 30000, limit=400)
                for e in ev.get("events") or []:
                    m = e["message"].rstrip()
                    if "[gbc-v3]" in m and "eq_bars=" in m:
                        eq_lines.append(m[m.find("[gbc-v3]"):][:260])
            for m in eq_lines[:40]:
                r.log("  " + m)
            no_eq = [m.split()[1] for m in eq_lines if "eq_z=None" in m]
            r.log(f"countries without an equity z: {len(no_eq)} {no_eq}")
        except Exception as e:  # noqa: BLE001
            r.warn(f"log tail failed: {str(e)[:100]}")
        dg = ((bc.get("DEU") or {}).get("components") or [])
        dip = next((x for x in dg if x.get("name") == "ip_yoy"), None)
        r.log(f"DEU ip_yoy component: {json.dumps(dip)[:300]}")
        if dip and (dip.get("months_stale") or 99) > 4:
            fails.append(f"DEU ip_yoy still stale ({dip.get('months_stale')} months) -- picker not freshness-first")

        r.section("S7 page v3 render gate")
        import subprocess
        import urllib.request

        def http(url, method="GET", timeout=25):
            req = urllib.request.Request(url, method=method, headers={"User-Agent": "ops5103", "Cache-Control": "no-cache"})
            try:
                with urllib.request.urlopen(req, timeout=timeout) as rr:
                    return rr.status, rr.read()
            except Exception as e:  # noqa: BLE001
                return None, str(e).encode()[:100]
        t0 = time.time()
        page_ok = False
        while time.time() - t0 < 900:
            stt, body = http("https://justhodl.ai/global-cycle.html?nocache=" + str(int(time.time())))
            if stt == 200 and b"pillarsSection" in body:
                page_ok = True
                r.ok(f"Pages deploy live after {time.time() - t0:.0f}s")
                break
            time.sleep(20)
        if not page_ok:
            fails.append("Pages deploy with pillarsSection not observed within 15 min")
        else:
            try:
                import playwright  # noqa: F401
            except Exception:  # noqa: BLE001
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
            from playwright.sync_api import sync_playwright
            PROBE = """() => {
              const q = (id) => { const e = document.getElementById(id); return e ? e.textContent.trim().slice(0, 200) : null; };
              const paths = Array.from(document.querySelectorAll('#world-map svg path'));
              const ps = document.getElementById('pillarsSection');
              return { decisive: q('decisiveText'), downturn: q('downturnProb'), downturnSub: q('downturnSub'), basis: q('basisCount'), basisSub: q('basisSub'),
                pillarsShown: !!(ps && ps.style.display !== 'none'),
                pillarCards: document.querySelectorAll('#pillarGrid .pillar-card').length, drivers: (q('driversText')||'').slice(0,160),
                tiles: document.querySelectorAll('#regionGrid .country-tile').length, pbars: document.querySelectorAll('#regionGrid .country-tile .pbar').length,
                confTags: Array.from(document.querySelectorAll('#regionGrid .fresh-tag')).filter(e => e.textContent.startsWith('conf')).length,
                ladder: document.querySelectorAll('#ladder .ladder-cell').length, mapWithData: paths.filter(p => p.classList.contains('has-data')).length,
                hook: window.__gbc || null }; }"""
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(channel="chrome", headless=True)
                except Exception:  # noqa: BLE001
                    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                    browser = p.chromium.launch(headless=True)
                for tag, vp in (("desktop", {"width": 1440, "height": 1000}), ("mobile", {"width": 390, "height": 844})):
                    ctx = browser.new_context(viewport=vp)
                    page = ctx.new_page()
                    console, errors = [], []
                    page.on("console", lambda m: console.append({"type": m.type, "text": m.text[:300]}))
                    page.on("pageerror", lambda e: errors.append(str(e)[:300]))
                    page.goto("https://justhodl.ai/global-cycle.html", wait_until="domcontentloaded", timeout=60000)
                    try:
                        page.wait_for_load_state("networkidle", timeout=20000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(9000)
                    dom = page.evaluate(PROBE)
                    page.screenshot(path=str(ROOT / "aws" / "ops" / "reports" / "latest" / f"5103-gc-v3-{tag}.jpg"), full_page=True, type="jpeg", quality=55)
                    csp = [c["text"] for c in console if "content security policy" in c["text"].lower()]
                    r.log(f"{tag}: {json.dumps(dom)[:1200]}")
                    r.log(f"{tag}: CSP violations {len(csp)} page errors {len(errors)} {json.dumps(errors)[:300]}")
                    r.kv(visit=tag, downturn=dom.get("downturn"), basis=dom.get("basis"), pillar_cards=dom.get("pillarCards"), pbars=dom.get("pbars"),
                         conf_tags=dom.get("confTags"), tiles=dom.get("tiles"), map_with_data=dom.get("mapWithData"), page_errors=len(errors), csp=len(csp))
                    if errors or csp:
                        fails.append(f"{tag}: page errors {len(errors)} / CSP {len(csp)}")
                    if not (dom.get("pillarsShown") and dom.get("pillarCards") == 5 and (dom.get("pbars") or 0) >= 30 and (dom.get("tiles") or 0) >= 34
                            and str(dom.get("downturn") or "").endswith("%") and (dom.get("mapWithData") or 0) >= 30 and (dom.get("hook") or {}).get("composite")):
                        fails.append(f"{tag}: v3 elements missing: {json.dumps(dom)[:300]}")
                    page.close()
                    ctx.close()
                browser.close()

        r.section("verdict")
        for f in fails:
            r.fail(f)
        if fails:
            sys.exit(1)
        r.ok(f"VERDICT: GREEN -- feature store live (schedule 10:30 UTC), GBC v{WANT} live with {n_multi}/34 multi-pillar countries, "
             f"downturn p6m={cal.get('probability_now')} (auc {cal.get('in_sample_auc')}), global {agg.get('global_phase')} {agg.get('global_avg_cli')}")


if __name__ == "__main__":
    main()
