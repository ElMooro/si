"""ops 6709 - release the false Nov 1 FREEZE (issue #131), refresh the ETF research chain, read the SPY strip.

Why (Khalid, 2026-11-02 10:34 ET: chart.html's ETF RESEARCH strip for SPY shows "USD Unavailable" for the 1 / 5 / 21
reported-observation flow windows - "isn't working").  ops 6708: the windows are in the data (SPY +$5.23B on 10-30,
+$2.20B 5-obs, +$1.58B 21-obs) but their source_valid_until passed at 2026-11-02 00:00 UTC and neither engine has run
since 2026-10-31 - both sit at reserved concurrency 0.  Cause: at 2026-11-01 00:07 UTC the governor's first November
tick restored the 904 functions of the October freeze (Khalid's planned Nov 1 thaw) and, in the same tick, read AWS
Budgets' not-yet-rolled-over ActualSpend - October's $1,176.94 - as November's month-to-date and FREEZE-held all 904
again (Cost Explorer: $0.00).  v1.5.3 (commit 16cb1bc87, verified by ops 6706) stops that figure from counting at a
rollover, but by design no version change lifts a freeze; 2 of 1,317 declared outputs were fresh at 11:27 UTC today
(ops 6707).  Budgets rolled over at 2026-11-01 15:59 UTC: the governor now reads November MTD $30.70 (= Cost Explorer),
forecast $311, freeze line $532.50.

A. pre-flight (read-only): governor status with a fresh Cost Explorer read; the freeze being released must be the #131
   one (event record escalate-freeze at 00:07:37 with MTD $1,176.94, no later escalation), stage FREEZE, MTD below the
   BRAKE line, and the budget figure now agreeing with Cost Explorer (so the next hourly tick cannot re-freeze).  Any
   guard failing -> nothing is released.
B. {"action":"restore","suppress_hours":0} on justhodl-spend-governor - the runbook's operator release of BRAKE/FREEZE
   entries only.  No suppression: every stage line, the fuses, the $50 engine cap and the anomaly sentinel stay armed.
   The five October CAP holds (event-coordinator, miss-calibrator, macro-financial-intelligence, ofrapi, treasury-api)
   are not touched (KHALID_ACTIONS 0d: Khalid's call).
C. proof: status again; every function's reserved concurrency read back - the only zeros left must be the CAP holds.
D. the ETF research chain, one on-demand run each in its scheduled order (all three would fire tonight anyway; one
   extra run of three daily engines ~ $0.07): justhodl-etf-fund-flows -> justhodl-etf-constituents ->
   justhodl-etf-global-desk, each awaited on its publication's LastModified; then SPY's windows read from the new desk
   packet.
E. the served page: headless Chrome on https://justhodl.ai/chart.html?s=SPY, the #etfhud strip's text.
No key involved.
"""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
GOVERNOR = "justhodl-spend-governor"
FREEZE_EVENT = "spend-governor/events/20261101T000737.791709Z-escalate-freeze.json"
CAPPED = {"justhodl-event-coordinator", "justhodl-miss-calibrator", "macro-financial-intelligence", "ofrapi", "treasury-api"}
CHAIN = [("justhodl-etf-fund-flows", "data/provider-fund-flow-research.json", 480),
         ("justhodl-etf-constituents", "data/etf-holdings-research.json", 1020),
         ("justhodl-etf-global-desk", "data/etf-desk-research.json", 1020)]
CFG = Config(retries={"max_attempts": 10, "mode": "adaptive"}, connect_timeout=10, read_timeout=900)
BASE = "https://justhodl.ai"


def invoke(lam, payload):
    resp = lam.invoke(FunctionName=GOVERNOR, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
    body = json.loads(resp["Payload"].read() or b"{}")
    if resp.get("FunctionError"):
        raise RuntimeError("%s: %s" % (payload.get("action"), str(body)[:600]))
    return body


def summary(st):
    th = st.get("thresholds") or {}
    return ("stage %s since %s | mtd $%s (brake $%s, freeze $%s) | budget actual $%s forecast $%s | sources %s | held %s | capped %s | suppress %s" % (
        st.get("stage"), st.get("stage_since"), th.get("mtd"), th.get("brake_at"), th.get("freeze_at"), st.get("budget_actual"),
        st.get("budget_forecast"), json.dumps(th.get("sources")), json.dumps(st.get("held")), st.get("capped"), st.get("suppress_until")))


def zero_concurrency(lam):
    names = []
    for page in lam.get_paginator("list_functions").paginate():
        names += [f["FunctionName"] for f in page["Functions"]]

    def one(n):
        try:
            return n, lam.get_function_concurrency(FunctionName=n).get("ReservedConcurrentExecutions")
        except Exception as e:  # noqa: BLE001
            return n, "ERR %s" % type(e).__name__
    with ThreadPoolExecutor(max_workers=6) as ex:
        res = dict(ex.map(one, names))
    return names, sorted(n for n, v in res.items() if v == 0), sorted(n for n, v in res.items() if isinstance(v, str))


def wait_for(s3, key, after, limit_s, r):
    t0 = time.time()
    while time.time() - t0 < limit_s:
        lm = s3.head_object(Bucket=LIVE, Key=key)["LastModified"]
        if lm > after:
            return lm
        time.sleep(15)
    r.warn("%s not rewritten within %d s" % (key, limit_s))
    return None


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6709_release_false_freeze") as r:
        r.heading("ops 6709 - release the false Nov 1 FREEZE (#131), refresh the ETF research chain, read the SPY strip")
        r.section("A. pre-flight (read-only)")
        st = invoke(lam, {"action": "status", "refresh": True})
        r.log("before: " + summary(st))
        th = st.get("thresholds") or {}
        ev = json.loads(s3.get_object(Bucket=PRIVATE, Key=FREEZE_EVENT)["Body"].read())
        later = [o["Key"] for p in boto3.client("s3", region_name=REGION).get_paginator("list_objects_v2").paginate(
            Bucket=PRIVATE, Prefix="spend-governor/events/2026110") for o in p.get("Contents", [])
            if o["Key"] > FREEZE_EVENT and ("escalate" in o["Key"] or "restore" in o["Key"] or "force" in o["Key"])]
        b, ce, base = st.get("budget_actual"), (th.get("sources") or {}).get("ce_mtd"), float(th.get("baseline_usd_day") or 10)
        guards = {
            "the freeze is #131's (escalate-freeze 00:07:37, MTD $1,176.94)": ev.get("kind") == "escalate-freeze" and abs(float((ev.get("thresholds") or {}).get("mtd") or 0) - 1176.94) < 0.01,
            "no escalation / restore / force since": not later,
            "stage is FREEZE": st.get("stage") == "FREEZE",
            "FREEZE entries held": any(k.startswith("FREEZE/") for k in (st.get("held") or {})),
            "MTD below the BRAKE line": th.get("mtd") is not None and float(th["mtd"]) < float(th.get("brake_at") or 0),
            "budget figure agrees with Cost Explorer (rolled over)": b is not None and ce is not None and abs(float(b) - float(ce)) <= 3 * max(base, 5.0)
            and "budget_actual_ignored" not in (th.get("sources") or {}),
            "mode enforce, version >= 1.5.3": st.get("mode") == "enforce" and tuple(int(x) for x in str(st.get("version", "0")).split(".")[:3]) >= (1, 5, 3),
        }
        for k, v in guards.items():
            (r.ok if v else r.warn)("%s: %s" % (k, v))
        if later:
            r.log("later governor records: %s" % later)
        if not all(guards.values()):
            r.fail("a guard failed - nothing released")
            raise SystemExit(1)

        r.section("B. operator release of the BRAKE/FREEZE entries (no suppression; CAP holds untouched)")
        t0 = time.time()
        out = invoke(lam, {"action": "restore", "suppress_hours": 0,
                           "reason": "ops 6709: the Nov 1 00:07 FREEZE read October's budget actual $1,176.94 as November MTD (issue #131); "
                                     "budget rolled over 2026-11-01 15:59 UTC, MTD now $%s; v1.5.3 guards the rollover" % th.get("mtd")})
        r.log("restore answered in %.1f s: %s" % (time.time() - t0, json.dumps(out)))
        failed = (out.get("restored") or {}).get("restore_failed", 0)
        (r.ok if out.get("stage") == "NORMAL" and not failed else r.warn)("stage after restore: %s; restore_failed %s" % (out.get("stage"), failed))

        r.section("C. proof: governor status + every function's reserved concurrency")
        st2 = invoke(lam, {"action": "status"})
        r.log("after: " + summary(st2))
        names, zeros, errs = zero_concurrency(lam)
        r.kv(functions=len(names), concurrency_zero=len(zeros), read_errors=len(errs))
        r.log("%d functions; reserved concurrency 0: %d %s; read errors %d %s" % (len(names), len(zeros), zeros[:40], len(errs), errs[:10]))
        stray = [n for n in zeros if n not in CAPPED]
        (r.ok if not stray else r.warn)("held at 0 beyond the five CAP holds: %s" % (stray[:60] or "none"))
        etf_held = [n for n, _, _ in CHAIN if n in zeros]
        if etf_held:
            r.fail("ETF engines still held: %s - chain refresh skipped" % etf_held)
            raise SystemExit(1)

        r.section("D. the ETF research chain: one run each, in scheduled order")
        for fn, key, limit in CHAIN:
            before = s3.head_object(Bucket=LIVE, Key=key)["LastModified"]
            start = datetime.now(timezone.utc)
            resp = lam.invoke(FunctionName=fn, InvocationType="Event", Payload=b"{}")
            r.log("%s invoked (HTTP %s); waiting for %s (was %s)" % (fn, resp.get("StatusCode"), key, before.isoformat()[:19]))
            lm = wait_for(s3, key, max(before, start), limit, r)
            if lm:
                r.ok("%s rewrote %s at %s (%.0f s)" % (fn, key, lm.isoformat()[:19], (lm - start).total_seconds()))
            else:
                r.fail("%s did not publish - chain stopped" % fn)
                raise SystemExit(1)
        desk = json.loads(s3.get_object(Bucket=LIVE, Key="data/etf-desk-research.json")["Body"].read())
        r.log("desk generated_at %s | quality %s" % (desk.get("generated_at"), json.dumps({k: v for k, v in (desk.get("quality") or {}).items() if k != "additional_funds"})[:400]))
        now = datetime.now(timezone.utc)
        for t in ("SPY", "QQQ", "IWM", "TLT", "GLD"):
            f = (desk.get("funds") or {}).get(t) or {}
            fl = f.get("flows") or {}
            wins = {n: ((fl.get("aligned_windows") or {}).get(n) or {}) for n in ("1", "5", "21")}
            valid = fl.get("source_valid_until")
            hours = (datetime.fromisoformat(valid) - now).total_seconds() / 3600 if valid else None
            r.log("%s: flow_current_eligible %s | latest effective %s | valid for %s h | %s" % (
                t, (f.get("quality") or {}).get("flow_current_eligible"), fl.get("latest_effective_date"),
                None if hours is None else round(hours, 1),
                " | ".join("%s: %s %s..%s %s" % (n, w.get("status"), w.get("start_date"), w.get("end_date"), w.get("flow_usd_decimal")) for n, w in wins.items())))
        spy = (desk.get("funds") or {}).get("SPY") or {}
        spy_ok = (spy.get("quality") or {}).get("flow_current_eligible") and all(
            ((spy.get("flows") or {}).get("aligned_windows") or {}).get(n, {}).get("status") == "matched_reporting_window" for n in ("1", "5", "21"))
        (r.ok if spy_ok else r.warn)("SPY flow windows current and matched: %s" % bool(spy_ok))

        r.section("E. the served page: chart.html?s=SPY, #etfhud")
        try:
            try:
                from playwright.sync_api import sync_playwright
            except ImportError:
                subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
                from playwright.sync_api import sync_playwright
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(channel="chrome", headless=True)
                except Exception:  # noqa: BLE001
                    subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                    browser = p.chromium.launch(headless=True)
                text = ""
                for attempt in range(1, 5):
                    ctx = browser.new_context(viewport={"width": 1600, "height": 1000}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6709 etf-strip-probe")
                    page = ctx.new_page()
                    page.set_default_timeout(45000)
                    page.goto("%s/chart.html?s=SPY&nocache=%d" % (BASE, int(time.time())), wait_until="domcontentloaded")
                    try:
                        page.wait_for_selector("#etfhud.on", timeout=45000)
                    except Exception:  # noqa: BLE001
                        pass
                    page.wait_for_timeout(4000)
                    text = page.evaluate("() => { const e = document.getElementById('etfhud'); return e ? e.innerText : null; }") or ""
                    r.log("attempt %d: #etfhud = %s" % (attempt, " ".join(text.split())[:600]))
                    ctx.close()
                    if text and "USD Unavailable" not in text and "reported observation" in text:
                        break
                    time.sleep(45)
                browser.close()
            (r.ok if text and "USD Unavailable" not in text and "reported observation" in text else r.warn)(
                "served strip shows flow values: %s" % bool(text and "USD Unavailable" not in text and "reported observation" in text))
        except Exception as e:  # noqa: BLE001
            r.warn("browser check failed: %s %s" % (type(e).__name__, str(e)[:300]))
        r.ok("ops 6709 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
