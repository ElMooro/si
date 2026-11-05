"""ops 6752 - READ-ONLY baseline of the chart's "Capitulation · panic volume at the lows" desk (Khalid 2026-11-05: "audit
it and see if we can improve it. maybe add etfs, crypto, metal, bonds and other asset class too").

The chart button btn-capit opens /wyckoff-desk.html?tab=capit, whose rows come from data/bottom.json's `board` (the
top 320 bottom-sequence rows only) filtered to frame D, state CLIMAX / TESTING / NO_RALLY, sc_vol_x >= 3.5,
sc_range_x >= 1.5, sc_decline_pct <= -15, climax <= 21 days old, within 15% of the climax low. This probe reads the live
artifact and reports: freshness, universe and desk counts, how many rows the desk's filter keeps (by desk), what the
full board_all holds (climaxes by desk inside 21 / 60 days), the market breadth block, the bench rows, the history
listing, the engine's schedule and last runs; then renders the served desk tab in headless Chrome (rows, desks shown).
Nothing is written.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import Counter
import json
import subprocess
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, read_timeout=120)
BASE = "https://justhodl.ai"


def days(d, today):
    try:
        return (today - datetime.strptime(d[:10], "%Y-%m-%d").date()).days
    except Exception:  # noqa: BLE001
        return 9999


def capit_filter(r, today):
    if r.get("frame") != "D" or r.get("state") not in ("CLIMAX", "TESTING", "NO_RALLY"):
        return False
    if (r.get("sc_vol_x") or 0) < 3.5 or (r.get("sc_range_x") or 0) < 1.5 or (r.get("sc_decline_pct") or 0) > -15:
        return False
    if days(r.get("sc_date") or "", today) > 21:
        return False
    if r.get("dist_sc_low_pct") is not None and r["dist_sc_low_pct"] > 15 and r.get("state") != "CLIMAX":
        return False
    return True


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:
        log("installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    return sync_playwright


def launch(p, log):
    try:
        return p.chromium.launch(channel="chrome", headless=True)
    except Exception as e:  # noqa: BLE001
        log("chrome channel unavailable (%s); installing chromium" % type(e).__name__)
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        return p.chromium.launch(headless=True)


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
    logs = boto3.client("logs", region_name="us-east-1", config=CFG)
    today = datetime.now(timezone.utc).date()
    with report("ops_6752_capitulation_baseline_probe") as r:
        r.heading("ops 6752 - capitulation desk baseline (read-only) %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("A. data/bottom.json")
        obj = s3.get_object(Bucket=LIVE, Key="data/bottom.json")
        doc = json.loads(obj["Body"].read())
        r.log("written %s | generated %s | session %s | version %s | %d bytes | elapsed %ss | universe %s | counts %s | degraded %s" % (
            obj["LastModified"].isoformat(timespec="seconds"), doc.get("generated_at"), doc.get("session"), doc.get("version"), obj["ContentLength"], doc.get("elapsed_s"),
            json.dumps(doc.get("universe")), json.dumps(doc.get("counts")), doc.get("degraded")))
        board, ball = doc.get("board") or [], doc.get("board_all") or []
        r.log("board %d rows by desk %s | board_all %d rows by desk %s" % (len(board), json.dumps(Counter(x.get("desk") for x in board)), len(ball), json.dumps(Counter(x.get("desk") for x in ball))))
        r.log("board states %s | board_all states %s" % (json.dumps(Counter(x.get("state") for x in board)), json.dumps(Counter(x.get("state") for x in ball))))
        kept = [x for x in board if capit_filter(x, today)]
        r.log("the desk's Capitulation filter keeps %d of the %d board rows: by desk %s" % (len(kept), len(board), json.dumps(Counter(x.get("desk") for x in kept))))
        for x in sorted(kept, key=lambda z: -(z.get("sc_vol_x") or 0))[:40]:
            r.log("  %-6s %-30s %-12s %-10s sc %s vol %sx range %sx decline %s%% off-low %s%% last %s state-bars %s" % (
                x.get("ticker"), (x.get("company") or "")[:30], x.get("desk"), x.get("state"), x.get("sc_date"), x.get("sc_vol_x"), x.get("sc_range_x"), x.get("sc_decline_pct"), x.get("dist_sc_low_pct"), x.get("last"), x.get("bars_in_state")))
        # what the whole board_all holds (no tape metrics there): climaxes by desk within 21 / 60 days
        for win in (21, 60):
            c = Counter()
            for x in ball:
                if x.get("frame") == "D" and x.get("sc_date") and days(x["sc_date"], today) <= win and x.get("state") in ("CLIMAX", "TESTING", "NO_RALLY", "ST_CONFIRMED", "TRIGGERED", "FAILED"):
                    c[x.get("desk")] += 1
            r.log("board_all: daily climaxes inside %d days by desk %s" % (win, json.dumps(c)))
        r.log("board rows' sc_vol_x distribution: %s" % json.dumps(Counter(("<2" if (x.get("sc_vol_x") or 0) < 2 else "2-3.5" if (x.get("sc_vol_x") or 0) < 3.5 else "3.5-5" if (x.get("sc_vol_x") or 0) < 5 else ">=5") for x in board if x.get("frame") == "D")))
        mk = doc.get("market") or {}
        r.log("market: %s" % json.dumps(mk)[:1500])
        r.log("climax_gates: %s" % json.dumps(doc.get("climax_gates"))[:800])
        r.log("by_desk sizes: %s | weekly_live %d | top_picks %d | changes %s" % (json.dumps({k: len(v) for k, v in (doc.get("by_desk") or {}).items()}), len(doc.get("weekly_live") or []), len(doc.get("top_picks") or []), json.dumps(doc.get("changes"))[:400]))
        r.log("feeds_asof: %s | row_errors %s | log tail: %s" % (json.dumps(doc.get("feeds_asof"))[:400], json.dumps(doc.get("row_errors"))[:300], " || ".join((doc.get("log") or [])[-6:])[:900]))
        keys = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LIVE, Prefix="data/bottom/history/"):
            keys += [(o["Key"], o["Size"]) for o in page.get("Contents") or []]
        r.log("history snapshots: %d (%s .. %s)" % (len(keys), keys[0][0] if keys else "-", keys[-1][0] if keys else "-"))
        r.section("B. the engine")
        cf = lam.get_function_configuration(FunctionName="justhodl-bottom")
        r.log("justhodl-bottom: %s MB / %s s, modified %s, state %s, env keys %s" % (cf.get("MemorySize"), cf.get("Timeout"), (cf.get("LastModified") or "")[:19], cf.get("State"), sorted((cf.get("Environment") or {}).get("Variables", {}).keys())))
        try:
            st = logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-bottom", orderBy="LastEventTime", descending=True, limit=5)
            for s in st.get("logStreams") or []:
                r.log("   stream last event %s" % datetime.fromtimestamp((s.get("lastEventTimestamp") or 0) / 1000, tz=timezone.utc).isoformat(timespec="minutes"))
            since = int((datetime.now(timezone.utc) - timedelta(days=3)).timestamp() * 1000)
            ev = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-bottom", startTime=since, filterPattern="wrote", limit=10)
            for e in ev.get("events") or []:
                r.log("   %s %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, tz=timezone.utc).isoformat(timespec="minutes"), (e.get("message") or "").strip()[:300]))
        except Exception as e:  # noqa: BLE001
            r.warn("logs: %s" % type(e).__name__)
        r.section("C. the served desk tab (headless Chrome)")
        sync_playwright = ensure_playwright(r.log)
        with sync_playwright() as p:
            browser = launch(p, r.log)
            page = browser.new_page(viewport={"width": 1440, "height": 900})
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)))
            page.goto("%s/wyckoff-desk.html?tab=capit&nocache=%d" % (BASE, int(time.time())), wait_until="domcontentloaded")
            try:
                page.wait_for_selector("table tbody tr", timeout=45000)
            except Exception:  # noqa: BLE001
                pass
            page.wait_for_timeout(2500)
            out = page.evaluate("""() => {
              const rows = Array.from(document.querySelectorAll('table tbody tr')).map(tr => Array.from(tr.cells).map(td => td.innerText.trim()));
              const heads = Array.from(document.querySelectorAll('table thead th')).map(th => th.innerText.trim());
              const kpis = Array.from(document.querySelectorAll('.kpi, .jh-kpi, [class*=kpi]')).map(e => e.innerText.replace(/\\s+/g,' ').trim()).slice(0, 12);
              const tabs = Array.from(document.querySelectorAll('[data-tab], .tab, button')).map(e => e.innerText.trim()).filter(Boolean).slice(0, 30);
              return {rows: rows.length, sample: rows.slice(0, 12), heads, kpis, tabs, text: document.body.innerText.slice(0, 1500)};
            }""")
            r.log("served capit tab: %d rows | heads %s | kpis %s" % (out["rows"], json.dumps(out["heads"]), json.dumps(out["kpis"])[:600]))
            for row in out["sample"]:
                r.log("  %s" % json.dumps(row, ensure_ascii=False)[:300])
            r.log("tabs: %s" % json.dumps(out["tabs"], ensure_ascii=False)[:600])
            r.log("page text head: %s" % out["text"][:900].replace("\n", " | "))
            r.log("page errors: %s" % (errors[:5] or "none"))
            browser.close()
        r.kv(board=str(len(board)), board_all=str(len(ball)), kept=str(len(kept)), served_rows=str(out["rows"]), session=str(doc.get("session")))
        r.ok("ops 6752 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
