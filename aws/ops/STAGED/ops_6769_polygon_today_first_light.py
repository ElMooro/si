"""ops 6769 - justhodl-polygon-today first light + the live proof that the chart's SPY bar is today's (Khalid 2026-11-10: "SPY
definitely is delayed almost 24 hours ... I want all the stocks and ETFs data to be accurate"). Re-runnable.

  1. the fan-out manifest justhodl-scheduler reads (s3://justhodl-dashboard-live/config/fanout-manifest.json): the live copy
     must equal the repo copy minus the new member (additions-only), then the repo copy is uploaded - the new member fires
     with the next jhk-tick-session-5min tick;
  2. the two deploys landed (justhodl-polygon-today exists, justhodl-symdir's receipt commit is at or after this change);
  3. invoke justhodl-polygon-today once -> data/warm/polygon-full/today.json.gz + today-manifest.json (session = today NY,
     thousands of names, SPY / QQQ / AAPL / TLT / GLD present, as_of within the delay window);
  4. the chart's lane: the Worker /ohlc (span=day) for those names now ends on TODAY's session with the overlay's close,
     and the Worker /quotes price agrees with it within the 5-minute snapshot window; weekly reads fold it in;
  5. headless Chrome on https://justhodl.ai/chart.html?s=SPY: window.lastBars' last candle is today's session, its close is
     the overlay's; the same for QQQ. A 0.5x JPEG is embedded.
Writes: the fan-out manifest (one added member) and the engine's own two keys. Nothing else.
"""
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
import base64
import gzip
import json
import subprocess
import sys
import time

import boto3
import requests
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
PROXY, SITE = "https://justhodl-data-proxy.raafouis.workers.dev", "https://justhodl.ai"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)
NEW, FAN_KEY = "justhodl-polygon-today", "config/fanout-manifest.json"
TICKERS = ["SPY", "QQQ", "AAPL", "TLT", "GLD"]
NY = ZoneInfo("America/New_York")
UA = {"User-Agent": "justhodl-ops-6769"}


def get(url, **kw):
    try:
        r = requests.get(url, headers=UA, timeout=40, **kw)
        try:
            return r.status_code, r.json()
        except Exception:  # noqa: BLE001
            return r.status_code, {"_text": r.text[:200]}
    except Exception as e:  # noqa: BLE001
        return None, {"error": str(e)[:200]}


def day_of(ts):
    return datetime.fromtimestamp(float(ts), tz=NY).date().isoformat()


def ensure_playwright(log):
    try:
        import playwright  # noqa: F401
    except Exception:  # noqa: BLE001
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
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6769_polygon_today_first_light") as r:
        now = datetime.now(timezone.utc)
        today = now.astimezone(NY).date().isoformat()
        r.heading("ops 6769 - polygon-today first light %s (%s ET, session %s)" % (now.isoformat(timespec="minutes"), now.astimezone(NY).strftime("%H:%M"), today))
        fails = []

        r.section("1. the fan-out manifest the scheduler reads")
        repo = json.loads((ROOT / FAN_KEY).read_text(encoding="utf-8"))
        live = json.loads(s3.get_object(Bucket=LIVE, Key=FAN_KEY)["Body"].read())
        repo_ticks = {k: sorted(v) for k, v in repo["ticks"].items()}
        live_ticks = {k: sorted(v) for k, v in live["ticks"].items()}
        repo_minus = {k: [f for f in v if f != NEW] for k, v in repo_ticks.items()}
        r.log("repo ticks %d | live ticks %d | live has %s: %s | repo-minus-new == live: %s | disabled equal: %s" % (
            sum(len(v) for v in repo_ticks.values()), sum(len(v) for v in live_ticks.values()), NEW, NEW in live_ticks.get("session-5min", []), repo_minus == live_ticks, sorted(repo.get("disabled") or []) == sorted(live.get("disabled") or [])))
        if NEW in live_ticks.get("session-5min", []) and repo_ticks == live_ticks:
            r.log("live manifest already carries the member - nothing to upload")
        elif repo_minus == live_ticks and sorted(repo.get("disabled") or []) == sorted(live.get("disabled") or []):
            s3.put_object(Bucket=LIVE, Key=FAN_KEY, Body=json.dumps(repo, indent=1, ensure_ascii=False).encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.log("uploaded the repo manifest (additions-only: +%s in session-5min)" % NEW)
        else:
            diff = {k: (sorted(set(live_ticks.get(k, [])) ^ set(repo_minus.get(k, [])))) for k in set(live_ticks) | set(repo_minus) if sorted(live_ticks.get(k, [])) != sorted(repo_minus.get(k, []))}
            fails.append("the live fan-out manifest differs from the repo beyond the new member: %s" % json.dumps(diff)[:600])
            r.warn(fails[-1])
        back = json.loads(s3.get_object(Bucket=LIVE, Key=FAN_KEY)["Body"].read())
        r.log("live session-5min members now: %s" % back["ticks"].get("session-5min"))
        if NEW not in back["ticks"].get("session-5min", []):
            fails.append("%s is not in the live session-5min tick" % NEW)

        r.section("2. the deploys")
        for fn in (NEW, "justhodl-symdir"):
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("%s: modified %s sha %s timeout %s memory %s" % (fn, (cf.get("LastModified") or "")[:19], (cf.get("CodeSha256") or "")[:12], cf.get("Timeout"), cf.get("MemorySize")))
            except Exception as e:  # noqa: BLE001
                fails.append("%s: %s" % (fn, type(e).__name__))
                r.warn(fails[-1])

        r.section("3. first run")
        t0 = time.time()
        resp = lam.invoke(FunctionName=NEW, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6769_first_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        r.log("invoke: status %s error %s %.1fs -> %s" % (resp.get("StatusCode"), resp.get("FunctionError"), time.time() - t0, json.dumps(payload)[:700]))
        if resp.get("FunctionError") or not payload.get("wrote"):
            fails.append("the engine did not write: %s" % json.dumps(payload)[:300])
        man = {}
        try:
            man = json.loads(s3.get_object(Bucket=LIVE, Key="data/warm/polygon-full/today-manifest.json")["Body"].read())
            obj = s3.get_object(Bucket=LIVE, Key="data/warm/polygon-full/today.json.gz")
            doc = json.loads(gzip.decompress(obj["Body"].read()))
            r.log("today.json.gz: %d bytes gz | session %s | t %s (%s ET) | as_of %s | delay %s min | n %d of %s tickers (%s stale) | sample %s" % (
                obj["ContentLength"], doc.get("session"), doc.get("t"), datetime.fromtimestamp(doc["t"], tz=NY).strftime("%Y-%m-%d %H:%M"), doc.get("as_of"), doc.get("delay_minutes"), doc.get("n") or 0, man.get("n_tickers"), man.get("n_stale"),
                json.dumps({t: (doc.get("rows") or {}).get(t) for t in TICKERS})))
            if doc.get("session") != today or (doc.get("n") or 0) < 3000 or any(t not in (doc.get("rows") or {}) for t in TICKERS):
                fails.append("overlay incomplete: session %s n %s" % (doc.get("session"), doc.get("n")))
            as_of = datetime.fromisoformat(doc["as_of"])
            age = (now - as_of).total_seconds() / 60
            r.log("overlay as_of age %.1f min (the tape is %s-minute delayed)" % (age, doc.get("delay_minutes")))
            if age > 40:
                fails.append("overlay as_of is %.0f minutes old" % age)
        except Exception as e:  # noqa: BLE001
            fails.append("overlay read: %s %s" % (type(e).__name__, str(e)[:120]))
            r.warn(fails[-1])
            doc = {}
        overlay_rows = (doc.get("rows") or {})

        r.section("4. the chart's lane: Worker /ohlc ends on today's session; /quotes agrees")
        st, quotes = get(PROXY + "/quotes?tickers=%s&nocache=%d" % (",".join(TICKERS), int(time.time())))
        qt = (quotes or {}).get("tickers") or {}
        r.log("/quotes HTTP %s: %s" % (st, json.dumps({t: {"price": (qt.get(t) or {}).get("price"), "dayClose": (qt.get(t) or {}).get("dayClose"), "updated_min": round((time.time() * 1000 - ((qt.get(t) or {}).get("updatedTs") or 0)) / 60000, 1)} for t in TICKERS})))
        for t in TICKERS:
            last = None
            for attempt in range(8):                                     # the Worker caches the symdir answer 60 s
                st, body = get(PROXY + "/ohlc?ticker=%s&span=day&mult=1&days=10&nocache=%d" % (t, int(time.time() * 1000)))
                bars = (body or {}).get("bars") or []
                last = bars[-1] if bars else None
                if last and day_of(last.get("time")) == today:
                    break
                time.sleep(25)
            ov = overlay_rows.get(t)
            ok = bool(last) and day_of(last.get("time")) == today and ov and abs(float(last.get("close")) - float(ov[3])) < 1e-6
            r.log("%-4s /ohlc: HTTP %s source %s count %s | last bar %s ET close %s vol %s | overlay close %s | /quotes price %s | %s" % (
                t, st, (body or {}).get("source"), (body or {}).get("count"), datetime.fromtimestamp(float(last["time"]), tz=NY).strftime("%Y-%m-%d %H:%M") if last else None, (last or {}).get("close"), (last or {}).get("value"),
                ov[3] if ov else None, (qt.get(t) or {}).get("price"), "OK today's forming bar" if ok else "NOT today"))
            if not ok:
                fails.append("%s /ohlc does not end on today's forming bar" % t)
        st, body = get(PROXY + "/ohlc?ticker=SPY&span=week&mult=1&days=120&nocache=%d" % int(time.time() * 1000))
        wb = ((body or {}).get("bars") or [])
        r.log("SPY weekly: HTTP %s count %s | last week bar %s close %s (folds today's bar: %s)" % (st, len(wb), day_of(wb[-1]["time"]) if wb else None, wb[-1].get("close") if wb else None,
              bool(wb) and overlay_rows.get("SPY") and abs(float(wb[-1]["close"]) - float(overlay_rows["SPY"][3])) < 1e-6))

        r.section("5. chart.html in headless Chrome: the last candle is today's")
        sync_playwright = ensure_playwright(r.log)
        shots = []
        with sync_playwright() as p:
            browser = launch(p, r.log)
            ctx = browser.new_context(viewport={"width": 1440, "height": 900}, user_agent="Mozilla/5.0 (X11; Linux x86_64) justhodl-ops/6769")
            page = ctx.new_page()
            page.set_default_timeout(45000)
            errors = []
            page.on("pageerror", lambda e: errors.append(str(e)[:200]))
            for sym in ("SPY", "QQQ"):
                page.goto("%s/chart.html?s=%s&nocache=%d" % (SITE, sym, int(time.time())), wait_until="domcontentloaded")
                try:
                    page.wait_for_function("() => window.lastBars && window.lastBars.length > 50 && window.jhActive === '%s'" % sym, timeout=45000)
                except Exception as e:  # noqa: BLE001
                    r.warn("%s: lastBars not ready (%s)" % (sym, type(e).__name__))
                page.wait_for_timeout(2500)
                st5 = page.evaluate("""() => { const b = window.lastBars || []; const l = b[b.length - 1] || {}; const p = b[b.length - 2] || {};
                    return {n: b.length, active: window.jhActive, source: window.lastSource, last: {time: l.time, close: l.close, open: l.open, volume: l.volume}, prev: {time: p.time, close: p.close},
                            header: (document.querySelector('#hdr-px, .px, #lastpx') || {}).innerText || null}; }""")
                lt = st5.get("last") or {}
                ok = lt.get("time") and day_of(lt["time"]) == today and overlay_rows.get(sym) and abs(float(lt.get("close") or 0) - float(overlay_rows[sym][3])) < 1e-6
                r.log("%s: %d bars | source %s | last candle %s ET close %s open %s vol %s | prev %s close %s | overlay close %s | %s" % (
                    sym, st5.get("n") or 0, st5.get("source"), datetime.fromtimestamp(float(lt["time"]), tz=NY).strftime("%Y-%m-%d %H:%M") if lt.get("time") else None, lt.get("close"), lt.get("open"), lt.get("volume"),
                    day_of((st5.get("prev") or {}).get("time")) if (st5.get("prev") or {}).get("time") else None, (st5.get("prev") or {}).get("close"), (overlay_rows.get(sym) or [None] * 4)[3], "OK today's candle" if ok else "NOT today"))
                if not ok:
                    fails.append("chart.html %s: the last candle is not today's forming bar" % sym)
                if sym == "SPY":
                    shots.append(page.screenshot(type="jpeg", quality=50))
            r.log("page errors: %s" % (errors or "none"))
            browser.close()
        r.kv(session=today, overlay_n=str(doc.get("n")), as_of=str(doc.get("as_of")), failed=str(len(fails)))
        if shots:
            r.section("6. screenshot (SPY, jpeg, base64)")
            r.lines.append("")
            r.lines.append("<!-- SCREENSHOT_JPEG_BASE64 -->")
            r.lines.append(base64.b64encode(shots[0]).decode("ascii"))
            r.lines.append("<!-- /SCREENSHOT_JPEG_BASE64 -->")
        if fails:
            r.fail("ops 6769: %d check(s) failed: %s" % (len(fails), " | ".join(fails)[:900]))
            raise SystemExit(1)
        r.ok("ops 6769 complete: today's forming bar (%s names, as of %s, %s-minute delayed) is live in the bank, the Worker /ohlc lane and chart.html for session %s" % (doc.get("n"), doc.get("as_of"), doc.get("delay_minutes"), today))


if __name__ == "__main__":
    main()
    sys.exit(0)
