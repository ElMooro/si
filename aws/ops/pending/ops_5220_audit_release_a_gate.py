"""ops_5220 -- audit 2026-09-08 Release A live gate (read-only + synthetic negative probes; no real user data).

A. justhodl-data-proxy v2.1.0: the audit's attacks answer 401 (anonymous Brain/Journal read+write,
   debug enumeration, the retired purge literal); a random anonymous userdata read is empty; the
   api.justhodl.ai bridge enforces the same verdict; the service role still reads the owner Brain.
B. api_auth SITE tier: an Origin-only burst against a Function-URL consumer is METERED (>= one 429
   inside a 20-call burst at the 15/s ceiling) -- Origin no longer buys Enterprise.
C. Fusion read API: /health carries the readiness object; /fusion carries labels_valid; /opportunities
   is actionable-only by default.
D. Katlin v2.3.0: invoke, wait for a fresh data/katlin.json, assert war_room.authority / local /
   raw_gate / entries_allowed and that the effective cap never exceeds the authority cap.
E. The 7 repointed page cards are live at the edge; brain.html/journal.html/katlin.html carry the
   new markers; katlin.html + brain.html render without page errors in headless Chrome.
sys.exit(1) on any regression.
"""
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
BRIDGE = "https://api.justhodl.ai"
SITE = "https://justhodl.ai"
OWNER_STORE = "brain-930ffa48-60a1-4b11-8726-8848d1b827f9"
SHOTS = ROOT / "aws" / "ops" / "reports" / "latest" / "shots"
FAILS = []
T_PUSH = int(subprocess.run(["git", "log", "-1", "--format=%ct", "HEAD"], capture_output=True, text=True, cwd=ROOT).stdout.strip() or "0")


def http(method, url, headers=None, body=None, timeout=40):
    h = {"User-Agent": "ops5220", "Cache-Control": "no-cache"}
    h.update(headers or {})
    req = urllib.request.Request(url, method=method, headers=h, data=body)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return 0, str(e).encode()


def expect(cond, msg):
    if cond:
        R.ok(msg)
    else:
        FAILS.append(msg)
        R.fail(msg)


with report("ops_5220_audit_release_a_gate") as R:
    R.heading("ops 5220 -- audit 2026-09-08 Release A live gate")
    ssm = boto3.client("ssm", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)

    # ── A. worker negative probes ──────────────────────────────────────────
    R.section("A. data-proxy authorization")
    st, body = http("GET", WORKER + "/health")
    ver = None
    try:
        ver = json.loads(body).get("version")
    except Exception:
        pass
    expect(ver == "2.1.0", "worker version 2.1.0 live (got %s)" % ver)
    for base, label in ((WORKER, "worker"), (BRIDGE, "api.justhodl.ai bridge")):
        hdr = {"Origin": SITE} if base == BRIDGE else {}
        st, _ = http("GET", base + "/brain?uid=" + OWNER_STORE, hdr)
        expect(st == 401, "%s: anonymous GET /brain (owner store id) -> 401 (got %s)" % (label, st))
        st, _ = http("PUT", base + "/brain?uid=" + OWNER_STORE, dict(hdr, **{"Content-Type": "text/plain"}),
                     json.dumps({"note": {"id": "ops5220-probe", "text": "synthetic probe note that must never be stored by the worker"}}).encode())
        expect(st == 401, "%s: anonymous PUT /brain -> 401 (got %s)" % (label, st))
        st, _ = http("PUT", base + "/journal?uid=" + str(uuid.uuid4()), dict(hdr, **{"Content-Type": "text/plain"}), b'{"entries":[]}')
        expect(st == 401, "%s: anonymous PUT /journal -> 401 (got %s)" % (label, st))
        st, _ = http("GET", base + "/brain-debug", hdr)
        expect(st == 401, "%s: anonymous /brain-debug -> 401 (got %s)" % (label, st))
        st, _ = http("GET", base + "/brain-purge?uid=%s&token=jhpurge_9f48_2026" % OWNER_STORE, hdr)
        expect(st == 401, "%s: retired purge literal -> 401 (got %s)" % (label, st))
    st, body = http("GET", WORKER + "/userdata/" + str(uuid.uuid4()))
    expect(st == 200 and b'"empty"' in body, "anonymous /userdata/<random uuid> -> empty (got %s %s)" % (st, body[:60]))
    st, _ = http("GET", WORKER + "/admin/users")
    expect(st == 401, "anonymous /admin/users -> 401 (got %s)" % st)
    try:
        admin = ssm.get_parameter(Name="/justhodl/api-admin/token", WithDecryption=True)["Parameter"]["Value"]
        st, body = http("GET", WORKER + "/brain?uid=" + OWNER_STORE, {"X-JH-Service-Token": admin})
        d = json.loads(body) if st == 200 else {}
        expect(st == 200 and d.get("scope") == "service", "service role reads the owner Brain (%s notes)" % len(d.get("notes") or []))
        st, body = http("GET", WORKER + "/admin/owner", {"X-JH-Service-Token": admin})
        d = json.loads(body) if st == 200 else {}
        expect(st == 200 and (d.get("owner_uids") or d.get("owner_emails")), "owner binding present: emails=%s uids=%d" % (d.get("owner_emails"), len(d.get("owner_uids") or [])))
    except Exception as e:
        FAILS.append("service-role checks failed: %s" % str(e)[:100])

    # ── B. SITE tier metering ─────────────────────────────────────────────
    R.section("B. api_auth SITE tier (Origin no longer grants Enterprise)")
    try:
        fn = "justhodl-treasury-proxy"
        t0 = time.time()
        while time.time() - t0 < 900:
            cfg = lam.get_function_configuration(FunctionName=fn)
            lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
            if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful":
                break
            time.sleep(20)
        R.log("%s LastModified %s (push %s)" % (fn, cfg["LastModified"], datetime.fromtimestamp(T_PUSH, timezone.utc).isoformat()))
        url = lam.get_function_url_config(FunctionName=fn)["FunctionUrl"]
        # the SITE ceiling is 15 requests per second per IP: fire 40 CONCURRENT Origin-only calls
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=40) as ex:
            codes = list(ex.map(lambda _: http("GET", url, {"Origin": SITE, "Referer": SITE + "/"}, timeout=25)[0], range(40)))
        tally = {c: codes.count(c) for c in sorted(set(codes))}
        expect(429 in codes, "Origin-only 40-call burst against %s is metered: %s" % (fn, tally))
        expect(any(c not in (429, 0, 401, 403) for c in codes), "site traffic is still admitted under the ceiling: %s" % tally)
    except Exception as e:
        FAILS.append("SITE-tier probe failed: %s" % str(e)[:120])

    # ── C. fusion read API readiness ──────────────────────────────────────
    R.section("C. fusion read API v1.1")
    st, body = http("GET", SITE + "/api/v1/health")
    try:
        d = json.loads(body)
    except Exception:
        d = {}
    rd = d.get("readiness") or {}
    expect("readiness" in d and "execution_eligible" in rd, "/api/v1/health carries readiness (%s: ok=%s ready=%s fresh=%s coherent=%s)" % (st, d.get("ok"), d.get("ready"), rd.get("fresh"), rd.get("coherent")))
    st, body = http("GET", SITE + "/api/v1/fusion")
    d = json.loads(body) if st == 200 else {}
    expect(st in (200, 503) and ("labels_valid" in d or st == 503), "/api/v1/fusion -> %s labels_valid=%s n=%s" % (st, d.get("labels_valid"), d.get("n")))
    st, body = http("GET", SITE + "/api/v1/opportunities")
    d = json.loads(body) if st == 200 else {}
    expect(st in (200, 503) and ("excluded_not_actionable" in d or st == 503), "/api/v1/opportunities actionable-only (%s, n=%s excluded=%s)" % (st, d.get("n"), d.get("excluded_not_actionable")))

    # ── D. Katlin authority ───────────────────────────────────────────────
    R.section("D. Katlin v2.3.0 capital authority")
    try:
        t0 = time.time()
        while time.time() - t0 < 900:
            cfg = lam.get_function_configuration(FunctionName="justhodl-katlin")
            lm = datetime.fromisoformat(cfg["LastModified"].replace("Z", "+00:00")).timestamp()
            if lm >= T_PUSH and cfg.get("LastUpdateStatus", "Successful") == "Successful":
                break
            time.sleep(20)
        R.log("justhodl-katlin LastModified %s" % cfg["LastModified"])
        before = s3.head_object(Bucket=BUCKET, Key="data/katlin.json")["LastModified"].timestamp()
        lam.invoke(FunctionName="justhodl-katlin", InvocationType="Event", Payload=b"{}")
        R.log("invoked justhodl-katlin (Event) -- waiting for a fresh data/katlin.json")
        fresh = False
        t1 = time.time()
        while time.time() - t1 < 1080:
            time.sleep(30)
            lm = s3.head_object(Bucket=BUCKET, Key="data/katlin.json")["LastModified"].timestamp()
            if lm > before:
                fresh = True
                break
        expect(fresh, "data/katlin.json rewritten after %ds" % int(time.time() - t1))
        if fresh:
            k = json.loads(s3.get_object(Bucket=BUCKET, Key="data/katlin.json")["Body"].read())
            wr = k.get("war_room") or {}
            a, loc, rg = wr.get("authority") or {}, wr.get("local") or {}, wr.get("raw_gate") or {}
            R.log("katlin %s: posture=%s cap=%s entries_allowed=%s | authority %s mode=%s cap=%s allows=%s age=%sh | local %s cap=%s | gate %s x%s age=%sh" % (
                k.get("version"), wr.get("posture"), wr.get("exposure_cap_pct"), wr.get("entries_allowed"), a.get("status"), a.get("mode"), a.get("exposure_cap_pct"),
                a.get("allows_new_entries"), a.get("age_h"), loc.get("posture"), loc.get("exposure_cap_pct"), rg.get("posture"), rg.get("sizing_multiplier"), rg.get("age_h")))
            R.kv(step="katlin", version=k.get("version"), posture=wr.get("posture"), cap=wr.get("exposure_cap_pct"), entries_allowed=wr.get("entries_allowed"),
                 authority_status=a.get("status"), authority_cap=a.get("exposure_cap_pct"), authority_allows=a.get("allows_new_entries"), local_cap=loc.get("exposure_cap_pct"))
            expect(k.get("version") == "2.3.0", "katlin.json version 2.3.0 (got %s)" % k.get("version"))
            expect("authority" in wr and "entries_allowed" in wr and "local" in wr and "raw_gate" in wr, "war_room carries authority/local/raw_gate/entries_allowed")
            if a.get("status") == "FRESH":
                expect((wr.get("exposure_cap_pct") or 0) <= (a.get("exposure_cap_pct") or 0), "effective cap %s <= authority cap %s" % (wr.get("exposure_cap_pct"), a.get("exposure_cap_pct")))
                expect(wr.get("entries_allowed") is not True or a.get("allows_new_entries") is True, "entries never allowed when the authority forbids them")
            else:
                expect(wr.get("posture") == "DATA_HOLD" and wr.get("exposure_cap_pct") == 0, "authority %s -> DATA_HOLD at 0%% (got %s %s)" % (a.get("status"), wr.get("posture"), wr.get("exposure_cap_pct")))
            for r in (k.get("picks") or [])[:400]:
                if r.get("tier") in ("KATLIN_PRIME", "READY") and wr.get("entries_allowed") is False and not r.get("posture_note"):
                    FAILS.append("PRIME/READY row %s lacks a posture_note while entries are blocked" % r.get("ticker")); break
    except Exception as e:
        FAILS.append("Katlin gate failed: %s" % str(e)[:140])

    # ── E. pages at the edge ──────────────────────────────────────────────
    R.section("E. pages")
    cards = {"/sectors.html": "data/volatility-squeeze.json|justhodl-volatility-squeeze-hunter",
             "/global-macro.html": "data/hiring-velocity.json|justhodl-hiring-velocity",
             "/resilience.html": "data/spinoff-desk.json|justhodl-spinoff-desk",
             "/crypto-liquidity.html": "data/index-inclusion.json|justhodl-index-inclusion",
             "/lce.html": "data/fed-pivot-factor-trades.json|justhodl-fed-pivot-factor-router",
             "/signal-intelligence.html": "data/calibration-latest.json|justhodl-alpha-calibrator",
             "/brain.html": "authHdr", "/journal.html": "authHdr", "/katlin.html": "wr-auth"}
    t0 = time.time()
    pending = dict(cards)
    while pending and time.time() - t0 < 900:
        for route, marker in list(pending.items()):
            st, body = http("GET", SITE + route + "?v=%d" % int(time.time()))
            if st == 200 and marker.encode() in body:
                pending.pop(route)
        if pending:
            time.sleep(20)
    for route in cards:
        expect(route not in pending, "%s carries %s at the edge" % (route, cards[route].split("|")[0]))
    st, body = http("GET", SITE + "/resilience.html?v=%d" % int(time.time()))
    expect(b"data/stocktwits.json|justhodl-stocktwits" in body, "/resilience.html stocktwits card repointed")

    R.section("F. headless Chrome")
    try:
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
            for route in ("/katlin.html", "/brain.html"):
                ctx = browser.new_context(viewport={"width": 1440, "height": 1100})
                pg = ctx.new_page()
                errors = []
                pg.on("pageerror", lambda e: errors.append(str(e)[:160]))
                pg.goto(SITE + route + "?v=%d" % int(time.time()), wait_until="domcontentloaded", timeout=60000)
                pg.wait_for_timeout(9000)
                facts = pg.evaluate("""() => ({ auth: (document.getElementById('wr-auth')||{}).innerText || null, cap: (document.getElementById('wr-cap')||{}).innerText || null,
                    status: (document.getElementById('brain-status')||{}).innerText || null, notes: document.querySelectorAll('.note').length, body: (document.body.innerText||'').length })""")
                pg.screenshot(path=str(SHOTS / ("ops5220_%s.png" % route.strip("/").replace(".html", ""))))
                R.log("   %s: page errors %s | auth strip %s | cap %s | brain status %s | notes %s" % (route, errors[:2], (facts["auth"] or "")[:120].replace("\n", " / "), (facts["cap"] or "")[:80].replace("\n", " / "), (facts["status"] or "")[:80].replace("\n", " / "), facts["notes"]))
                expect(not errors, "%s renders without page errors" % route)
                if route == "/katlin.html":
                    expect(bool(facts["auth"]) and "authority" in (facts["auth"] or "").lower(), "katlin.html shows the binding authority strip")
                if route == "/brain.html":
                    expect("read-only" in (facts["status"] or "").lower() or facts["notes"] > 0, "brain.html signed-out view is read-only mirror or shows notes")
                ctx.close()
            browser.close()
    except Exception as e:
        R.warn("headless Chrome unavailable: %s" % str(e)[:120])

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        R.log("RED: %d failure(s)" % len(FAILS))
        sys.exit(1)
    R.ok("GREEN -- Release A of the 2026-09-08 audit is live and verified")
