"""ops_5173 (re-run 2026-09-03T23:05Z) -- Google sign-in VERIFY after Khalid restored the paused Supabase project,
+ keep-alive sentinel armed. Same legs as ops 5171 (which proved NXDOMAIN on the project host).

Khalid: "investigate why I can't login from Google".

The login path is: page -> /auth-config.js -> supabase-js (cdn.jsdelivr.net)
-> /auth.js -> supabase.auth.signInWithOAuth(google) -> top-level navigation
to https://bdmjenqcyvzouusfcgow.supabase.co/auth/v1/authorize?provider=google
-> 302 to accounts.google.com (client_id from the Supabase Google provider)
-> Google -> Supabase callback -> back to the page with ?code= -> supabase-js
exchanges the code at /auth/v1/token (connect-src) -> session.

Known history: the Cloudflare CSP header `a2a-csp-header` (ops 4386, Aug 4)
blocked cdn.jsdelivr.net -- i.e. supabase-js itself -- on every page until
ops 5094 regenerated it on Sep 2. A month without auth traffic is exactly
what pauses a free Supabase project. So the legs are tested one by one:

  S1  Supabase project: /auth/v1/health, /auth/v1/settings (providers)
  S2  OAuth start: /auth/v1/authorize -> where does it send us? then GET
      the Google URL: valid client (account chooser) or an error page
      (deleted_client / invalid_client / redirect_uri_mismatch)
  S3  Site wiring at the edge: auth-config.js, script order, CSP header
  S4  Headless Chrome: load my-portfolio.html and chart-pro.html, collect
      console + CSP violations, click Sign In -> Continue with Google,
      record the navigation target; then the return leg with ?code=x
      to see whether the token exchange is attempted (expect 4xx from
      Supabase for a fake code -- what matters is that the call happens)
  S5  verdict: the first leg that fails, in plain words
"""
import json
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

SB = "https://bdmjenqcyvzouusfcgow.supabase.co"
ANON = "sb_publishable_W6V6OaQ9aXvpVVV9k4Lrpg_pCPuBLvB"
SITE = "https://justhodl.ai"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0 Safari/537.36"
FINDINGS = []


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def get(url, headers=None, follow=True, timeout=25):
    req = urllib.request.Request(url, headers=dict({"User-Agent": UA}, **(headers or {})))
    opener = urllib.request.build_opener() if follow else urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b""
        return e.code, dict(e.headers), body
    except Exception as e:
        return -1, {}, str(e).encode()


def text(b, n=300):
    t = re.sub(r"<[^>]+>", " ", b.decode("utf-8", "ignore"))
    return re.sub(r"\s+", " ", t).strip()[:n]


with report("ops_5173_google_login_forensics") as R:
    R.heading("ops 5173 -- Google sign-in forensics (read-only)")

    # ---------------------------------------------------------------- P0
    R.section("P0 wait for the restored project (DNS + /auth/v1/health), up to 14 minutes")
    import socket
    host = urllib.parse.urlparse(SB).hostname
    back = False
    for i in range(42):
        try:
            socket.gethostbyname(host)
            st0, _, b0 = get(SB + "/auth/v1/health", {"apikey": ANON})
            if st0 == 200:
                back = True
                R.ok("   %s resolves and /auth/v1/health -> 200 after %ds" % (host, i * 20))
                break
            R.log("   t+%3ds resolves; health -> %s %s" % (i * 20, st0, text(b0, 80)))
        except Exception:
            if i % 3 == 0:
                R.log("   t+%3ds still NXDOMAIN" % (i * 20))
        time.sleep(20)
    if not back:
        R.fail("   project still not reachable after 14 minutes -- restore not finished (or not started)")
        FINDINGS.append("project not reachable yet")
        sys.exit(1)

    # ---------------------------------------------------------------- S1
    R.section("S1 Supabase project health + auth settings")
    st, h, b = get(SB + "/auth/v1/health", {"apikey": ANON})
    R.log("   GET /auth/v1/health -> %s %s" % (st, text(b, 160)))
    paused = st in (-1, 502, 503, 540) or b"paused" in b.lower() or b"not found" in b.lower() and st >= 400
    if st != 200:
        FINDINGS.append("Supabase auth health is %s (%s) -- project paused/unreachable?" % (st, text(b, 100)))
    st, h, b = get(SB + "/auth/v1/settings", {"apikey": ANON})
    providers = {}
    try:
        settings = json.loads(b)
        providers = settings.get("external") or {}
        R.log("   GET /auth/v1/settings -> %s  google=%s email=%s facebook=%s  disable_signup=%s  mailer_autoconfirm=%s"
              % (st, providers.get("google"), providers.get("email"), providers.get("facebook"),
                 settings.get("disable_signup"), settings.get("mailer_autoconfirm")))
        if not providers.get("google"):
            FINDINGS.append("Supabase reports the Google provider DISABLED (external.google=%s)" % providers.get("google"))
        R.kv(section="S1", health=st, google=providers.get("google"), email=providers.get("email"),
             disable_signup=settings.get("disable_signup"))
    except Exception:
        R.warn("   settings not JSON: %s %s" % (st, text(b, 200)))
        FINDINGS.append("Supabase /auth/v1/settings unreadable (%s)" % st)
    st, h, b = get(SB + "/rest/v1/profiles?select=plan&limit=1", {"apikey": ANON, "Authorization": "Bearer " + ANON})
    R.log("   GET /rest/v1/profiles (anon) -> %s %s" % (st, text(b, 120)))

    # ---------------------------------------------------------------- S2
    R.section("S2 OAuth start: Supabase authorize -> Google")
    redirect_to = SITE + "/my-portfolio.html"
    url = SB + "/auth/v1/authorize?provider=google&redirect_to=" + urllib.parse.quote(redirect_to, safe="")
    st, h, b = get(url, follow=False)
    loc = h.get("Location") or h.get("location") or ""
    R.log("   GET /auth/v1/authorize?provider=google -> %s  Location: %s" % (st, loc[:160]))
    google_url, client_id, cb = None, None, None
    if st in (302, 303, 307) and "accounts.google.com" in loc:
        google_url = loc
        q = urllib.parse.parse_qs(urllib.parse.urlparse(loc).query)
        client_id = (q.get("client_id") or [""])[0]
        cb = (q.get("redirect_uri") or [""])[0]
        R.log("   client_id=%s  redirect_uri=%s  scope=%s" % (client_id, cb, (q.get("scope") or [""])[0][:60]))
        R.kv(section="S2", client_id=client_id, redirect_uri=cb)
    else:
        R.fail("   authorize did not hand off to Google: %s %s" % (st, text(b, 240)))
        FINDINGS.append("Supabase authorize step failed: %s %s" % (st, text(b, 160)))
    if google_url:
        st, h, b = get(google_url, follow=True)
        t = text(b, 400)
        err = re.search(r"(Error \d{3}: \w+|deleted_client|invalid_client|redirect_uri_mismatch|access_denied|disallowed_useragent|invalid_request|unauthorized_client)", t)
        R.log("   GET Google authorize -> %s  title/text: %s" % (st, t[:220]))
        if err or st >= 400:
            R.fail("   Google rejects the OAuth client: %s" % (err.group(1) if err else st))
            FINDINGS.append("Google OAuth client rejected: %s (client_id %s)" % (err.group(1) if err else st, client_id))
        else:
            R.ok("   Google serves the account chooser for this client -- client and redirect URI are valid")

    # ---------------------------------------------------------------- S3
    R.section("S3 Site wiring at the edge")
    st, h, b = get(SITE + "/auth-config.js")
    cfg = b.decode("utf-8", "ignore")
    en = re.search(r"enabled:\s*(true|false)", cfg)
    su = re.search(r"supabaseUrl:\s*\"([^\"]+)\"", cfg)
    R.log("   /auth-config.js -> %s enabled=%s url=%s key=%s..." % (st, en.group(1) if en else "?", su.group(1) if su else "?",
                                                                    (re.search(r"supabaseAnonKey:\s*\"([^\"]{18})", cfg) or [None, "?"])[1] if st == 200 else "?"))
    if st != 200 or not en or en.group(1) != "true":
        FINDINGS.append("auth-config.js at the edge is %s / enabled=%s" % (st, en.group(1) if en else "?"))
    st, h, b = get("https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2", follow=True)
    ver = re.search(r"supabase-js@([\d.]+)", (h.get("x-jsd-version") or "") + " " + b[:600].decode("utf-8", "ignore"))
    R.log("   supabase-js CDN -> %s  %s bytes  version=%s" % (st, len(b), ver.group(1) if ver else (h.get("x-jsd-version") or "?")))
    for page in ("/my-portfolio.html", "/chart-pro.html"):
        st, h, b = get(SITE + page)
        csp = h.get("Content-Security-Policy") or h.get("content-security-policy") or ""
        body = b.decode("utf-8", "ignore")
        order = [m.group(1) for m in re.finditer(r'<script[^>]+src="([^"]+)"', body) if any(k in m.group(1) for k in ("auth-config", "supabase-js", "/auth.js"))]
        csp_ok = ("supabase.co" in csp and "cdn.jsdelivr.net" in csp.split("connect-src")[0]) if csp else None
        R.log("   %-20s -> %s  scripts=%s  CSP present=%s supabase+jsdelivr allowed=%s"
              % (page, st, order, bool(csp), csp_ok))
        if csp and not csp_ok:
            FINDINGS.append("%s: edge CSP blocks supabase/jsdelivr (%s)" % (page, csp[:120]))
        if order != ["/auth-config.js", "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2", "/auth.js"]:
            R.warn("   script order on %s is %s" % (page, order))
        R.kv(section="S3", page=page, http=st, csp_ok=csp_ok, scripts=len(order))

    # ---------------------------------------------------------------- S4
    R.section("S4 Headless Chrome: click-through and return leg")
    try:
        try:
            import playwright  # noqa: F401  (probe only)
        except Exception:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright

        def launch(p):
            try:
                return p.chromium.launch(channel="chrome", headless=True)
            except Exception:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                return p.chromium.launch(headless=True)

        with sync_playwright() as p:
            browser = launch(p)
            for page_path in ("/my-portfolio.html", "/chart-pro.html?s=VLO&tf=1W"):
                ctx = browser.new_context(viewport={"width": 1440, "height": 1000}, user_agent=UA)
                pg = ctx.new_page()
                console, viol, reqs = [], [], []
                pg.on("console", lambda m: console.append("%s: %s" % (m.type, m.text[:160])))
                pg.on("pageerror", lambda e: console.append("pageerror: %s" % str(e)[:160]))
                pg.on("request", lambda rq: reqs.append(rq.url) if ("supabase.co" in rq.url or "accounts.google" in rq.url) else None)
                pg.on("response", lambda rs: reqs.append("%s %s" % (rs.status, rs.url[:120])) if "supabase.co" in rs.url else None)
                try:
                    pg.goto(SITE + page_path, wait_until="domcontentloaded", timeout=45000)
                    pg.wait_for_timeout(5000)
                    facts = pg.evaluate("""() => ({
                        supabaseLoaded: typeof window.supabase !== 'undefined' && !!window.supabase.createClient,
                        authObj: typeof window.JustHodlAuth !== 'undefined',
                        signinBtn: !!document.querySelector('#jh-signin-btn'),
                        user: (window.JustHodlAuth && window.JustHodlAuth.getUser && window.JustHodlAuth.getUser()) ? 'yes' : 'no'})""")
                    viol = [c for c in console if "Content Security Policy" in c or "Refused to" in c]
                    R.log("   %-32s facts=%s console=%d csp_violations=%d" % (page_path, json.dumps(facts), len(console), len(viol)))
                    for c in console[:6]:
                        R.log("      console: %s" % c)
                    if not facts.get("supabaseLoaded"):
                        FINDINGS.append("%s: supabase-js NOT loaded in the browser (CSP/CDN) -> button does nothing" % page_path)
                    if not facts.get("signinBtn"):
                        R.warn("   no #jh-signin-btn rendered on %s" % page_path)
                    # click-through
                    nav_target = None
                    if facts.get("signinBtn"):
                        pg.click("#jh-signin-btn")
                        pg.wait_for_timeout(800)
                        has_google = pg.evaluate("() => !!document.querySelector('#jh-google')")
                        R.log("   modal opened=%s google button=%s" % (pg.evaluate("() => !!document.querySelector('#jh-auth-modal.open')"), has_google))
                        if has_google:
                            try:
                                with pg.expect_navigation(timeout=15000):
                                    pg.click("#jh-google")
                            except Exception as e:
                                R.warn("   no navigation after clicking Continue with Google: %s" % str(e)[:120])
                            pg.wait_for_timeout(3000)
                            nav_target = pg.url
                            R.log("   after click -> %s" % nav_target[:200])
                            if "accounts.google.com" in nav_target:
                                R.ok("   reached Google's sign-in page (%s)" % pg.title()[:60])
                            elif "supabase.co" in nav_target:
                                R.fail("   stuck at Supabase: %s" % text(pg.content().encode(), 240))
                                FINDINGS.append("%s: authorize hop stuck at Supabase: %s" % (page_path, text(pg.content().encode(), 160)))
                            else:
                                R.warn("   unexpected target %s ; recent console: %s" % (nav_target[:120], console[-3:]))
                                FINDINGS.append("%s: clicking Google did not navigate (target %s)" % (page_path, nav_target[:80]))
                    R.kv(section="S4_click", page=page_path, supabase_loaded=facts.get("supabaseLoaded"),
                         signin_btn=facts.get("signinBtn"), nav=(nav_target or "")[:80])
                except Exception as e:
                    R.warn("   %s: %s" % (page_path, str(e)[:160]))
                ctx.close()
                # return leg: fake ?code= -> supabase-js must call /auth/v1/token (expect 4xx for a fake code)
                ctx = browser.new_context(viewport={"width": 1440, "height": 1000}, user_agent=UA)
                pg = ctx.new_page()
                token_calls = []
                pg.on("response", lambda rs: token_calls.append((rs.status, rs.url[:100])) if "/auth/v1/token" in rs.url else None)
                sep = "&" if "?" in page_path else "?"
                try:
                    pg.goto(SITE + page_path + sep + "code=ops5171-fake-code", wait_until="domcontentloaded", timeout=45000)
                    pg.wait_for_timeout(6000)
                    R.log("   return leg %s -> token exchange calls: %s ; url now %s" % (page_path, token_calls, pg.url[:120]))
                    if not token_calls:
                        FINDINGS.append("%s: no /auth/v1/token exchange attempted on return (code stripped or supabase-js absent)" % page_path)
                    R.kv(section="S4_return", page=page_path, token_calls=len(token_calls),
                         first_status=(token_calls[0][0] if token_calls else None))
                except Exception as e:
                    R.warn("   return leg %s: %s" % (page_path, str(e)[:120]))
                ctx.close()
            browser.close()
    except Exception as e:
        R.warn("   headless stage unavailable: %s" % str(e)[:200])

    # ---------------------------------------------------------------- P2
    R.section("P2 keep-alive sentinel: justhodl-supabase-keepalive every 4 hours + first run")
    try:
        import boto3
        lam = boto3.client("lambda", region_name="us-east-1")
        sch = boto3.client("scheduler", region_name="us-east-1")
        arn = "arn:aws:lambda:us-east-1:857687956942:function:justhodl-supabase-keepalive"
        for _ in range(30):
            try:
                c = lam.get_function_configuration(FunctionName="justhodl-supabase-keepalive")
                if c.get("LastUpdateStatus") in (None, "Successful"):
                    break
            except Exception:
                pass
            time.sleep(20)
        resp = lam.invoke(FunctionName="justhodl-supabase-keepalive", InvocationType="RequestResponse", Payload=b"{}")
        out = json.loads(resp["Payload"].read() or b"{}")
        R.log("   first run -> %s" % json.dumps(out)[:300])
        if not out.get("up"):
            FINDINGS.append("keep-alive first run reports DOWN: %s" % json.dumps(out)[:120])
        tgt = {"Arn": arn, "RoleArn": "arn:aws:iam::857687956942:role/justhodl-scheduler-role", "Input": "{}",
               "RetryPolicy": {"MaximumRetryAttempts": 1}}
        try:
            sch.get_schedule(Name="justhodl-supabase-keepalive", GroupName="default")
            sch.update_schedule(Name="justhodl-supabase-keepalive", GroupName="default", ScheduleExpression="rate(4 hours)",
                                FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15}, Target=tgt, State="ENABLED",
                                Description="Supabase Free-plan keep-alive + reachability sentinel (ops 5173)")
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name="justhodl-supabase-keepalive", GroupName="default", ScheduleExpression="rate(4 hours)",
                                FlexibleTimeWindow={"Mode": "FLEXIBLE", "MaximumWindowInMinutes": 15}, Target=tgt, State="ENABLED",
                                Description="Supabase Free-plan keep-alive + reachability sentinel (ops 5173)")
        R.ok("   schedule justhodl-supabase-keepalive rate(4 hours) armed")
    except Exception as e:
        R.warn("   keep-alive: %s" % str(e)[:160])
        FINDINGS.append("keep-alive not armed: %s" % str(e)[:80])

    # ---------------------------------------------------------------- S5
    R.section("S5 verdict")
    if FINDINGS:
        for f in FINDINGS:
            R.fail("   " + f)
    else:
        R.ok("   GREEN: project restored, Google provider on, authorize hop reaches Google's account chooser, "
             "site wiring + CSP clean, return-leg exchange attempted, keep-alive armed")
    (ROOT / "aws" / "ops" / "reports" / "5173_google_login_verify.json").write_text(
        json.dumps({"findings": FINDINGS, "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, indent=1), encoding="utf-8")
    if FINDINGS:
        R.log("   RED: %d failing leg(s) listed above" % len(FINDINGS))
        sys.exit(1)
    R.ok("ops 5173 complete -- read-only")
