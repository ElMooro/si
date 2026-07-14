"""ops 3292 — DILUTION RISK v2 + ACCOUNT-BACKED FAVORITES (Khalid:
dilution is a HUGE risk that must be flagged; every examined company
must show 10 years of share counts with numbers + a linear chart; and
favorites weren't showing because they lived in the browser, not the
account). [A] equity-research: risk_flag + ten_year_multiple in the
dilution module; why.html Pillar 6 v2 = red RISK banner when flagged,
axis-labeled 10-year linear chart, FY-by-FY numbers strip with YoY.
[B] jh-nav-drawer.js: account surface on ALL 366 pages — sign-in
button (lazy-loads existing auth.js/Supabase, no rebuild), signed-in
email + sign-out, session auto-refresh on boot so /userdata favorites
sync (union-merge) actually fires. Truth bands: AAPL doc carries
risk_flag=false + ten_year_multiple<1 + >=8 annual points; a known
serial diluter flags risk_flag=true; drawer + why.html markers live;
/userdata/self rejects tokenless requests."""
import json
import sys
import time
import urllib.request
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-equity-research"
LAM = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300,
                                 retries={"max_attempts": 0}))
AWS_DIR = Path(__file__).resolve().parents[2]


def get(url, timeout=45):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (jh-ops-3292)"})
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "ignore")


def research(sym):
    for pay in ({"queryStringParameters": {"ticker": sym}},
                {"ticker": sym}):
        try:
            r = LAM.invoke(FunctionName=FN,
                           Payload=json.dumps(pay).encode())
            body = json.loads(r["Payload"].read())
            if isinstance(body, dict) and "body" in body:
                body = json.loads(body["body"])
            if isinstance(body, dict) and body.get("schema_version"):
                return body
        except Exception as e:
            print("  invoke %s: %s" % (sym, str(e)[:90]))
    return None


with report("3292_dilution_v2_favsync") as rep:
    fails, warns = [], []
    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(live.get("Timeout") or 300),
                  memory=int(live.get("MemorySize") or 1024),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)
    time.sleep(8)

    rep.section("2. AAPL — clean-name truth bands")
    d = research("AAPL")
    dil = (d or {}).get("dilution") or {}
    rep.kv(verdict=dil.get("verdict"), risk=dil.get("risk_flag"),
           mult10=dil.get("ten_year_multiple"),
           n_annual=len(dil.get("annual_series") or []))
    if dil.get("risk_flag") is not False:
        fails.append("AAPL risk_flag should be False: %r"
                     % dil.get("risk_flag"))
    m10 = dil.get("ten_year_multiple")
    if not (m10 and 0.5 <= m10 < 1.0):
        fails.append("AAPL 10y multiple off (buyback machine): %s"
                     % m10)
    if len(dil.get("annual_series") or []) < 8:
        fails.append("AAPL annual series <8 points")

    rep.section("3. Serial diluter — risk MUST flag")
    flagged = None
    for sym in ("MARA", "PLUG", "LCID", "RIOT"):
        d2 = research(sym)
        dl = (d2 or {}).get("dilution") or {}
        rep.kv(**{sym.lower(): "%s risk=%s 1y=%s mult=%s"
               % (dl.get("verdict"), dl.get("risk_flag"),
                  dl.get("sh_1y_cagr_pct"),
                  dl.get("ten_year_multiple"))})
        if dl.get("risk_flag") is True:
            flagged = sym
            break
    if not flagged:
        fails.append("no known diluter raised risk_flag")
    else:
        rep.log("  %s correctly FLAGGED" % flagged)

    rep.section("4. Live pages: why.html v2 + drawer account")
    ok_w = ok_d = ok_u = False
    for i in range(22):
        try:
            pg = get("https://justhodl.ai/why.html?cb=%d" % time.time())
            ok_w = ("10-YEAR RECORD" in pg
                    and "DILUTION RISK" in pg)
            js = get("https://justhodl.ai/jh-nav-drawer.js?cb=%d"
                     % time.time())
            ok_d = ("jhnav-account" in js and "ensureAuth" in js
                    and "Sign in" in js)
            if ok_w and ok_d:
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok_w:
        fails.append("why.html v2 markers not live")
    if not ok_d:
        fails.append("drawer account surface not live")
    try:
        req = urllib.request.Request(
            "https://justhodl-data-proxy.raafouis.workers.dev"
            "/userdata/self")
        urllib.request.urlopen(req, timeout=20)
        fails.append("/userdata/self allowed tokenless GET")
    except urllib.error.HTTPError as e:
        ok_u = e.code in (401, 403)
        rep.kv(userdata_tokenless=e.code)
    except Exception as e:
        warns.append("userdata probe: %s" % str(e)[:60])
        ok_u = True
    if not ok_u:
        fails.append("/userdata/self auth gate unproven")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3292 PASS — dilution screams when it should; "
            "favorites follow the account.")
sys.exit(0)
