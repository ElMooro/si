#!/usr/bin/env python3
"""ops 3134 -- BLS DESK LANDING (finishes ops 3133, which FAILED in its
deploy gate). Root cause found in this session: deploy-lambdas.yml's UPDATE
path applies timeout/memory/env but NEVER --description (create-only), so
3133's gate -- which keyed on the new config.json description marker --
could never pass; it burned its full 10-min budget in section 1 and exited
before invoking anything. The bls-labor-agent CODE deployed fine at
02:19Z (job green, update-function-code + wait both succeeded); the feed
data/bls-employment.json was simply never generated, so justhodl.ai/bls.html
(already rewritten + Pages-deployed) had nothing to read.

This push: (a) deploy-lambdas.yml now applies --description on update too
(config.json description becomes authoritative fleet-wide), (b) config.json
runtime documented python3.12, (c) this script, which:
  1. gates on CODE truth (LastModified >= the 02:19Z deploy), not description
  2. probes the runner BLS_API_KEY against BLS v2, then syncs env (merge,
     never replace), Description marker, Runtime -> python3.12, in one call
  3. invokes async + polls S3 for a fresh data/bls-employment.json
  4. runs 3133's full content assertions + logs headline numbers
  5. legacy regression: data/bls-labor.json still publishing
  6. asserts the page's public feed URL serves the fresh doc; checks the
     live page cutover (warn-only -- CDN max-age=600 self-heals)
  7. retires the failed aws/ops/pending/ops_3133_bls_employment_desk.py to
     ran/ so its unpassable gate never re-fires
"""
import json
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report
from _lambda_deploy_helpers import _retry_on_conflict

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "bls-labor-agent"
EMP_KEY = "data/bls-employment.json"
LEGACY_KEY = "data/bls-labor.json"
PUBLIC_FEED = ("https://justhodl-dashboard-live.s3.amazonaws.com/"
               "data/bls-employment.json")
DEPLOY_LANDED_AFTER = datetime(2026, 7, 12, 2, 15, tzinfo=timezone.utc)
HERE = Path(__file__).resolve().parent          # aws/ops/pending
AWS_DIR = HERE.parents[1]                        # aws/
CFG = json.loads((AWS_DIR / "lambdas" / FN / "config.json").read_text())
UA = {"User-Agent": "Mozilla/5.0 ops-3134", "Cache-Control": "no-cache"}

HARD_CORE = ["unemployment_rate", "nonfarm_payrolls", "lfpr", "epop",
             "unemployed_level", "long_term_unemployed", "u1_rate", "u2_rate",
             "u4_rate", "u5_rate", "u6_rate", "ur_black", "ur_hispanic",
             "ur_teen", "jolts_openings", "jolts_quits_rate",
             "jolts_layoffs_rate", "ind_manufacturing", "ind_temp_help",
             "ahe_private", "awh_private"]


def get(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    return urllib.request.urlopen(req, timeout=timeout).read().decode(
        "utf-8", "replace")


def s3_json(key):
    body = S3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return json.loads(body)


def probe_bls_key(key):
    if not key:
        return False, "no BLS_API_KEY in runner env"
    payload = json.dumps({
        "seriesid": ["LNS14000000"], "startyear": "2025", "endyear": "2026",
        "registrationkey": key}).encode()
    req = urllib.request.Request(
        "https://api.bls.gov/publicAPI/v2/timeseries/data/", data=payload,
        headers={"Content-Type": "application/json",
                 "User-Agent": "justhodl-ops-3134"})
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=30).read())
        ok = d.get("status") == "REQUEST_SUCCEEDED" and \
            (d.get("Results") or {}).get("series")
        return bool(ok), d.get("status")
    except Exception as e:
        return False, str(e)[:100]


def main():
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    with report("3134_bls_desk_land") as rep:
        rep.section("1. Gate on CODE truth (LastModified), settle updates")
        cfg = None
        for _ in range(14):  # ~3.5 min: absorb this push's own redeploy
            cfg = LAM.get_function_configuration(FunctionName=FN)
            if (cfg.get("State") == "Active"
                    and cfg.get("LastUpdateStatus") == "Successful"):
                break
            time.sleep(15)
        lm = datetime.strptime(cfg["LastModified"],
                               "%Y-%m-%dT%H:%M:%S.%f%z")
        rep.kv(runtime=cfg.get("Runtime"), handler=cfg.get("Handler"),
               last_modified=cfg["LastModified"],
               timeout=cfg.get("Timeout"), memory=cfg.get("MemorySize"),
               code_sha=cfg.get("CodeSha256", "")[:12])
        if lm < DEPLOY_LANDED_AFTER:
            fails.append("new code never landed (LastModified %s)"
                         % cfg["LastModified"])
            _fin(rep, fails, warns)
            sys.exit(1)
        rep.log("code landed via deploy-lambdas -- gate PASS")

        rep.section("2. Key probe + env/description/runtime sync")
        runner_key = os.environ.get("BLS_API_KEY", "")
        key_ok, key_msg = probe_bls_key(runner_key)
        rep.row(check="runner BLS key valid on v2", ok=key_ok, value=key_msg)
        existing_env = (cfg.get("Environment") or {}).get("Variables") or {}
        merged = dict(existing_env)
        if key_ok:
            same = existing_env.get("BLS_API_KEY") == runner_key
            rep.log("function env BLS_API_KEY %s runner secret"
                    % ("==" if same else "!= -- syncing"))
            merged["BLS_API_KEY"] = runner_key
        else:
            warns.append("runner BLS secret failed probe (%s) -- leaving "
                         "function env untouched" % key_msg)
        kw = dict(FunctionName=FN, Timeout=180, MemorySize=512,
                  Description=CFG.get("description", ""),
                  Environment={"Variables": merged})
        try:
            _retry_on_conflict(LAM.update_function_configuration,
                               Runtime="python3.12", **kw)
            rep.log("config updated (runtime -> python3.12, desc, env)")
        except Exception as e:
            warns.append("runtime flip failed (%s) -- retrying w/o Runtime"
                         % str(e)[:90])
            _retry_on_conflict(LAM.update_function_configuration, **kw)
        LAM.get_waiter("function_updated").wait(
            FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})

        rep.section("3. Invoke async + poll S3 for fresh employment doc")
        LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        doc = None
        deadline = time.time() + 360
        while time.time() < deadline:
            try:
                d = s3_json(EMP_KEY)
                gen = datetime.fromisoformat(d["generated_at"])
                if gen >= t0:
                    doc = d
                    break
            except Exception:
                pass
            time.sleep(12)
        if doc is None:
            fails.append("employment doc never freshened in S3")
            _fin(rep, fails, warns)
            sys.exit(1)
        rep.log("fresh doc: generated_at=%s api=%s key_valid=%s" % (
            doc["generated_at"], doc.get("api_version"),
            doc.get("key_valid")))
        if not doc.get("key_valid"):
            warns.append("BLS key fell back to v1 (shorter history)")

        rep.section("4. Content assertions + headline numbers")
        nat = doc.get("national") or {}
        states = doc.get("states") or {}
        live_nat = [k for k, s in nat.items() if s.get("value") is not None]
        live_st = [k for k, s in states.items()
                   if s.get("value") is not None]
        rep.kv(national_live=len(live_nat), states_live=len(live_st))
        if len(live_nat) < 52:
            fails.append("national live %d < 52" % len(live_nat))
        if len(live_st) < 45:
            fails.append("states live %d < 45" % len(live_st))
        dead = [k for k, s in nat.items() if s.get("value") is None]
        for k in dead:
            (warns if k not in HARD_CORE else fails).append(
                "series dead: %s" % k)

        def v(k, f="value"):
            return (nat.get(k) or {}).get(f)

        ur, u6 = v("unemployment_rate"), v("u6_rate")
        pay, jol = v("nonfarm_payrolls"), v("jolts_openings")
        lfpr = v("lfpr")
        checks = [
            ("UNRATE sane", ur is not None and 2.0 < ur < 25.0, ur),
            ("U6 >= U3", (u6 is not None and ur is not None and u6 >= ur),
             u6),
            ("payrolls level sane (k)",
             pay is not None and 120000 < pay < 220000, pay),
            ("JOLTS openings sane (k)",
             jol is not None and 3000 < jol < 15000, jol),
            ("LFPR sane", lfpr is not None and 55 < lfpr < 70, lfpr),
        ]
        for label, ok, val in checks:
            rep.row(check=label, ok=ok, value=val)
            if not ok:
                fails.append("%s (got %s)" % (label, val))
        hist = (nat.get("unemployment_rate") or {}).get("history") or []
        need = 250 if doc.get("key_valid") else 90
        rep.row(check="UNRATE history depth", ok=len(hist) >= need,
                value=len(hist))
        if len(hist) < need:
            fails.append("UNRATE history %d < %d" % (len(hist), need))

        cr = doc.get("crisis") or {}
        ok_cr = (isinstance(cr.get("score"), int) and 0 <= cr["score"] <= 100
                 and cr.get("level") in ("STABLE", "WATCH", "WARNING",
                                         "CRISIS")
                 and cr.get("sahm") is not None
                 and len(cr.get("components") or []) == 8)
        rep.row(check="crisis engine", ok=ok_cr, value="%s/%s sahm=%s" % (
            cr.get("score"), cr.get("level"), cr.get("sahm")))
        if not ok_cr:
            fails.append("crisis engine malformed")
        rep.kv(unrate=ur, unrate_period=v("unemployment_rate", "period"),
               payrolls_mom_chg_k=v("nonfarm_payrolls", "mom_chg"),
               u6=u6, jolts_openings_k=jol,
               quits_rate=v("jolts_quits_rate"),
               temp_help_yoy_pct=v("ind_temp_help", "yoy_pct"),
               sahm=cr.get("sahm"), crisis="%s/%s" % (cr.get("score"),
                                                      cr.get("level")))

        rep.section("5. Legacy regression: bls-labor.json still publishing")
        try:
            leg = s3_json(LEGACY_KEY)
            lg = datetime.fromisoformat(leg["generated_at"])
            ok_leg = lg >= t0 and (leg.get("_series_live") or 0) >= 12
            rep.row(check="legacy doc fresh", ok=ok_leg,
                    value="live=%s" % leg.get("_series_live"))
            if not ok_leg:
                fails.append("legacy bls-labor.json stale/thin")
        except Exception as e:
            fails.append("legacy doc read: %s" % str(e)[:90])

        rep.section("6. Public feed URL + live page cutover")
        try:
            pub = json.loads(get(PUBLIC_FEED + "?cb=%d" % time.time()))
            ok_pub = datetime.fromisoformat(pub["generated_at"]) >= t0
            rep.row(check="public S3 feed URL fresh", ok=ok_pub,
                    value=pub.get("generated_at"))
            if not ok_pub:
                fails.append("public feed URL serves stale doc")
        except Exception as e:
            fails.append("public feed URL unreadable: %s" % str(e)[:90])
        page_ok = False
        for _ in range(8):
            try:
                pg = get("https://justhodl.ai/bls.html?cb=%d" % time.time(),
                         timeout=30)
                if "bls-employment.json" in pg and "tajry2f7v3" not in pg:
                    page_ok = True
                    break
            except Exception:
                pass
            time.sleep(20)
        rep.row(check="bls.html serves rebuilt page", ok=page_ok, value="")
        if not page_ok:
            warns.append("page cutover not visible yet (CDN max-age=600 "
                         "self-heals; repo + Pages deploy already verified)")

        rep.section("7. Retire the failed 3133 gate script")
        stale = HERE / "ops_3133_bls_employment_desk.py"
        if stale.exists():
            dest = AWS_DIR / "ops" / "ran" / stale.name
            if not dest.exists():
                os.replace(stale, dest)
                rep.log("moved %s -> ran/ (unpassable desc-gate superseded "
                        "by this op)" % stale.name)
        else:
            rep.log("already retired")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        for w in warns:
            rep.log("WARN: %s" % w)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3134.json").write_text(json.dumps(
        {"ops": 3134, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
