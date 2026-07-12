#!/usr/bin/env python3
"""ops 3133 -- BLS EMPLOYMENT DESK VERIFY (Khalid: justhodl.ai/bls.html was
dead -- 'No data received from BLS API'). Root cause: the page still called
the Aug-2025 API Gateway https://tajry2f7v3.execute-api... in front of the
retired nodejs bls-employment-api-v2, which carries the OLD INVALID BLS key.
Fix shipped on this same push: (1) bls-labor-agent extended -- keeps its
legacy data/bls-labor.json contract AND now publishes a comprehensive
data/bls-employment.json (core CPS/CES, U-1..U-6 all verified vs FRED,
9 demographics, 51 LAUS state rates, 10 JOLTS, 17 CES industries, hours &
earnings, per-indicator crisis thresholds + composite crisis engine w/ Sahm
rule) from history 2000+; (2) bls.html rewritten to the standard S3 pattern
(no client-side keys, no API Gateway). VERIFY-ONLY per AUTONOMY trap: the
push touches lambda source+config so deploy-lambdas.yml owns the deploy --
this script gates on config description landing + LastUpdateStatus, invokes
async, polls S3, asserts content, checks the live page cutover."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1")
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
FN = "bls-labor-agent"
EMP_KEY = "data/bls-employment.json"
LEGACY_KEY = "data/bls-labor.json"
NEW_DESC_MARK = "comprehensive employment crisis dataset"
AWS_DIR = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "Mozilla/5.0 ops-3133", "Cache-Control": "no-cache"}

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


def main():
    fails, warns = [], []
    t0 = datetime.now(timezone.utc)
    with report("3133_bls_employment_desk") as rep:
        rep.section("1. Gate: deploy-lambdas landed the new bundle")
        deployed = False
        for _ in range(40):  # up to ~10 min
            try:
                cfg = LAM.get_function_configuration(FunctionName=FN)
                if (NEW_DESC_MARK in (cfg.get("Description") or "")
                        and cfg.get("LastUpdateStatus") == "Successful"
                        and cfg.get("State") == "Active"):
                    deployed = True
                    rep.log("deployed: timeout=%ss mem=%sMB" % (
                        cfg.get("Timeout"), cfg.get("MemorySize")))
                    if cfg.get("Timeout", 0) < 180:
                        warns.append("timeout not applied (%s)" %
                                     cfg.get("Timeout"))
                    break
            except Exception as e:
                rep.log("cfg poll: %s" % str(e)[:80])
            time.sleep(15)
        if not deployed:
            fails.append("deploy gate: new config never landed")
            _fin(rep, fails, warns)
            sys.exit(1)
        time.sleep(10)  # settle per AUTONOMY LastModified trap

        rep.section("2. Invoke async + poll S3 for fresh employment doc")
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        deadline = time.time() + 330
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

        rep.section("3. Content assertions")
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

        rep.section("4. Legacy regression: bls-labor.json still publishing")
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

        rep.section("5. Live page cutover (pages.yml + CDN)")
        page_ok = False
        for _ in range(10):
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
            warns.append("page cutover not visible yet (CDN lag) -- "
                         "recheck manually next run")

        rep.section("verdict")
        _fin(rep, fails, warns)
        if fails:
            for f in fails:
                rep.log("FAIL: %s" % f)
            sys.exit(1)
        rep.log("PASS")


def _fin(rep, fails, warns):
    (AWS_DIR / "ops" / "reports" / "3133.json").write_text(json.dumps(
        {"ops": 3133, "verdict": "FAIL" if fails else "PASS",
         "fails": fails, "warns": warns,
         "ts": datetime.now(timezone.utc).isoformat()}, indent=1))
    rep.kv(verdict="FAIL" if fails else "PASS", n_fails=len(fails),
           n_warns=len(warns))


main()
sys.exit(0)
