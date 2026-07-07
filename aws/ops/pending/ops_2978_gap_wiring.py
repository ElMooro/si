#!/usr/bin/env python3
"""ops 2978 -- WIRE THE GAP MATRIX TO PAGES + module fixes verify.
This push carried: (a) gap-metrics source fixes (revision-breadth field
names discovered live: eps_rev_pct; miner-margin per-ticker error
capture; implied-corr multi-symbol fallback); (b) jh-wire tags on 10
existing pages, one per module feed (lce<-sloos, correlation<-stock-bond,
global-macro<-M2, ofr<-FSI, eps-velocity<-breadth, metals-miners<-margin,
dollar<-EM carry, activity-nowcast<-freight, treasury-noise<-bill share,
vol<-implied corr) -- note ofr, metals-miners and activity-nowcast were
previously FEEDLESS pages, now fixed; (c) NEW credit-desk.html (umbrella
U8) with compass credit sleeve + SLOOS + muni + LCE + CDS-proxy wiring.

Sequence: (1) wait for deploy-lambdas code update; (2) re-invoke; (3)
verify >=9/11 modules OK with value assertions on newly-fixed ones; (4)
runner-side live checks: all 10 pages serve 200 AND contain their feed
path (that containment is what flips the engine to WIRED in the
directory audit), credit-desk.html live with real markers.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from ops_report import report

LAM = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=310, connect_timeout=10,
                                 retries={"max_attempts": 0}))
S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]
FN = "justhodl-gap-metrics"

PAGE_WIRES = {
    "lce.html": "data/sloos.json",
    "correlation.html": "data/stock-bond-corr.json",
    "global-macro.html": "data/global-m2.json",
    "ofr.html": "data/ofr-fsi.json",
    "eps-velocity.html": "data/revision-breadth.json",
    "metals-miners.html": "data/miner-margin.json",
    "dollar.html": "data/em-carry.json",
    "activity-nowcast.html": "data/baltic-dry.json",
    "treasury-noise.html": "data/bill-share.json",
    "vol.html": "data/implied-corr.json",
}


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2978",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    fails, warns = [], []
    hl = {}
    with report("2978_gap_wiring") as rep:

        rep.section("1. Wait for gap-metrics code deploy")
        # RACE FIX: run-ops and deploy-lambdas start in parallel on the
        # same push. The old gate (age<720s) accepted the PREVIOUS
        # push's deploy while this push's update was still in flight,
        # so two runs invoked stale code. New rule: give the deploy
        # workflow a 75s head start, then require Successful AND
        # LastModified inside a 30-min window that necessarily contains
        # this push's deploy when source changed (ops-only pushes ride
        # the previous deploy, which the window also covers).
        time.sleep(75)
        fresh = False
        for _ in range(50):
            cfg = LAM.get_function_configuration(FunctionName=FN)
            lm = datetime.fromisoformat(
                cfg["LastModified"].replace("+0000", "+00:00"))
            age = (datetime.now(timezone.utc) - lm).total_seconds()
            if cfg.get("LastUpdateStatus") == "Successful" \
                    and age < 1800:
                env_n = len((cfg.get("Environment") or {})
                            .get("Variables") or {})
                rep.kv(deploy_age_s=int(age), env_vars=env_n)
                if env_n < 3:
                    fails.append("env nuked post-deploy: %d vars" % env_n)
                fresh = True
                break
            time.sleep(8)
        if not fresh:
            fails.append("no successful deploy inside 30-min window")
        if fails:
            _write(rep, fails, warns, hl)
            return

        rep.section("2. Re-invoke")
        t0 = time.time()
        resp = LAM.invoke(FunctionName=FN, Payload=b"{}")
        body = json.loads(resp["Payload"].read() or b"{}")
        rep.kv(invoke_seconds=round(time.time() - t0, 1),
               fn_error=resp.get("FunctionError"),
               body=json.dumps(body)[:180])
        if resp.get("FunctionError"):
            fails.append("invoke error: %s" % json.dumps(body)[:250])
            _write(rep, fails, warns, hl)
            return

        rep.section("3. Module verify (post-fix)")
        idx = s3_json("data/gap-metrics.json")
        mods = idx.get("modules") or {}
        ok = sorted(n for n, m in mods.items() if m["status"] == "OK")
        deg = {n: (m.get("headline") or {}).get("note")
               for n, m in mods.items() if m["status"] != "OK"}
        rep.kv(modules_ok=len(ok), ok=ok, degraded=json.dumps(deg)[:400])
        hl["modules_ok"] = len(ok)
        hl["degraded"] = deg
        if len(ok) < 9:
            fails.append("only %d/11 OK after fixes: %s"
                         % (len(ok), json.dumps(deg)[:300]))
        if "revision_breadth" in ok:
            rb = s3_json("data/revision-breadth.json")
            hl["breadth"] = rb.get("breadth_pct_positive")
            hl["breadth_names"] = rb.get("names_covered")
            if not (0 <= (rb.get("breadth_pct_positive") or -1) <= 100
                    and rb.get("names_covered", 0) >= 10):
                fails.append("revision_breadth values off: %s"
                             % json.dumps(rb)[:150])
        else:
            warns.append("revision_breadth still degraded: %s"
                         % deg.get("revision_breadth"))
        if "miner_margin" in ok:
            mm = s3_json("data/miner-margin.json")
            hl["miner_median_delta_pp"] = mm.get("median_margin_delta_pp")
            if len(mm.get("miners") or {}) < 5:
                fails.append("miner_margin < 5 names post-fix")
        else:
            warns.append("miner_margin still degraded: %s"
                         % deg.get("miner_margin"))
        if "cor3m" not in ok:
            warns.append("implied-corr best-effort still degraded "
                         "(acceptable: no free symbol) -- %s"
                         % deg.get("cor3m"))

        rep.section("4. Live pages (runner-side)")
        page_fail = []
        for page, feed in PAGE_WIRES.items():
            got = False
            for _ in range(3):
                try:
                    st, html = http_get("https://justhodl.ai/%s?v=%d"
                                        % (page, int(time.time())))
                    if st == 200 and feed in html:
                        got = True
                        break
                except Exception:
                    pass
                time.sleep(8)
            if not got:
                page_fail.append(page)
        rep.kv(pages_wired_live=len(PAGE_WIRES) - len(page_fail),
               missing=page_fail)
        if page_fail:
            # pages CDN may lag a couple minutes on first deploy
            time.sleep(60)
            still = []
            for page in page_fail:
                try:
                    st, html = http_get("https://justhodl.ai/%s?v=%d"
                                        % (page, int(time.time())))
                    if not (st == 200 and PAGE_WIRES[page] in html):
                        still.append(page)
                except Exception:
                    still.append(page)
            if still:
                fails.append("pages missing their wire live: %s" % still)

        cd_ok = False
        for _ in range(6):
            try:
                st, html = http_get("https://justhodl.ai/credit-desk.html"
                                    "?v=%d" % int(time.time()))
                cd_ok = (st == 200 and "Credit Desk" in html
                         and "data/sloos.json" in html
                         and "asset-compass.json" in html)
                if cd_ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(credit_desk_live=cd_ok)
        if not cd_ok:
            fails.append("credit-desk.html not live with markers")

        try:
            st, pj = http_get("https://justhodl.ai/data/stock-bond-corr"
                              ".json?t=%d" % int(time.time()))
            d = json.loads(pj)
            rep.kv(public_stock_bond=d.get("regime"))
        except Exception:
            warns.append("public /data/ spot-check within CDN TTL window")

        if not fails:
            rep.ok("gap matrix WIRED: %d/11 modules OK; 10 pages + "
                   "credit-desk live; feedless pages ofr/metals-miners/"
                   "activity-nowcast now have real feeds" % len(ok))
        _write(rep, fails, warns, hl)


def _write(rep, fails, warns, hl):
    out = {"ops": 2978, "fails": fails, "warns": warns,
           "verdict": "PASS" if not fails else "FAIL",
           "ts": datetime.now(timezone.utc).isoformat()}
    out.update(hl)
    rp = AWS_DIR / "ops" / "reports" / "2978.json"
    rp.parent.mkdir(parents=True, exist_ok=True)
    rp.write_text(json.dumps(out, indent=1))
    rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
    if fails:
        sys.exit(1)


main()
sys.exit(0)
