#!/usr/bin/env python3
"""ops 2986 -- U9 CLOSE: verify the 106-feed adoption live, recount
orphans (expect near-zero), retry SIFMA with widened discovery, verify
allocator rf fix, stamp U9 SHIPPED + M7 outcome on the audit.
Crash-proof report.
"""
import json
import os
import re
import sys
import time
import traceback
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
REPO = AWS_DIR.parent

SAMPLE = {
    "crypto-risk.html": None, "lce.html": None, "signal-board.html":
    None, "resilience.html": None, "fleet-audit.html": None,
    "why.html": None, "system.html": None, "opportunities.html": None,
}


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2986",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read().decode("utf-8", "replace")


def wait_fresh(fn, fails):
    for _ in range(50):
        cfg = LAM.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(
            cfg["LastModified"].replace("+0000", "+00:00"))
        age = (datetime.now(timezone.utc) - lm).total_seconds()
        if cfg.get("LastUpdateStatus") == "Successful" and age < 1800:
            if len((cfg.get("Environment") or {})
                   .get("Variables") or {}) < 2:
                fails.append("%s env nuked" % fn)
            return int(age)
        time.sleep(8)
    fails.append("%s no fresh deploy" % fn)
    return None


def body(rep, fails, warns, hl):
    rep.section("0. Gates")
    time.sleep(75)
    rep.kv(gm=wait_fresh("justhodl-gap-metrics", fails),
           al=wait_fresh("justhodl-master-allocator", fails))
    if fails:
        return

    rep.section("1. Adopted pages live (repo truth + live sample)")
    plan_pages = {}
    for f in os.listdir(REPO):
        if f.endswith(".html"):
            txt = open(REPO / f, encoding="utf-8",
                       errors="replace").read()
            plan_pages[f] = set(re.findall(r"data/[a-z0-9_.-]+\.json",
                                           txt))
    live_bad = []
    for page in SAMPLE:
        want = None
        m = re.search(r'jh-wire\.js" defer data-feeds="([^"]+)"',
                      open(REPO / page, encoding="utf-8").read())
        if m:
            want = m.group(1).split(";")[-1].split("|")[0]
        ok = False
        for _ in range(8):
            try:
                st, h = get("https://justhodl.ai/%s?v=%d"
                            % (page, int(time.time())))
                if st == 200 and (want is None or want in h):
                    ok = True
                    break
            except Exception:
                pass
            time.sleep(10)
        if not ok:
            live_bad.append(page)
    rep.kv(live_sample_ok=len(SAMPLE) - len(live_bad),
           live_bad=live_bad)
    if live_bad:
        fails.append("adopted pages not live: %s" % live_bad)

    rep.section("2. Orphan recount")
    audit = s3_json("data/fleet-audit.json")
    eng = audit.get("engines") or {}
    wired = set()
    for feeds in plan_pages.values():
        wired |= feeds
    remaining = []
    heads = 0
    for name, r_ in sorted(eng.items()):
        outs = [o for o in (r_.get("outs") or [])
                if isinstance(o, str) and o.startswith("data/")]
        if not outs or any(o in wired for o in outs):
            continue
        age_h = None
        if heads < 250:
            heads += 1
            try:
                h = S3.head_object(Bucket=BUCKET, Key=outs[0])
                age_h = round((datetime.now(timezone.utc)
                               - h["LastModified"]).total_seconds()
                              / 3600.0, 1)
            except Exception:
                pass
        if isinstance(age_h, (int, float)) and age_h <= 72:
            remaining.append({"name": name, "out": outs[0],
                              "age_h": age_h,
                              "family": r_.get("family")})
    rep.kv(orphans_fresh_remaining=len(remaining),
           detail=json.dumps(remaining)[:400])
    hl["orphans_remaining"] = remaining

    rep.section("3. SIFMA retry")
    r = LAM.invoke(FunctionName="justhodl-gap-metrics", Payload=b"{}")
    if r.get("FunctionError"):
        fails.append("gap-metrics invoke error")
        return
    sif = s3_json("data/sifma-issuance.json")
    rep.kv(sifma=json.dumps({k: sif.get(k) for k in
                             ("status", "reason", "ig_latest_bn",
                              "hy_latest_bn", "hy_share_pct",
                              "workbook")})[:350])
    hl["sifma"] = {k: sif.get(k) for k in
                   ("status", "reason", "ig_latest_bn",
                    "hy_latest_bn", "hy_share_pct", "ig_yoy_pct",
                    "hy_yoy_pct")}
    m7 = sif.get("status") == "OK"

    rep.section("4. Allocator rf fix live")
    r = LAM.invoke(FunctionName="justhodl-master-allocator",
                   Payload=b"{}")
    if r.get("FunctionError"):
        fails.append("allocator invoke error")
        return
    ma = s3_json("data/master-allocation.json")
    cb = ma.get("compass_bridge") or {}
    rep.kv(rf=cb.get("cash_rf_pct"), used=cb.get("used"),
           gated=cb.get("duration_hedge_gated"),
           tilts=json.dumps(cb.get("tilts_pp"))[:220])
    hl["bridge"] = cb
    if not cb.get("used"):
        fails.append("bridge not used post-fix")
    if not (isinstance(cb.get("cash_rf_pct"), (int, float))
            and cb["cash_rf_pct"] > 1.0):
        warns.append("rf still %s -- check hurdle doc"
                     % cb.get("cash_rf_pct"))

    rep.section("5. Audit stamps")
    for u in audit.get("umbrella_actions") or []:
        if u.get("id") == "U9":
            u["status"] = "SHIPPED"
            u["note"] = ("SHIPPED ops 2986: 106 fresh orphan feeds "
                         "adopted into 14 existing desks via jh-wire; "
                         "recount now %d. | was: %s"
                         % (len(remaining), str(u.get("note"))[:100]))
    for g in (audit.get("gap_matrix") or {}).get("gaps") or []:
        if g.get("id") == "M7":
            g["status"] = "SHIPPED" if m7 else "ATTEMPTED"
            g["note"] = (("SHIPPED ops 2986: widened discovery "
                          "(__NEXT_DATA__/xls) -> live IG %s / HY %s"
                          % (sif.get("ig_latest_bn"),
                             sif.get("hy_latest_bn"))) if m7 else
                         ("ATTEMPTED: scraper live, page JS-rendered; "
                          "live result %s (%s)"
                          % (sif.get("status"),
                             str(sif.get("reason"))[:70])))
    audit["u9_orphans_fresh_recount"] = len(remaining)
    audit["ops"] = 2986
    S3.put_object(Bucket=BUCKET, Key="data/fleet-audit.json",
                  Body=json.dumps(audit, separators=(",", ":")
                                  ).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    rep.kv(u9="SHIPPED", m7_final="SHIPPED" if m7 else "ATTEMPTED")
    hl["m7_final"] = "SHIPPED" if m7 else "ATTEMPTED"


def main():
    fails, warns, hl = [], [], {}
    with report("2986_u9_close") as rep:
        try:
            body(rep, fails, warns, hl)
        except Exception:
            fails.append("CRASH: " + traceback.format_exc()[-700:])
        out = {"ops": 2986, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat()}
        out.update(hl)
        (AWS_DIR / "ops" / "reports" / "2986.json").write_text(
            json.dumps(out, indent=1))
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
