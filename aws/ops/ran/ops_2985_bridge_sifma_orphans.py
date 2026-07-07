#!/usr/bin/env python3
"""ops 2985 -- allocator<-compass BRIDGE live-verify + SIFMA (M7) live
attempt + dealer-gex 0DTE ruling (M10) + ORPHAN RECOMPUTE from first
principles (engine outs x live page scan x S3 freshness -- no stale
wiring docs), + audit stamps. Crash-proof report.
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


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def wait_fresh(fn, fails):
    for _ in range(50):
        cfg = LAM.get_function_configuration(FunctionName=fn)
        lm = datetime.fromisoformat(
            cfg["LastModified"].replace("+0000", "+00:00"))
        age = (datetime.now(timezone.utc) - lm).total_seconds()
        if cfg.get("LastUpdateStatus") == "Successful" and age < 1800:
            env_n = len((cfg.get("Environment") or {})
                        .get("Variables") or {})
            if env_n < 2:
                fails.append("%s env nuked (%d vars)" % (fn, env_n))
            return int(age)
        time.sleep(8)
    fails.append("%s: no fresh deploy" % fn)
    return None


def body(rep, fails, warns, hl):
    rep.section("0. Deploy gates (both lambdas)")
    time.sleep(75)
    rep.kv(gapmetrics_age=wait_fresh("justhodl-gap-metrics", fails),
           allocator_age=wait_fresh("justhodl-master-allocator", fails))
    if fails:
        return

    rep.section("1. SIFMA live (M7)")
    r = LAM.invoke(FunctionName="justhodl-gap-metrics", Payload=b"{}")
    if r.get("FunctionError"):
        fails.append("gap-metrics invoke: %s"
                     % r["Payload"].read()[:250])
        return
    sif = s3_json("data/sifma-issuance.json")
    rep.kv(sifma=json.dumps({k: sif.get(k) for k in
                             ("status", "reason", "ig_latest_bn",
                              "hy_latest_bn", "hy_share_pct",
                              "ig_yoy_pct", "hy_yoy_pct",
                              "workbook")})[:400])
    hl["sifma"] = {k: sif.get(k) for k in
                   ("status", "reason", "ig_latest_bn", "hy_latest_bn",
                    "hy_share_pct", "ig_yoy_pct", "hy_yoy_pct")}
    m7_shipped = sif.get("status") == "OK"
    idx = s3_json("data/gap-metrics.json")
    if "sifma_issuance" not in json.dumps(idx):
        fails.append("sifma module missing from gap-metrics index")

    rep.section("2. Allocator bridge live")
    r = LAM.invoke(FunctionName="justhodl-master-allocator",
                   Payload=b"{}")
    if r.get("FunctionError"):
        fails.append("allocator invoke: %s" % r["Payload"].read()[:250])
        return
    ma = s3_json("data/master-allocation.json")
    cb = ma.get("compass_bridge") or {}
    rep.kv(bridge=json.dumps(cb)[:400],
           summary=json.dumps(ma.get("summary"))[:200],
           total=round(sum((ma.get("target_allocation") or {})
                           .values()), 2))
    hl["compass_bridge"] = cb
    hl["alloc_summary"] = ma.get("summary")
    if not cb.get("used"):
        fails.append("compass bridge not used: %s" % json.dumps(cb)[:200])
    else:
        if abs(sum((ma.get("target_allocation") or {}).values())
               - 100.0) > 0.1:
            fails.append("allocation no longer sums to 100")
        contrib_txt = json.dumps(ma.get("contributions") or {})
        if "Asset Compass ER" not in contrib_txt:
            fails.append("compass contributions missing from rationale")

    rep.section("3. dealer-gex zero_dte content (M10 ruling)")
    dg = s3_json("data/dealer-gex.json")
    und = dg.get("underlyings") or {}
    first = und.get("SPY") or (list(und.values())[0] if und else {})
    zd = (first or {}).get("zero_dte")
    rep.kv(zero_dte=json.dumps(zd)[:400])
    hl["zero_dte"] = zd
    m10_covered = isinstance(zd, dict) and any(
        "share" in k or "pct" in k for k in zd)

    rep.section("4. Orphan recompute (first principles)")
    audit = s3_json("data/fleet-audit.json")
    eng = audit.get("engines") or {}
    wired_feeds = set()
    n_pages = 0
    for f in os.listdir(REPO):
        if not f.endswith(".html"):
            continue
        n_pages += 1
        try:
            txt = open(REPO / f, encoding="utf-8",
                       errors="replace").read()
        except Exception:
            continue
        wired_feeds.update(re.findall(r"data/[a-z0-9_.-]+\.json", txt))
    rep.kv(pages_scanned=n_pages, feeds_on_pages=len(wired_feeds))
    roster = []
    heads = 0
    for name, r_ in sorted(eng.items()):
        outs = [o for o in (r_.get("outs") or [])
                if isinstance(o, str) and o.startswith("data/")]
        if not outs:
            continue
        if any(o in wired_feeds for o in outs):
            continue
        age_h = None
        for o in outs[:2]:
            if heads >= 400:
                break
            heads += 1
            try:
                h = S3.head_object(Bucket=BUCKET, Key=o)
                a = (datetime.now(timezone.utc)
                     - h["LastModified"]).total_seconds() / 3600.0
                age_h = a if age_h is None else min(age_h, a)
            except Exception:
                continue
        roster.append({"name": name,
                       "family": r_.get("family") or "?",
                       "out": outs[0], "outs_n": len(outs),
                       "age_h": round(age_h, 1)
                       if age_h is not None else None})
    fresh = [x for x in roster if isinstance(x["age_h"], (int, float))
             and x["age_h"] <= 72]
    from collections import Counter
    fam_c = Counter(x["family"] for x in fresh)
    rep.kv(orphans_with_outs=len(roster), orphans_fresh=len(fresh),
           fresh_by_family=json.dumps(dict(fam_c.most_common())))
    hl["orphan_roster"] = roster
    hl["orphans_fresh_n"] = len(fresh)
    if len(roster) < 30:
        warns.append("orphan roster only %d -- verify" % len(roster))

    rep.section("5. Audit stamps")
    changed = 0
    for g in (audit.get("gap_matrix") or {}).get("gaps") or []:
        gid = g.get("id")
        if gid == "M7":
            g["status"] = "SHIPPED" if m7_shipped else "ATTEMPTED"
            g["note"] = (("SHIPPED via justhodl-gap-metrics "
                          "sifma_issuance -> data/sifma-issuance.json "
                          "on credit-desk.html") if m7_shipped else
                         ("ATTEMPTED ops 2985: scraper built + "
                          "deployed; live result %s (%s)"
                          % (hl["sifma"].get("status"),
                             str(hl["sifma"].get("reason"))[:80])))
            changed += 1
        if gid == "M10":
            if m10_covered:
                g["status"] = "COVERED"
                g["note"] = ("COVERED by justhodl-dealer-gex "
                             "underlyings.*.zero_dte: %s"
                             % json.dumps(zd)[:120])
            else:
                g["note"] = ("dealer-gex has zero_dte block but no "
                             "share metric yet: %s -- extension "
                             "candidate" % json.dumps(zd)[:100])
            changed += 1
    audit["u9_orphans_fresh_recount"] = len(fresh)
    audit["allocator_compass_bridge"] = {
        "status": "SHIPPED", "ops": 2985,
        "detail": hl.get("compass_bridge")}
    audit["ops"] = 2985
    S3.put_object(Bucket=BUCKET, Key="data/fleet-audit.json",
                  Body=json.dumps(audit, separators=(",", ":")
                                  ).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=900")
    rep.kv(gap_stamps=changed, m7_shipped=m7_shipped,
           m10_covered=m10_covered)
    hl["m7_shipped"] = m7_shipped
    hl["m10_covered"] = m10_covered


def main():
    fails, warns, hl = [], [], {}
    with report("2985_bridge_sifma_orphans") as rep:
        try:
            body(rep, fails, warns, hl)
        except Exception:
            fails.append("CRASH: " + traceback.format_exc()[-700:])
        out = {"ops": 2985, "fails": fails, "warns": warns,
               "verdict": "PASS" if not fails else "FAIL",
               "ts": datetime.now(timezone.utc).isoformat()}
        out.update(hl)
        rp = AWS_DIR / "ops" / "reports" / "2985.json"
        rp.write_text(json.dumps(out, indent=1))
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
