#!/usr/bin/env python3
"""ops 2990 -- PARALLEL-SESSION RECONCILE (read-only). Two concurrent
sessions collided on ops 2984-2986; the other stream shipped U9
adoption, the allocator<-compass bridge, SIFMA M7, and an M10 ruling,
but its verify reports were overwritten by this stream's dump reports.
Independent verification: (1) allocator live doc carries compass_bridge
used:true with tilts; (2) gap-metrics live doc sifma module status;
(3) audit gap_matrix M7/M10/M21 + u9 recount + totals; (4) one adopted
page live with a sampled adopted feed in source. Numbering jumps to
2990 to clear the collision range.
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
from ops_report import report

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    return json.loads(S3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-2990",
                                               "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    fails, warns = [], []
    out = {"ops": 2990, "ts": datetime.now(timezone.utc).isoformat()}
    with report("2990_parallel_reconcile") as rep:

        rep.section("1. Allocator bridge live")
        alloc = None
        for k in ("data/master-allocator.json", "data/allocator.json",
                  "data/master-allocation.json"):
            try:
                alloc = s3_json(k)
                out["alloc_key"] = k
                break
            except Exception:
                continue
        if alloc is None:
            fails.append("allocator output doc not found")
        else:
            br = alloc.get("compass_bridge") or {}
            out["bridge"] = {kk: br.get(kk) for kk in
                             ("used", "spy_tlt_corr_90d",
                              "duration_hedge_gated", "sleeves_mapped",
                              "note")}
            out["bridge_tilts"] = br.get("tilts_pp")
            rep.kv(bridge=json.dumps(out["bridge"]))
            if not br.get("used"):
                warns.append("bridge present but used=false: %s"
                             % br.get("note"))
            if br.get("used") and not br.get("tilts_pp"):
                fails.append("bridge used but no tilts_pp")

        rep.section("2. SIFMA module live")
        gm = s3_json("data/gap-metrics.json")
        mods = gm.get("modules") or {}
        sif = mods.get("sifma") or mods.get("sifma_issuance") or {}
        out["sifma"] = {kk: sif.get(kk) for kk in
                        ("status", "reason", "note")}
        out["gap_modules_ok"] = sum(
            1 for m in mods.values()
            if isinstance(m, dict) and m.get("status") == "OK")
        rep.kv(sifma=json.dumps(out["sifma"]),
               modules_ok=out["gap_modules_ok"])
        if not sif:
            fails.append("sifma module absent from gap-metrics doc")
        elif sif.get("status") not in ("OK", "DEGRADED", "WARMING_UP"):
            fails.append("sifma status odd: %s" % sif.get("status"))

        rep.section("3. Audit stamps")
        audit = s3_json("data/fleet-audit.json")
        gaps = (audit.get("gap_matrix") or {}).get("gaps") or []
        out["gap_status"] = {g["id"]: (g.get("status") or g.get(
            "note", "")[:60]) for g in gaps
            if g.get("id") in ("M7", "M10", "M21")}
        out["u9_recount"] = audit.get("u9_orphans_fresh_recount")
        out["totals"] = audit.get("totals")
        rep.kv(gap_status=json.dumps(out["gap_status"]),
               u9=out["u9_recount"])
        if out["u9_recount"] not in (0, None):
            warns.append("u9 recount nonzero: %s" % out["u9_recount"])

        rep.section("4. Adopted page live sample")
        ok = False
        for _ in range(6):
            try:
                st, html = get("https://justhodl.ai/crypto-risk.html"
                               "?v=%d" % int(time.time()))
                n = html.count("data/")
                ok = st == 200 and n >= 10
                out["crypto_risk_feed_refs"] = n
                if ok:
                    break
            except Exception:
                pass
            time.sleep(10)
        rep.kv(adopted_page_ok=ok)
        if not ok:
            fails.append("crypto-risk.html live missing adopted feeds")

        out["fails"], out["warns"] = fails, warns
        out["verdict"] = "PASS" if not fails else "FAIL"
        (AWS_DIR / "ops" / "reports" / "2990.json").write_text(
            json.dumps(out, indent=1))
        rep.log("FAILS=%d WARNS=%d" % (len(fails), len(warns)))
        if fails:
            sys.exit(1)


main()
sys.exit(0)
