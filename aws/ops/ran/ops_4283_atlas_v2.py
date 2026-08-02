"""
ops_4283 -- Atlas v2: nested stores in, dormant sixty triaged and
switch-on wave 1 fired.

v1 scanned top-level data/*.json only; the crypto fleet (and every
alert router) lives in subfolders. v2: (1) discover data/<dir>/
prefixes, scan signal-bearing ones (skip history/archive/date-stamped
keys), merge into the atlas -- crypto/ entries force the Crypto
family; (2) triage all 60 dormant targets by reading each writer's
code around the Key= usage -- keys written inside alert/except/
state-checkpoint branches are LAZY_EVENT (empty-until-event is
CORRECT, not dormant alpha), main-path keys become invoke candidates;
(3) fire wave 1: sync-invoke up to 10 candidate writers and re-check
materialization. Every disposition lands in the atlas doc.
"""
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=55, retries={"max_attempts": 1}))
NOW = datetime.now(timezone.utc)

SKIP_DIR = re.compile(r"histor|archive|snapshot|backup|hourly|daily|"
                      r"weekly|days|weeks|raw|cache$", re.I)
SKIP_FILE = re.compile(r"20\d\d[-_]|[-_]history|[-_]archive|snapshot",
                       re.I)
SIGNALY = {"score", "conviction", "verdict", "signal", "rank", "fit",
           "rating", "opportunity", "setup", "alert", "z", "zscore",
           "percentile", "upside", "er", "expected_return", "grade",
           "status", "flag", "pressure", "level", "band", "risk",
           "mvrv", "nupl", "phase"}
NAMEY = ("ticker", "symbol", "name", "label", "asset", "pair", "key",
         "industry", "sector", "class", "id", "title", "coin", "chain")

def looks_actionable(rows):
    if not rows or not isinstance(rows[0], dict):
        return None
    f0 = set(rows[0])
    if not (f0 & SIGNALY):
        return None
    return next((n for n in NAMEY if n in f0), None), sorted(
        f0 & SIGNALY)[:3]

def top_signal(doc, depth=0, path="$"):
    if depth > 3:
        return None
    if isinstance(doc, list) and len(doc) >= 3:
        hit = looks_actionable(doc[:5])
        if hit:
            namef, sigf = hit
            row = doc[0]
            samp = {("name" if namef else "row"):
                    str(row.get(namef, ""))[:26] if namef else "-"}
            for sf in sigf:
                v = row.get(sf)
                samp[sf] = (round(v, 3) if isinstance(v, float)
                            else str(v)[:22])
            return path, len(doc), samp
        for i, x in enumerate(doc[:4]):
            rr = top_signal(x, depth + 1, "%s[%d]" % (path, i))
            if rr:
                return rr
    if isinstance(doc, dict):
        # crypto engines often key by COIN at top with signal dicts
        f0keys = list(doc)[:400]
        coinish = [k for k in f0keys
                   if re.fullmatch(r"[A-Z]{2,8}", str(k))]
        if len(coinish) >= 8 and isinstance(doc[coinish[0]], dict) \
                and set(doc[coinish[0]]) & SIGNALY:
            row = doc[coinish[0]]
            sigf = sorted(set(row) & SIGNALY)[:3]
            samp = {"name": coinish[0]}
            for sf in sigf:
                v = row.get(sf)
                samp[sf] = (round(v, 3) if isinstance(v, float)
                            else str(v)[:22])
            return path + ".<COIN>", len(coinish), samp
        for k in f0keys[:36]:
            rr = top_signal(doc[k], depth + 1, "%s.%s" % (path, k))
            if rr:
                return rr
    return None

fails = []
with report("4283_atlas_v2") as r:
    r.heading("ops 4283 -- Atlas v2: nested stores + dormant triage + "
              "switch-on wave 1")

    # ── 1. nested prefix census + scan ──
    r.section("1. nested stores")
    prefixes = {}
    for pg in s3.get_paginator("list_objects_v2").paginate(
            Bucket=BUCKET, Prefix="data/", Delimiter="/"):
        for cp in pg.get("CommonPrefixes", []):
            prefixes[cp["Prefix"]] = 0
    keep = []
    for pref in sorted(prefixes):
        sub = pref[len("data/"):].strip("/")
        if SKIP_DIR.search(sub):
            continue
        keep.append(pref)
    r.log("subdirs: %d total, %d signal-candidates: %s"
          % (len(prefixes), len(keep),
             [p[len("data/"):] for p in keep][:20]))
    nested_entries = []
    scanned_n = 0
    for pref in keep:
        objs = []
        for pg in s3.get_paginator("list_objects_v2").paginate(
                Bucket=BUCKET, Prefix=pref):
            for o in pg.get("Contents", []):
                k = o["Key"]
                if not k.endswith(".json") or SKIP_FILE.search(
                        k.rsplit("/", 1)[-1]):
                    continue
                if o["Size"] > 2_500_000 or o["Size"] < 40:
                    continue
                objs.append((k, o["LastModified"], o["Size"]))
        objs.sort(key=lambda x: x[1], reverse=True)
        for k, lm, size in objs[:60]:
            if scanned_n >= 420:
                break
            try:
                doc = json.loads(s3.get_object(
                    Bucket=BUCKET, Key=k)["Body"].read())
            except Exception:
                continue
            scanned_n += 1
            ts = top_signal(doc)
            if not ts:
                continue
            path, n, samp = ts
            fam = ("Crypto Cycle & On-Chain" if "/crypto/" in k
                   else "Alerts & Routing" if "/_alerts/" in k
                   else "Nested Signals")
            nested_entries.append({
                "artifact": k,
                "age_h": round((NOW - lm).total_seconds() / 3600, 1),
                "signal_path": path, "n_signals": n, "top_now": samp})
    by_fam = {}
    for e in nested_entries:
        by_fam.setdefault(
            "Crypto Cycle & On-Chain" if "/crypto/" in e["artifact"]
            else "Alerts & Routing" if "/_alerts/" in e["artifact"]
            else "Nested Signals", []).append(e)
    r.ok("nested scan: %d docs -> %d opportunity artifacts (%s)"
         % (scanned_n, len(nested_entries),
            {k: len(v) for k, v in by_fam.items()}))
    for fam, rows in by_fam.items():
        rows.sort(key=lambda x: x["age_h"])
        for e in rows[:6]:
            r.kv(fam=fam[:10], artifact=e["artifact"][5:45],
                 age_h=e["age_h"],
                 top=json.dumps(e["top_now"])[:78])

    # ── 2. dormant triage ──
    r.section("2. dormant triage (writer-code classification)")
    try:
        atlas = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/alpha-atlas.json")["Body"].read())
    except Exception as e:
        atlas = {}
        fails.append("atlas v1 read: %s" % str(e)[:80])
    dorm = atlas.get("dormant") or []
    triage = []
    LAZY = re.compile(r"\b(if|except)\b.{0,120}?(alert|fired|error|"
                      r"state|checkpoint|first|miss|new_)", re.I | re.S)
    for d in dorm:
        tgt = d["target"]
        wr = (d.get("writers") or [None])[0]
        cls, ctx = "NO_WRITER_SRC", ""
        if wr:
            sp = "aws/lambdas/%s/source/lambda_function.py" % wr
            if os.path.exists(sp):
                srct = open(sp, encoding="utf-8",
                            errors="ignore").read()
                i = srct.find(tgt)
                if i < 0:
                    i = srct.find(tgt.split("/")[-1])
                if i >= 0:
                    ctx = srct[max(0, i - 900):i + 200]
                    cls = ("LAZY_EVENT" if LAZY.search(ctx)
                           else "MAIN_PATH")
                else:
                    cls = "KEY_VIA_VAR"
        triage.append({"target": tgt, "writer": wr, "class": cls})
    counts = {}
    for t in triage:
        counts[t["class"]] = counts.get(t["class"], 0) + 1
    r.ok("triage of %d dormant targets: %s" % (len(triage), counts))

    # ── 3. switch-on wave 1 ──
    r.section("3. switch-on wave 1 (MAIN_PATH writers, cap 10)")
    cands = [t for t in triage if t["class"] == "MAIN_PATH"
             and t["writer"]]
    seen_w, fired = set(), []
    for t in cands:
        if len(fired) >= 10 or t["writer"] in seen_w:
            continue
        seen_w.add(t["writer"])
        try:
            p = lam.invoke(FunctionName=t["writer"],
                           InvocationType="RequestResponse",
                           Payload=b"{}")
            err = p.get("FunctionError")
            fired.append((t, err))
            r.log("invoked %s%s" % (t["writer"],
                                    " FN-ERROR" if err else ""))
        except Exception as e:
            fired.append((t, str(e)[:60]))
            r.log("invoke %s: %s" % (t["writer"], str(e)[:70]))
    time.sleep(4)
    lit = 0
    for t, err in fired:
        try:
            s3.head_object(Bucket=BUCKET, Key=t["target"])
            t["result"] = "MATERIALIZED"
            lit += 1
            r.ok("LIT: %s (by %s)" % (t["target"], t["writer"]))
        except Exception:
            t["result"] = "still_absent%s" % (
                " (fn_err)" if err else "")
    r.log("wave 1: %d invoked, %d targets materialized"
          % (len(fired), lit))

    # ── 4. atlas v2 write ──
    fams = atlas.get("families") or []
    fams = [f for f in fams
            if f["family"] not in by_fam]
    for fam, rows in by_fam.items():
        fams.append({"family": fam, "n": len(rows), "engines": rows})
    fams.sort(key=lambda f: -f["n"])
    tot = atlas.get("totals") or {}
    tot.update(nested_scanned=scanned_n,
               nested_opportunity_artifacts=len(nested_entries),
               opportunity_engines=(tot.get("opportunity_engines") or 0)
               + len(nested_entries),
               families=len(fams),
               dormant_lazy=counts.get("LAZY_EVENT", 0),
               dormant_true=counts.get("MAIN_PATH", 0),
               wave1_lit=lit)
    atlas.update(version=2, generated_at=NOW.isoformat(
        timespec="seconds"), families=fams, totals=tot,
        dormant_triage=triage)
    s3.put_object(Bucket=BUCKET, Key="data/alpha-atlas.json",
                  Body=json.dumps(atlas, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    r.ok("ATLAS v2 published: %d engines / %d families; dormant split "
         "lazy=%s true=%s; wave1 lit=%d"
         % (tot["opportunity_engines"], len(fams),
            tot["dormant_lazy"], tot["dormant_true"], lit))

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4283 PASS")
if fails:
    sys.exit(1)
