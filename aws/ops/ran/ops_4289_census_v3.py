"""
ops_4289 -- census v3 (put-proximity writers) + attribution widened.

Two corrections earned by evidence this session:
  A. Writer maps counted get_object keys as writers (morning-intel's
     "writer" was its reader). v3 anchors attribution on Key literals/
     consts within put_object proximity ONLY, regenerates the writer
     maps inside BOTH atlas docs, re-heads the original 60 dormant
     targets for a FINAL ledger, and issues orphan verdicts for keys
     with no put-proximate writer anywhere.
  B. eFD filer names carry middles/titles the party map's exact-key
     lookup missed (64% attribution). v1.3 indexes first+last and
     last+initial (ambiguity dropped, never guessed) and forces one
     schema refresh. Gate: attribution >= 80% or the residual names
     printed for the next widening.
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
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
NOW = datetime.now(timezone.utc)

RX_PUT = re.compile(r"put_object\s*\(", re.S)
RX_KEYLIT = re.compile(r"""Key\s*=\s*["'](data/[A-Za-z0-9_\-./]+\.json)["']""")
RX_KEYVAR = re.compile(r"Key\s*=\s*([A-Z][A-Z0-9_]*)")
RX_CONST = re.compile(
    r"""^\s*([A-Z][A-Z0-9_]*)\s*=\s*["'](data/[A-Za-z0-9_\-./]+\.json)["']""",
    re.M)

def put_writers():
    """artifact -> [engines], anchored strictly on put_object windows."""
    w = {}
    for eng in sorted(os.listdir("aws/lambdas")):
        sp = "aws/lambdas/%s/source/lambda_function.py" % eng
        if not os.path.exists(sp):
            continue
        t = open(sp, encoding="utf-8", errors="ignore").read()
        consts = dict((m.group(1), m.group(2))
                      for m in RX_CONST.finditer(t))
        keys = set()
        for m in RX_PUT.finditer(t):
            win = t[m.start():m.start() + 600]
            for k in RX_KEYLIT.findall(win):
                keys.add(k)
            for v in RX_KEYVAR.findall(win):
                if v in consts:
                    keys.add(consts[v])
        for k in keys:
            w.setdefault(k, []).append(eng)
    return w

def exists(key):
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except Exception:
        return False

fails = []
with report("4289_census_v3") as r:
    r.heading("ops 4289 -- census v3 + attribution widened")

    r.section("A. put-proximity writer map")
    W = put_writers()
    r.ok("v3 writer map: %d artifacts with proven put-writers "
         "(engines: %d)" % (len(W), len(os.listdir("aws/lambdas"))))
    mi = W.get("data/morning-intel.json")
    r.log("morning-intel true writer under v3: %s"
          % (mi or "ORPHAN CONFIRMED (no put anywhere)"))

    atlas = json.loads(s3.get_object(
        Bucket=BUCKET, Key="data/alpha-atlas.json")["Body"].read())
    fixed = 0
    for fam in atlas.get("families") or []:
        for e in fam.get("engines") or []:
            k = e.get("artifact")
            k = k if str(k).startswith("data/") else "data/%s" % k
            v3w = (W.get(k) or [])[:2]
            if v3w != (e.get("writers") or []):
                e["writers"] = v3w
                fixed += 1
    triage = atlas.get("dormant_triage") or []
    final = {"ALIVE": 0, "LAZY_EVENT": 0, "ORPHAN": 0,
             "STILL_DORMANT": 0}
    orphans = []
    for t in triage:
        if exists(t["target"]):
            t["final"] = "ALIVE"
        elif not W.get(t["target"]):
            t["final"] = "ORPHAN"
            orphans.append(t["target"])
        elif t.get("class") == "LAZY_EVENT":
            t["final"] = "LAZY_EVENT"
        else:
            t["final"] = "STILL_DORMANT"
        final[t["final"]] += 1
    r.ok("FINAL dormant ledger (n=%d): %s" % (len(triage), final))
    if orphans:
        r.log("orphans (no put-writer exists; reader-blamed before): "
              "%s" % [o.replace("data/", "") for o in orphans][:8])
    still = [t["target"] for t in triage
             if t["final"] == "STILL_DORMANT"]
    if still:
        r.log("still dormant with real writers: %s"
              % [x.replace("data/", "") for x in still][:8])
    atlas["writer_map_version"] = 3
    atlas["totals"]["writers_corrected"] = fixed
    atlas["totals"]["dormant_final"] = final
    atlas["dormant_triage"] = triage
    s3.put_object(Bucket=BUCKET, Key="data/alpha-atlas.json",
                  Body=json.dumps(atlas, separators=(",", ":"),
                                  default=str).encode(),
                  ContentType="application/json",
                  CacheControl="public, max-age=1800")
    try:
        qd = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/quantum-desk-sources.json")["Body"].read())
        for pt in qd.get("per_ticker_sources") or []:
            pt["writers"] = (W.get(pt.get("key")) or [])[:2]
        qd["writer_map_version"] = 3
        s3.put_object(Bucket=BUCKET,
                      Key="data/quantum-desk-sources.json",
                      Body=json.dumps(qd, separators=(",", ":"),
                                      default=str).encode(),
                      ContentType="application/json",
                      CacheControl="public, max-age=3600")
        r.ok("both census docs on v3 writers (%d atlas rows corrected)"
             % fixed)
    except Exception as e:
        r.warn("qd-sources update: %s" % str(e)[:80])

    r.section("B. attribution v1.3, measured")
    ok_dep = False
    for _ in range(45):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-political-stocks")
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") == "Active":
                lm = datetime.strptime(
                    c["LastModified"].split(".")[0], "%Y-%m-%dT%H:%M:%S"
                ).replace(tzinfo=timezone.utc)
                if (NOW - lm).total_seconds() < 12 * 60:
                    ok_dep = True
                    break
        except Exception:
            pass
        time.sleep(8)
    if not ok_dep:
        fails.append("political-stocks deploy window missed")
    else:
        p = lam.invoke(FunctionName="justhodl-political-stocks",
                       InvocationType="RequestResponse", Payload=b"{}")
        r.log("invoked: %s"
              % (p["Payload"].read() or b"")[:160].decode("utf-8",
                                                          "ignore"))
        doc = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/political-stocks.json")["Body"].read())
        cong = doc.get("congress") or {}
        n_att = n_tot = 0
        misses = {}
        pools = [v for v in cong.values()
                 if isinstance(v, list) and v
                 and isinstance(v[0], dict) and "recent_trades" in v[0]]
        for pool in pools:
            for tk in pool:
                for tr in (tk.get("recent_trades") or []):
                    n_tot += 1
                    if tr.get("party") in ("D", "R", "I"):
                        n_att += 1
                    else:
                        nm = tr.get("politician") or "?"
                        misses[nm] = misses.get(nm, 0) + 1
        rate = 100.0 * n_att / n_tot if n_tot else 0
        top_miss = sorted(misses.items(), key=lambda kv: -kv[1])[:8]
        (r.ok if rate >= 80 else r.warn)(
            "attribution: %d/%d = %.0f%% (was 64%%)"
            % (n_att, n_tot, rate))
        if top_miss:
            r.log("residual unattributed: %s" % top_miss)
        if rate < 80:
            fails.append("attribution %.0f%% < 80%% -- residual names "
                         "above drive the next widening" % rate)

    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4289 PASS -- census truthful, attribution >=80%%")
if fails:
    sys.exit(1)
