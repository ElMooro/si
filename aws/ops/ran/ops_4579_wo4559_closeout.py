"""ops 4579 — wo4559 close-out: resolve 4574's single contract miss.

ops 4574 ran the full ops-4559 verification: 24/25 assertions green, one
miss — port-cargo v1.0.1 crashed on ISO date strings from the live ArcGIS
layer (int('2026-06-29')), so the payload it asserted on carried
n_ports_with_data=0. v1.0.2 (shipped in the 4576 arc) fixed the parser and
its one-line gate saw 2,065 ports. The 4574 verdict, however, is still
stamped FAILURE and the full five-assert port-cargo contract was never
re-run. This op finishes the work order:

  1. settles justhodl-port-cargo and proves the DEPLOYED zip carries the
     v1.0.2 marker (race-safe rule: never assert on unverified code)
  2. fresh RequestResponse invoke with the S3 baseline recorded first;
     any FunctionError is surfaced in-band, and the payload object must
     be strictly newer than the pre-invoke baseline
  3. re-runs the EXACT port-cargo contract set from 4574 — fetch_status
     split, parsed-or-gap-stated, lag_months, tonnage rows n>0, country
     rollups — plus data_age_days honesty
  4. CDN truth: cache-busted live payload + page marker via justhodl.ai
  5. VERDICT: wo4559 CLOSED — supersedes the 4574 failure verdict
     (10/10 engines, all ops-4559 contracts green)

Read-only against the repo; touches AWS only to invoke and read. No
lambda source changes ride this commit → [skip-deploy].
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
FN = "justhodl-port-cargo"
KEY = "data/port-cargo.json"
MARKER = "v1.0.2"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=840, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def obj_mtime(key):
    try:
        return s3.head_object(Bucket=B, Key=key)["LastModified"]
    except Exception:
        return None


def http_probe(url, needle=None):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 ops-4579",
                                                   "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=25) as r:
            body = r.read().decode("utf-8", "replace")
            hit = (needle in body) if needle else True
            return r.status, hit
    except urllib.error.HTTPError as e:
        return e.code, False
    except Exception:
        return None, False


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4579_wo4559_closeout") as r:
        misses = 0

        # ── 1. settle + deployed-code marker proof ──────────────────────
        r.section("1. Settle + deployed v1.0.2 marker proof")
        t0 = time.time()
        cfg = None
        while time.time() - t0 < 420:
            cfg = lam.get_function(FunctionName=FN)
            c = cfg["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
                r.log("  %s settled (LastModified %s, timeout=%ss, mem=%sMB)"
                      % (FN, c.get("LastModified"), c.get("Timeout"),
                         c.get("MemorySize")))
                break
            time.sleep(6)
        else:
            r.fail("  %s never settled" % FN)
            sys.exit(1)

        loc = cfg["Code"]["Location"]
        with urllib.request.urlopen(loc, timeout=60) as resp:
            zbytes = resp.read()
        src = ""
        with zipfile.ZipFile(io.BytesIO(zbytes)) as z:
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", "replace")
                    break
        misses += contract(r, "deploy", MARKER in src,
                           "deployed zip carries the %s ISO-date parser" % MARKER)
        if MARKER not in src:
            sys.exit(1)  # asserting further would repeat 4574's blind spot

        # ── 2. baseline → invoke → strict refresh ───────────────────────
        r.section("2. Fresh invoke (baseline-strict)")
        base = obj_mtime(KEY)
        r.log("  pre-invoke %s LastModified=%s" % (KEY, base))
        t_inv = time.time()
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
        took = time.time() - t_inv
        ferr = resp.get("FunctionError")
        body = resp["Payload"].read().decode("utf-8", "replace")[:600]
        if ferr:
            r.fail("  FunctionError after %.0fs: %s" % (took, body))
            sys.exit(1)
        r.log("  invoke OK in %.0fs" % took)
        fresh = None
        t0 = time.time()
        while time.time() - t0 < 180:
            m = obj_mtime(KEY)
            if m and (base is None or m > base):
                fresh = m
                break
            time.sleep(5)
        misses += contract(r, "port-cargo", fresh is not None,
                           "payload strictly newer than pre-invoke baseline "
                           "(now %s)" % fresh)

        # ── 3. the exact 4574 contract set, on the fresh payload ────────
        r.section("3. Contract assertions (4574 set, verbatim)")
        j = get_json(KEY) or {}
        misses += contract(r, "port-cargo", j.get("fetch_status") is not None,
                           "fetch_status separate from data age")
        misses += contract(r, "port-cargo",
                           (j.get("n_ports_with_data") or 0) > 0 or bool(j.get("gaps")),
                           "ports parsed OR gap stated — never silent (n=%s)"
                           % j.get("n_ports_with_data"))
        misses += contract(r, "port-cargo", "lag_months" in j, "lag_months declared")
        npc = j.get("n_ports_with_data") or 0
        misses += contract(r, "port-cargo", npc > 0,
                           "tonnage rows parsed after where-negotiation (n=%s)" % npc)
        if 0 < npc < 1500:
            r.warn("  [port-cargo] only %d of ~2065 ports parsed — check gaps: %s"
                   % (npc, (j.get("gaps") or [])[:2]))
        misses += contract(r, "port-cargo", not npc or bool(j.get("countries")),
                           "country rollups populated when ports parse")
        age = j.get("data_age_days")
        misses += contract(r, "port-cargo", age is not None,
                           "data_age_days declared (=%s; source lag is stated, "
                           "not hidden)" % age)
        r.log("  version=%s ports=%s countries=%s global_pulse=%s"
              % (j.get("version"), npc, len(j.get("countries") or {}),
                 (j.get("global") or {}).get("pulse_pct",
                                             j.get("global_pulse_pct"))))

        # ── 4. CDN truth (cache-busted) ─────────────────────────────────
        r.section("4. CDN truth")
        cb = int(time.time())
        code, hit = http_probe("https://justhodl.ai/%s?cb=%s" % (KEY, cb),
                               '"n_ports_with_data"')
        misses += contract(r, "cdn", code == 200 and hit,
                           "live payload serves 200 with schema key (HTTP %s)" % code)
        code, hit = http_probe("https://justhodl.ai/port-cargo.html?cb=%s" % cb,
                               "Port Cargo")
        misses += contract(r, "cdn", code == 200 and hit,
                           "page 200 with marker (HTTP %s)" % code)

        # ── 5. work-order verdict ───────────────────────────────────────
        r.section("VERDICT — work order ops-4559")
        if misses == 0:
            r.ok("wo4559 CLOSED — port-cargo contract green on v1.0.2; "
                 "supersedes the 4574 failure verdict. 10/10 engines, "
                 "all ops-4559 contracts passing "
                 "(4574: 24/25 + this op: port-cargo 8/8).")
            r.log("  open follow-ups carried OUT of this work order: "
                  "(a) 10 fleet engines flagged by 4574's BUG-4 sweep "
                  "(confident QUIET on zero inputs) — separate patch arc; "
                  "(b) grid-queue ISO-NE + PJM adapters (PJM_API_KEY pending "
                  "Khalid); (c) legacy desk-v2.html sidebar 404 (cosmetic).")
        else:
            r.fail("%d contract miss(es) — wo4559 stays open" % misses)
        r.kv(contract_misses=misses, n_ports=npc,
             finished_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"))
        if misses:
            sys.exit(1)


if __name__ == "__main__":
    main()
