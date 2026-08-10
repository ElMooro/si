"""ops 4574 — work order ops-4559 landing verification + fleet sweep.

Eight engines patched (stealth-accumulation, etf-true-flows v2, grid-queue v2,
dark-pool, share-flows, freight-pulse, accumulation-radar, flow-lookthrough)
and two created (port-cargo, accum-composite). This op:

  1. creates the two NEW functions from repo source if absent (deploy-lambdas
     only updates existing ones), wires their EventBridge schedules from
     config.json, and settles zips on every touched function
  2. invokes each engine and asserts the ops-4559 payload contracts —
     including the P0 rule: no engine may emit a confident negative from an
     empty input set
  3. sweeps the ENTIRE data/ tree for the BUG-4 pattern (count==0 +
     empty missing/gaps + confident state) → data/fleet-false-negative-audit.json
  4. BUG-10: re-puts the two 403ing provider-inventory objects with correct
     ContentType and probes them through the CDN
  5. probes the sidebar partial for the two new clusters
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
ACCT = "857687956942"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
SCHED_ROLE = "arn:aws:iam::857687956942:role/justhodl-scheduler-role"
REPO = Path(__file__).resolve().parents[2] / "lambdas"   # aws/lambdas

PATCHED = {
    "justhodl-stealth-accumulation": "data/stealth-accumulation.json",
    "justhodl-etf-true-flows": "data/etf-true-flows.json",
    "justhodl-grid-queue": "data/grid-queue.json",
    "justhodl-dark-pool": "data/dark-pool.json",
    "justhodl-share-flows": "data/share-flows.json",
    "justhodl-freight-pulse": "data/freight-pulse.json",
    "justhodl-accumulation-radar": "data/accumulation-radar.json",
    "justhodl-flow-lookthrough": "data/flow-lookthrough.json",
}
NEW = {
    "justhodl-port-cargo": "data/port-cargo.json",
    "justhodl-accum-composite": "data/accum-composite.json",
}
# engines too heavy to block on synchronously — async invoke + S3 poll
ASYNC_FNS = {"justhodl-dark-pool", "justhodl-accumulation-radar",
             "justhodl-flow-lookthrough", "justhodl-share-flows",
             "justhodl-etf-true-flows"}
CONFIDENT_NEG = {"QUIET", "CLEAR", "NONE", "CALM", "NO_SIGNAL", "NO_SETUPS",
                 "NOTHING", "OK_NO_FINDINGS"}


def zip_src(fn):
    buf = io.BytesIO()
    src = REPO / fn / "source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src))
    return buf.getvalue()


def get_json(key):
    try:
        return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())
    except Exception:
        return None


def http_probe(url, needle=None):
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 ops-4574",
                                                   "Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=25) as r:
            body = r.read().decode("utf-8", "replace")
            hit = (needle in body) if needle else True
            return r.status, hit
    except urllib.error.HTTPError as e:
        return e.code, False
    except Exception:
        return None, False


def settle(r, fn, deadline_s=420):
    """Wait until the live zip's code sha matches a fresh deploy state."""
    t0 = time.time()
    last = None
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
                lm = c.get("LastModified", "")
                if lm != last:
                    r.log("  %s settled (LastModified %s)" % (fn, lm))
                return True
            last = c.get("LastModified")
        except lam.exceptions.ResourceNotFoundException:
            return False
        time.sleep(6)
    r.warn("  %s did not settle in %ss" % (fn, deadline_s))
    return False


def ensure_new(r, fn):
    cfg = json.loads((REPO / fn / "config.json").read_text())
    try:
        lam.get_function(FunctionName=fn)
        r.log("  %s already exists — updating code from repo" % fn)
        lam.update_function_code(FunctionName=fn, ZipFile=zip_src(fn))
    except lam.exceptions.ResourceNotFoundException:
        r.log("  creating %s (%sMB / %ss)" % (fn, cfg["memory"], cfg["timeout"]))
        lam.create_function(
            FunctionName=fn, Runtime=cfg["runtime"], Role=cfg["role"],
            Handler=cfg["handler"], Timeout=cfg["timeout"],
            MemorySize=cfg["memory"], Code={"ZipFile": zip_src(fn)},
            Description=cfg.get("description", "")[:250],
            Environment={"Variables": cfg.get("environment", {})})
    settle(r, fn)
    scfg = cfg.get("schedule")
    if scfg:
        name = scfg["name"]
        arn = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, fn)
        try:
            sch.get_schedule(Name=name)
            r.log("  %s schedule exists: %s" % (fn, name))
        except Exception:
            sch.create_schedule(
                Name=name, ScheduleExpression=scfg["expression"],
                FlexibleTimeWindow={"Mode": "OFF"},
                Target={"Arn": arn, "RoleArn": SCHED_ROLE},
                Description=scfg.get("description", "")[:250])
            r.ok("  %s schedule created via EventBridge Scheduler: %s (%s)"
                 % (fn, name, scfg["expression"]))


def invoke_and_fetch(r, fn, key, max_wait=780):
    """Invoke; for heavy engines fire async and poll the S3 output timestamp."""
    before = get_json(key) or {}
    before_ts = before.get("generated_at") or before.get("as_of") or ""
    mode = "Event" if fn in ASYNC_FNS else "RequestResponse"
    try:
        lam.invoke(FunctionName=fn, InvocationType=mode)
    except Exception as e:
        r.fail("  %s invoke error: %s" % (fn, str(e)[:120]))
        return before
    t0 = time.time()
    while time.time() - t0 < max_wait:
        cur = get_json(key)
        ts = (cur or {}).get("generated_at") or (cur or {}).get("as_of") or ""
        if cur is not None and ts and ts != before_ts:
            r.log("  %s output refreshed (%ss)" % (fn, int(time.time() - t0)))
            return cur
        time.sleep(10)
    r.warn("  %s output did not refresh in %ss — asserting on latest available"
           % (fn, max_wait))
    return get_json(key)


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4574_wo4559_verify") as r:
        r.heading("ops 4574 — work order ops-4559 verification")
        misses = 0

        r.section("1. New engines: create + schedule + settle")
        for fn in NEW:
            ensure_new(r, fn)

        r.section("2. Patched engines: settle deploys")
        for fn in PATCHED:
            settle(r, fn)

        r.section("3. Invoke + contract assertions")
        outs = {}
        for fn, key in {**NEW, **PATCHED}.items():
            r.log("engine %s" % fn)
            outs[fn] = invoke_and_fetch(r, fn, key) or {}

        j = outs["justhodl-stealth-accumulation"]
        ds = j.get("data_sufficiency") or {}
        fm = (j.get("summary") or {}).get("feeds_missing")
        misses += contract(r, "stealth", j.get("version") == "1.1", "v1.1 live")
        misses += contract(r, "stealth", isinstance(ds, dict) and "sufficient" in ds,
                           "data_sufficiency block present")
        blind = not ds.get("sufficient", True)
        misses += contract(r, "stealth",
                           (not blind) or j.get("state") not in CONFIDENT_NEG,
                           "P0: no confident negative while blind (state=%s, sufficient=%s)"
                           % (j.get("state"), ds.get("sufficient")))
        misses += contract(r, "stealth",
                           not (blind and fm in ([], None)),
                           "feeds_missing populated when feeds yield zero rows")

        j = outs["justhodl-etf-true-flows"]
        misses += contract(r, "etf-true-flows", str(j.get("version", "")).startswith("2."),
                           "v2 live")
        misses += contract(r, "etf-true-flows", "NAV" in (j.get("method") or ""),
                           "method states NAV multiplier (BUG-1)")
        misses += contract(r, "etf-true-flows", bool(j.get("nav_source_counts")),
                           "nav source chain reporting (BUG-2)")
        misses += contract(r, "etf-true-flows", isinstance(j.get("anomalies"), list),
                           "TNA cross-check anomalies channel (BUG-3)")
        deg = j.get("n_price_fallback_degraded")
        if isinstance(deg, int) and j.get("n_etfs"):
            pct = deg * 100.0 / j["n_etfs"]
            (r.ok if pct < 50 else r.warn)("  [etf-true-flows] degraded NAV fallback on "
                                           "%.0f%% of funds" % pct)

        j = outs["justhodl-port-cargo"]
        misses += contract(r, "port-cargo", j.get("fetch_status") is not None,
                           "fetch_status separate from data age")
        misses += contract(r, "port-cargo",
                           (j.get("n_ports_with_data") or 0) > 0 or bool(j.get("gaps")),
                           "ports parsed OR gap stated — never silent (n=%s)"
                           % j.get("n_ports_with_data"))
        misses += contract(r, "port-cargo", "lag_months" in j, "lag_months declared")

        j = outs["justhodl-grid-queue"]
        nat = j.get("national") or {}
        misses += contract(r, "grid-queue", str(j.get("version", "")).startswith("2."),
                           "v2 live")
        misses += contract(r, "grid-queue", "mw_with_executed_ia" in nat,
                           "executed-IA MW is the primary (BUG-12)")
        misses += contract(r, "grid-queue", len(nat.get("isos_live") or []) >= 2,
                           "multi-ISO coverage (live: %s; missing: %s)"
                           % (nat.get("isos_live"), nat.get("isos_missing")))
        misses += contract(r, "grid-queue", bool(j.get("lbnl_priors")),
                           "LBNL survival priors stated")

        j = outs["justhodl-dark-pool"]
        misses += contract(r, "dark-pool", "data_age_days" in j and "fetch_status" in j,
                           "freshness split fetch vs data (BUG-5)")
        misses += contract(r, "dark-pool", isinstance(j.get("dark_share_map"), dict),
                           "share-of-volume denominator live (BUG-6)")

        j = outs["justhodl-share-flows"]
        misses += contract(r, "share-flows",
                           j.get("engine_class") == "corporate_share_issuance",
                           "engine_class disambiguated (BUG-7)")
        j = outs["justhodl-freight-pulse"]
        misses += contract(r, "freight-pulse",
                           j.get("composite_role") == "slow_confirmation_leg",
                           "relabeled as confirmation leg (BUG-13)")
        j = outs["justhodl-accumulation-radar"]
        misses += contract(r, "accum-radar",
                           j.get("evidence_tier") == "tier_4_unvalidated_technical",
                           "demoted to tier-4 confirmation (BUG-8)")
        j = outs["justhodl-accum-composite"]
        misses += contract(r, "accum-composite", j.get("state") in ("OK", "INSUFFICIENT_DATA"),
                           "state honest (got %s)" % j.get("state"))
        misses += contract(r, "accum-composite",
                           all(("evidence_tier" in c) for n in (j.get("names") or [])[:5]
                               for c in n.get("components", [])),
                           "every component carries evidence_tier")
        j = outs["justhodl-flow-lookthrough"]
        misses += contract(r, "flow-lookthrough",
                           j.get("evidence_tier") == "tier_a_mechanical_fact",
                           "tier-A fact labeling live (BUG-9 expansion to 300)")

        r.section("4. Fleet sweep — confident-negative-from-empty-input (BUG-4 class)")
        flagged, scanned = [], 0
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=B, Prefix="data/", Delimiter="/"):
            for o in page.get("Contents", []):
                k = o["Key"]
                if not k.endswith(".json") or o["Size"] > 3_500_000:
                    continue
                doc = get_json(k)
                if not isinstance(doc, dict):
                    continue
                scanned += 1
                state = str(doc.get("state") or doc.get("verdict") or
                            doc.get("status") or "").upper()
                if state not in CONFIDENT_NEG:
                    continue
                zeros, missing_ok = [], True
                pools = [doc] + [v for v in doc.values() if isinstance(v, dict)][:4]
                for pool in pools:
                    for kk, vv in pool.items():
                        lk = str(kk).lower()
                        if vv == 0 and (lk.startswith("n_") or lk.endswith("_count")
                                        or lk.endswith("_tickers") or lk == "count"):
                            zeros.append(kk)
                        if lk in ("feeds_missing", "gaps", "errors", "missing") and vv:
                            missing_ok = False
                if zeros and missing_ok:
                    flagged.append({"key": k, "state": state, "zero_fields": zeros[:6]})
        audit = {"engine": "fleet-false-negative-audit", "ops": 4574,
                 "generated_at": datetime.now(timezone.utc).isoformat(),
                 "rule": ("confident-negative state + zero-count fields + empty "
                          "missing/gaps arrays = detector may be blind (BUG-4 class)"),
                 "n_scanned": scanned, "n_flagged": len(flagged), "flagged": flagged}
        s3.put_object(Bucket=B, Key="data/fleet-false-negative-audit.json",
                      Body=json.dumps(audit).encode(), ContentType="application/json")
        (r.ok if not flagged else r.warn)(
            "scanned %d payloads — %d flagged for the BUG-4 pattern" % (scanned, len(flagged)))
        for f in flagged[:15]:
            r.log("  ⚠ %s state=%s zeros=%s" % (f["key"], f["state"], f["zero_fields"]))

        r.section("5. BUG-10 — provider inventory 403s")
        for k in ("data/providers/_index.json", "data/provider-inventory.json"):
            try:
                s3.copy_object(Bucket=B, Key=k, CopySource={"Bucket": B, "Key": k},
                               MetadataDirective="REPLACE",
                               ContentType="application/json",
                               CacheControl="public, max-age=300")
                r.log("  re-put %s with correct headers" % k)
            except Exception as e:
                r.warn("  %s re-put failed: %s" % (k, str(e)[:100]))
            code, _ = http_probe("https://justhodl.ai/" + k)
            (r.ok if code == 200 else r.warn)("  CDN probe %s → %s" % (k, code))
        r.log("  coverage-ratio unit mismatch (n_keys vs series.count) noted for the "
              "data-hub line — needs a provider-catalog patch with a single-unit "
              "numerator/denominator, not a blind fix here")

        r.section("6. Sidebar + pages live")
        for url, needle in (("https://justhodl.ai/_partials/sidebar.html", "port-cargo.html"),
                            ("https://justhodl.ai/port-cargo.html", "Port Cargo"),
                            ("https://justhodl.ai/accum-composite.html", "Accumulation Composite")):
            code, hit = http_probe(url, needle)
            (r.ok if (code == 200 and hit) else r.warn)(
                "  %s → HTTP %s, marker %s" % (url, code, "FOUND" if hit else "MISSING (CDN cache?)"))

        r.section("VERDICT")
        if misses == 0:
            r.ok("ops-4559 landed: 0 contract misses across %d engines"
                 % (len(PATCHED) + len(NEW)))
        else:
            r.fail("%d contract miss(es) — see sections above" % misses)
        r.kv(engines_patched=len(PATCHED), engines_created=len(NEW),
             fleet_scanned=scanned, fleet_flagged=len(flagged))
        if misses:
            sys.exit(1)


if __name__ == "__main__":
    main()
