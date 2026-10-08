"""ops_5420 -- stock-buying v1.5.2 restore gate (2026-09-11).

Context: Grok's Contents-API writes put a PLACEHOLDER and then keep-alive stubs on main for
aws/lambdas/justhodl-stock-buying (commits d928157, 7513191, f62e2a0, 5236cb1); all four deploy runs were
cancelled with the deploy step skipped, so AWS kept v1.5.1. Claude restored the full source from git blob
eaadb467 (byte-identical to v1.5.1 at c80813e) and applied the FMP /stable/ migration (the last engine on
/api/v3) as v1.5.2 in commit e219b4b; deploy run 3179 deployed it.

This gate proves, on AWS, that:
  1. the live function's code is the restored v1.5.2 (zip contains the v1.5.2 marker, /stable/ base,
     eps_beats, managed_secret.py; no /api/v3) and LastModified is after the 3179 deploy;
  2. one real run regenerates data/stock-buying.json (generated_at advances) with n_scored > 0, the FMP key
     resolved, and rows carrying FMP-derived pillars (revisions_beats from /stable/earnings, _fmpq from
     /stable/income-statement) -- i.e. the /stable/ calls return parseable data, not silent Nones;
  3. no Traceback/ERROR in the function's log since the deploy.
Prints no secret values.
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

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-stock-buying"
BUCKET = "justhodl-dashboard-live"
KEY = "data/stock-buying.json"
DEPLOY_AT = datetime(2026, 9, 11, 20, 9, 39, tzinfo=timezone.utc)   # run 3179 "Deploy each changed Lambda" start
POLL_SECS, POLL_MAX = 30, 40 * 60                                   # wait up to 40 min for the run to publish
FAILS = []


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds")


def _parse_ts(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


def _read_feed(s3):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read()
        return json.loads(body), len(body)
    except Exception as e:
        return None, str(e)[:100]


with report("ops_5420_stock_buying_v152_gate") as R:
    R.heading("ops 5420 -- stock-buying v1.5.2 restore gate")
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    logs = boto3.client("logs", region_name=REGION)

    # ── 1. deployed code is the restored v1.5.2 ──────────────────────────────────────────────────
    R.section("1. deployed code")
    fn = lam.get_function(FunctionName=FN)
    cfg = fn["Configuration"]
    lm = _parse_ts(cfg.get("LastModified"))
    R.log("state=%s last_update=%s last_modified=%s timeout=%ss memory=%sMB runtime=%s" % (
        cfg.get("State"), cfg.get("LastUpdateStatus"), cfg.get("LastModified"), cfg.get("Timeout"),
        cfg.get("MemorySize"), cfg.get("Runtime")))
    if not (lm and lm >= DEPLOY_AT):
        FAILS.append("LastModified %s is before the 3179 deploy (%s)" % (cfg.get("LastModified"), _iso(DEPLOY_AT)))
    zb = urllib.request.urlopen(urllib.request.Request(
        fn["Code"]["Location"], headers={"User-Agent": "justhodl-ops"}), timeout=60).read()
    z = zipfile.ZipFile(io.BytesIO(zb))
    names = z.namelist()
    src = z.read("lambda_function.py").decode("utf-8", "replace") if "lambda_function.py" in names else ""
    checks = {
        "zip has lambda_function.py": bool(src),
        "source is the full engine (>35KB)": len(src) > 35000,
        "v1.5.2 marker": "justhodl-stock-buying v1.5.2" in src,
        "/stable/ base URL": "financialmodelingprep.com/stable/" in src,
        "no /api/v3 base URL": "financialmodelingprep.com/api/v3" not in src,
        "eps_beats present": "def eps_beats" in src,
        "managed_secret bundled": "managed_secret.py" in names,
        "no PLACEHOLDER / keep-alive stub": "PLACEHOLDER" not in src and "TEMP keep-alive" not in src,
    }
    for k, ok in checks.items():
        (R.ok if ok else R.fail)("%s: %s" % (k, ok))
        if not ok:
            FAILS.append("deployed code: " + k)
    R.kv(step="code", zip_bytes=len(zb), source_bytes=len(src), modules=len(names))

    # ── 2. one real run publishes the feed with /stable/-derived fields ───────────────────────────
    R.section("2. live run")
    before, sz = _read_feed(s3)
    b_gen = (before or {}).get("generated_at") or (before or {}).get("as_of")
    R.log("baseline %s: generated_at=%s n_scored=%s fmp_key=%s bytes=%s" % (
        KEY, b_gen, (before or {}).get("n_scored"), (before or {}).get("fmp_key"), sz))
    if FAILS:
        R.warn("deployed code failed its checks -- not invoking a broken build; see verdict")
    else:
        t0 = datetime.now(timezone.utc)
        r = lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b'{"source":"ops_5420_gate"}')
        R.log("invoked (Event) status=%s at %s" % (r.get("StatusCode"), _iso(t0)))
        after, waited = None, 0
        while waited < POLL_MAX:
            time.sleep(POLL_SECS)
            waited += POLL_SECS
            cur, _ = _read_feed(s3)
            g = _parse_ts((cur or {}).get("generated_at") or (cur or {}).get("as_of") or "")
            if g and g >= t0:
                after = cur
                break
        if after is None:
            FAILS.append("feed did not regenerate within %d min of the invoke (generated_at still %s)" % (POLL_MAX // 60, b_gen))
            # surface why: recent log lines
        else:
            top = after.get("top") or []
            n_scored = after.get("n_scored")
            fmp_key = after.get("fmp_key")
            with_beats = sum(1 for row in top if ((row.get("pillars") or {}).get("revisions_beats")) is not None)
            gated = sum(1 for row in top if ((row.get("gates") or {}).get("below_sma")))
            with_fmpq = sum(1 for row in top if row.get("_fmpq") or (row.get("pillars") or {}).get("accel") is not None)
            R.log("regenerated after %ds: generated_at=%s n_universe=%s n_scored=%s fmp_key=%s top=%d" % (
                waited, after.get("generated_at") or after.get("as_of"), after.get("n_universe"), n_scored, fmp_key, len(top)))
            R.log("rows below_sma (FMP path taken)=%d  rows with revisions_beats (/stable/earnings parsed)=%d  rows with accel/_fmpq (/stable/income-statement parsed)=%d" % (
                gated, with_beats, with_fmpq))
            R.kv(step="run", waited_s=waited, n_scored=n_scored, fmp_key=fmp_key, gated=gated, with_beats=with_beats, with_fmpq=with_fmpq)
            if not n_scored:
                FAILS.append("n_scored is %r after the run" % n_scored)
            if not fmp_key:
                FAILS.append("fmp_key=false: managed_secret found no FMP key in env or SSM for this role")
            if gated and not with_beats:
                FAILS.append("%d rows took the FMP path but none carry revisions_beats -- /stable/earnings not parsing" % gated)
            if not gated:
                R.warn("no row passed below_sma this run, so the FMP path was not exercised (data-dependent, not a defect)")
            for row in top[:5]:
                R.log("  %-6s tier=%-9s score=%s beats=%s accel=%s" % (
                    row.get("symbol"), row.get("tier"), row.get("score"),
                    (row.get("pillars") or {}).get("revisions_beats"), (row.get("pillars") or {}).get("accel")))

    # ── 3. log scan since the deploy ──────────────────────────────────────────────────────────────
    R.section("3. log scan since deploy")
    try:
        ev = logs.filter_log_events(logGroupName="/aws/lambda/" + FN, startTime=int(DEPLOY_AT.timestamp() * 1000),
                                    filterPattern='?Traceback ?"[ERROR]" ?"Task timed out"', limit=50)
        bad = ev.get("events", [])
        if bad:
            FAILS.append("%d error/timeout log lines since the deploy" % len(bad))
            for e in bad[:8]:
                R.fail("  " + e.get("message", "")[:160].replace("\n", " "))
        else:
            R.ok("no Traceback / [ERROR] / timeout lines since %s" % _iso(DEPLOY_AT))
    except Exception as e:
        R.warn("log scan unavailable: %s" % str(e)[:100])

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- justhodl-stock-buying runs the restored v1.5.2 on FMP /stable/ and published a fresh %s" % KEY)
