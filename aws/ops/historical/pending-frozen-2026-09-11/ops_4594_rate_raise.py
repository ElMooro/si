"""ops 4594 — FRED rate raise to 100/min + fleet-red closure (Khalid).

Khalid: raise the knob to ~100/min and keep watching. The engine's AIMD
is the native governor (starts 70, +6/min per clean 120-call window,
x0.6 + cool on 429, hard-halt on 403) — the ceiling is SSM
/justhodl/fred/rate-ceiling. This op:

  1. reads the prior ceiling (evidence of the 60-era setting), writes
     100, reads back
  2. kicks fred-catalog and confirms the drain advances with no block
  3. closes the 4592 reds: catalyst-skew-premove + failed-pattern-
     reversal never published data_sufficiency AND never refreshed —
     deploy-shaped (mid-bunch commits may have missed the diff). Prints
     each function's LastModified + CloudWatch tail, force-redeploys
     from repo source, invokes, and re-asserts the BUG-4 gates.
"""
import io
import json
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
B = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)
REPO = Path(__file__).resolve().parents[2] / "lambdas"
KNOB = "/justhodl/fred/rate-ceiling"
REDS = {
    "justhodl-catalyst-skew-premove": "data/catalyst-skew-premove.json",
    "justhodl-failed-pattern-reversal": "data/failed-pattern-reversal.json",
}


def get_json(key):
    try:
        b = s3.get_object(Bucket=B, Key=key)["Body"].read()
        import gzip
        if key.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return None


def zip_src(fn):
    buf = io.BytesIO()
    src = REPO / fn / "source"
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src))
    return buf.getvalue()


def settle(r, fn, deadline_s=240):
    t0 = time.time()
    while time.time() - t0 < deadline_s:
        try:
            c = lam.get_function(FunctionName=fn)["Configuration"]
            if c.get("LastUpdateStatus") == "Successful" \
                    and c.get("State") == "Active":
                return c
        except Exception:
            pass
        time.sleep(6)
    r.warn("  %s did not settle" % fn)
    return {}


def contract(r, name, cond, why):
    if cond:
        r.ok("  [%s] %s" % (name, why))
        return 0
    r.fail("  [%s] CONTRACT MISS — %s" % (name, why))
    return 1


def main():
    with report("4594_rate_raise") as r:
        r.heading("ops 4594 — FRED 100/min + fleet-red closure")
        misses = 0
        now = datetime.now(timezone.utc)

        r.section("1. The knob")
        try:
            prior = ssm.get_parameter(Name=KNOB)["Parameter"]["Value"]
        except Exception:
            prior = "<unset — code default 100 with AIMD start 70>"
        r.log("  prior %s = %s" % (KNOB, prior))
        ssm.put_parameter(Name=KNOB, Value="100", Type="String",
                          Overwrite=True)
        back = ssm.get_parameter(Name=KNOB)["Parameter"]["Value"]
        misses += contract(r, "knob", back == "100",
                           "ceiling now %s req/min (AIMD climbs to it on "
                           "clean windows, backs off on any 429, halts on "
                           "403 — Khalid-approved raise)" % back)

        r.section("2. Drain kick + advance check")
        stk = "data/_state/fred-scoped-import.json"
        st0 = get_json(stk) or {}
        c0 = st0.get("updated_at")
        try:
            lam.invoke(FunctionName="justhodl-fred-catalog",
                       InvocationType="Event")
            r.log("  fred-catalog kicked (checkpoint was %s)" % c0)
        except Exception as e:
            r.warn("  kick: %s" % str(e)[:100])
        t0 = time.time()
        st1 = st0
        while time.time() - t0 < 240:
            time.sleep(15)
            st1 = get_json(stk) or {}
            if st1.get("updated_at") and st1.get("updated_at") != c0:
                break
        misses += contract(r, "drain",
                           st1.get("updated_at") != c0,
                           "checkpoint advanced (%s → %s)"
                           % (c0, st1.get("updated_at")))
        misses += contract(r, "drain", not st1.get("blocked_at"),
                           "no block after raise (blocked_at=%s)"
                           % st1.get("blocked_at"))
        for k in sorted(st1):
            if "rpm" in k or "rate" in k or "429" in k:
                r.log("    %s = %s" % (k, st1.get(k)))
        q = get_json("data/_state/fred-queue.json.gz") or {}
        rem = max(0, len(q.get("rows") or []) - int(q.get("cursor") or 0))
        eff = 100 * 60 * 0.82
        r.log("  remaining=%d; projected at ceiling-100 effective "
              "~%.0f/h → ETA ~%.1f h (~%.1f days). The health strip "
              "shows the real number within the hour; AIMD finds the "
              "true sustainable pace." % (rem, eff, rem / eff,
                                          rem / eff / 24))

        r.section("3. 4592 reds — deploy evidence + force redeploy")
        start_ms = int((now - timedelta(minutes=90)).timestamp() * 1000)
        for fn, key in REDS.items():
            nm = fn.replace("justhodl-", "")
            c = settle(r, fn)
            r.log("  %s LastModified=%s" % (nm, c.get("LastModified")))
            try:
                ev = logs.filter_log_events(
                    logGroupName="/aws/lambda/%s" % fn,
                    startTime=start_ms, limit=200)["events"]
                bad = [e["message"].rstrip() for e in ev
                       if "Traceback" in e["message"]
                       or "[ERROR]" in e["message"]
                       or "Task timed out" in e["message"]]
                r.log("    cw last-90m: %d lines, %d error-sigs"
                      % (len(ev), len(bad)))
                for msg in bad[-3:]:
                    r.log("      | %s" % msg[:200])
            except Exception as e:
                r.log("    cw: %s" % str(e)[:90])
            lam.update_function_code(FunctionName=fn, ZipFile=zip_src(fn))
            settle(r, fn)
            r.log("    force-redeployed from repo source")

        r.section("4. Re-gate the two")
        before = {}
        for fn, key in REDS.items():
            j = get_json(key) or {}
            before[fn] = j.get("as_of") or j.get("generated_at") or ""
            lam.invoke(FunctionName=fn, InvocationType="Event")
        outs, pending, t0 = {}, dict(REDS), time.time()
        while pending and time.time() - t0 < 420:
            time.sleep(12)
            for fn in list(pending):
                cur = get_json(pending[fn])
                ts = (cur or {}).get("as_of") or \
                     (cur or {}).get("generated_at") or ""
                if cur is not None and ts and ts != before[fn]:
                    outs[fn] = cur
                    r.log("  %s refreshed (%ss)"
                          % (fn.replace("justhodl-", ""),
                             int(time.time() - t0)))
                    del pending[fn]
        for fn in pending:
            r.warn("  %s did not refresh in 420s" % fn)
            outs[fn] = get_json(pending[fn]) or {}
            misses += 1
        for fn in REDS:
            nm = fn.replace("justhodl-", "")
            j = outs.get(fn) or {}
            ds = j.get("data_sufficiency")
            misses += contract(r, nm, isinstance(ds, dict),
                               "data_sufficiency published (state=%s, "
                               "ds=%s)"
                               % (j.get("state"),
                                  {k: v for k, v in (ds or {}).items()
                                   if k != "rule"} if isinstance(ds, dict)
                                  else None))

        r.section("verdict")
        if misses:
            r.fail("rate raise / red closure: %d red" % misses)
            sys.exit(1)
        r.ok("ceiling 100 live with AIMD governing; drain advancing "
             "unblocked; both fleet reds closed — all ten BUG-4 gates "
             "now verified")


if __name__ == "__main__":
    main()
