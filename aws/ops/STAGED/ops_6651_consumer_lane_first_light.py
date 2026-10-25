"""ops 6651 - read-only: first light of the bank-first consumer lane + the banks' day-two manifests.

209 FMP-consuming engines were made bank-first on 2026-10-25 (aws/shared/fmp_bankfirst).  This op reads, for every
engine whose live source carries the shim and that ran in the look-back window (default 8 h, event {"hours": N}):
the REPORT lines (duration, max memory), error-ish lines, and the shim's own lines ("[fmp-bankfirst] hits=... " and
"bank stepped aside ...").  It then reads each bank's state/manifest heads (fmp-bank, fmp-walker incl. universe_source,
polygon-bank, provider-bank by_provider verdicts - TE 200 after the colon fix?, CQ splits -, eia-bank) and the
coverage scoreboard head.  Nothing is changed.  Keys never printed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
BANKS = {
    "justhodl-fmp-bank": "data/warm/fmp-bank/",
    "justhodl-fmp-walker": "data/warm/fmp/",
    "justhodl-polygon-bank": "data/warm/polygon-bank/",
    "justhodl-provider-bank": "data/warm/provider-bank/",
    "justhodl-eia-bank": "data/warm/eia-bank/",
}
SECRET_RE = re.compile(r"(apikey|apiKey|api_key|token|c=|key=)[=:]?[A-Za-z0-9_\-]{12,}", re.I)
REPORT_RE = re.compile(r"Duration: ([\d.]+) ms.*?Max Memory Used: (\d+) MB", re.S)


def redact(s):
    return SECRET_RE.sub(lambda m: m.group(0)[:8] + "<redacted>", str(s))


def jget(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return {}


def shimmed_engines():
    out = []
    for p in sorted((ROOT / "aws" / "lambdas").glob("*/source/lambda_function.py")):
        try:
            if "from fmp_bankfirst import install" in p.read_text(encoding="utf-8", errors="replace"):
                out.append(p.parts[-3])
        except OSError:
            continue
    return out


def scan(logs, fn, since_ms):
    group = "/aws/lambda/" + fn
    reports, errors, shim = [], [], []
    kwargs = {"logGroupName": group, "startTime": since_ms, "limit": 10000}
    for _ in range(12):
        resp = logs.filter_log_events(**kwargs)
        for ev in resp.get("events", []):
            msg = (ev.get("message") or "").strip()
            if msg.startswith("REPORT"):
                reports.append((ev["timestamp"], msg))
            elif "[fmp-bankfirst]" in msg:
                shim.append((ev["timestamp"], msg[:400]))
            elif "Task timed out" in msg or "Traceback" in msg or "[ERROR]" in msg or "MemoryError" in msg or "Runtime.OutOfMemory" in msg:
                errors.append((ev["timestamp"], msg[:500]))
        if resp.get("nextToken"):
            kwargs["nextToken"] = resp["nextToken"]
        else:
            break
    return reports, errors, shim


def main(event=None):
    hours = float((event or {}).get("hours") or 8)
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    since_ms = int(since.timestamp() * 1000)
    with report("ops_6651_consumer_lane_first_light") as r:
        r.heading("ops 6651 - bank-first consumer lane: first light (read-only, last %.0f h)" % hours)
        engines = shimmed_engines()
        r.log("engines carrying the shim in the repo: %d" % len(engines))
        ran, quiet, with_shim_lines, with_errors, stepped_aside, missing_group = 0, 0, 0, 0, 0, 0
        hits = stale = misses = unshaped = bypassed = 0
        r.section("engines that ran since %s UTC" % since.strftime("%H:%M"))
        for fn in engines:
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
            except Exception:  # noqa: BLE001
                r.warn("%s: not on AWS" % fn)
                continue
            try:
                reports, errors, shim = scan(logs, fn, since_ms)
            except Exception as e:  # noqa: BLE001
                if "ResourceNotFound" in type(e).__name__ or "ResourceNotFoundException" in str(e):
                    missing_group += 1
                else:
                    r.warn("%s logs: %s" % (fn, type(e).__name__))
                continue
            if not reports:
                quiet += 1
                continue
            ran += 1
            deployed = (cf.get("LastModified") or "")[:16]
            last_ts, last = reports[-1]
            m = REPORT_RE.search(last)
            dur = "%.0fs" % (float(m.group(1)) / 1000) if m else "?"
            mem = "%s/%sMB" % (m.group(2), cf.get("MemorySize")) if m else "?"
            line = "%s: runs %d, last %s dur %s mem %s, deployed %s" % (fn, len(reports), datetime.fromtimestamp(last_ts / 1000, timezone.utc).strftime("%H:%M"), dur, mem, deployed)
            if shim:
                with_shim_lines += 1
                for _, s in shim:
                    mm = re.search(r"hits=(\d+) stale=(\d+) misses=(\d+) unshaped=(\d+) passthrough=\d+ bypassed=(\d+)", s)
                    if mm:
                        hits += int(mm.group(1)); stale += int(mm.group(2)); misses += int(mm.group(3)); unshaped += int(mm.group(4)); bypassed += int(mm.group(5))
                    if "stepped aside" in s or "consecutive misses" in s:
                        stepped_aside += 1
                line += " | " + redact(shim[-1][1])[:220]
            if errors:
                with_errors += 1
                line += " | ERRORS %d: %s" % (len(errors), redact(errors[0][1])[:200])
            (r.warn if errors else r.log)(line)
        r.section("totals")
        r.log("ran %d, quiet %d, no log group %d; engines with shim lines %d (summary lines only appear where an engine calls print_stats); errors in %d; stepped-aside events %d" % (ran, quiet, missing_group, with_shim_lines, with_errors, stepped_aside))
        r.log("shim counters summed over printed summaries: hits %d stale %d misses %d unshaped %d bypassed %d" % (hits, stale, misses, unshaped, bypassed))
        r.section("banks")
        for fn, root in BANKS.items():
            st = jget(s3, root + "_state/state.json")
            man = jget(s3, root + "manifest.json")
            lr = st.get("last_run") or {}
            r.log("%s: runs %s phase %s updated %s | last_run calls %s puts %s seconds %s http %s" % (fn, st.get("runs"), st.get("phase"), st.get("updated_at"), lr.get("calls"), lr.get("puts"), lr.get("seconds"), json.dumps(lr.get("http"))[:300]))
            r.log("   manifest: objects %s GB %.2f datasets %s/%s refused %s" % (man.get("objects"), (man.get("bytes") or 0) / 1e9, man.get("datasets"), man.get("datasets_declared"), json.dumps(man.get("endpoints_refused") or man.get("endpoints_not_served") or man.get("refused"))[:500]))
            if man.get("universe_counts") or st.get("universe_source"):
                r.log("   universes: source %s counts %s" % (st.get("universe_source"), json.dumps(man.get("universe_counts"))[:400]))
            if man.get("by_provider"):
                r.log("   by_provider: %s" % json.dumps(man.get("by_provider"))[:900])
        cov = jget(s3, "data/provider-coverage.json")
        r.section("coverage scoreboard")
        r.log("provider-coverage.json: generated %s providers %s" % (cov.get("generated_at") or cov.get("updated_at"), json.dumps({k: (v.get("coverage") if isinstance(v, dict) else v) for k, v in (cov.get("providers") or {}).items()})[:900]))
        r.ok("ops 6651 complete (read-only)")


if __name__ == "__main__":
    main(json.loads(sys.argv[1]) if len(sys.argv) > 1 else None)
    sys.exit(0)
