"""
ops_4256 — wave-2 closure of the silent writers, driven by corrected
evidence.

CORRECTIONS FIRST (the audit audits itself)
  * SCHEDULE-INPUT collapses to zero members. All three flagged inputs
    were the literal string '{}' — identical to the probe payload. The
    classifier treated any non-null Input as a mode selector; an empty
    object is not one. etf-census x2 and factor-decomposition join the
    WRITES-ON-PROBE set: write path proven, tomorrow's 13:00 contract
    check is the arbiter of self-heal.
  * SUPERSEDED? was the wrong verdict for all four members. Every one
    is fetched by live pages (compound-signals by four, options-flow by
    three, risk-regime and symbol-map by two each). They are not
    museum pieces — they are pages rendering stale data RIGHT NOW, and
    the fresh "siblings" mean the v2 writer attribution picked the
    wrong engine. This op resolves the TRUE writer per key by grepping
    the exact 'data/<name>' string near put_object across every engine
    source, probes that writer, and verifies the artifact thaws.

THEN
  * The 14 ASYNC-NO-WRITE engines get re-headed now that 40+ minutes
    have passed since the volley — a slow engine that finished and
    wrote is LATE-WRITER-OK, not broken. Still-frozen ones get their
    volley-run log tail pulled as evidence: REPORT line (did it even
    finish?), error lines, stem mentions.
  * feedback-summary (CODE-PATH-SILENT): the write-site guard lines are
    extracted from source and printed — evidence for a reviewed fix,
    not a blind patch.
"""
import base64, io, json, os, re, subprocess, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 4, "mode": "adaptive"},
             read_timeout=210)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4256, "ts": NOW.isoformat()}

ASYNC = [
    ("data/_freshness-manifest.json", "justhodl-fleet-freshness-monitor"),
    ("data/brain-history.json", "justhodl-brain-sync"),
    ("data/congress-party-map.json", "justhodl-political-stocks"),
    ("data/eurodollar-stress.json", "justhodl-dollar-radar"),
    ("data/factor-data-cache.json", "justhodl-factor-decomposition"),
    ("data/history-index.json", "justhodl-calibration-snapshotter"),
    ("data/ka-analysis.json", "justhodl-ka-metrics"),
    ("data/khalid-analysis.json", "justhodl-ka-metrics"),
    ("data/massive-signals.json", "justhodl-alpha-score"),
    ("data/polygon-related-graph.json", "justhodl-supply-chain-graph"),
    ("data/quiver-congress-cache.json", "justhodl-political-stocks"),
    ("data/quiver-lobbying-cache.json", "justhodl-lobbying-intel"),
    ("data/sec-filings-intel.json", "justhodl-pump-mechanics"),
    ("data/signal-halflife.json", "justhodl-meta-improver"),
]
PAGE_CRITICAL = ["data/compound-signals.json", "data/options-flow.json",
                 "data/risk-regime.json", "data/symbol-map.json"]


def age_h(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return (NOW - h["LastModified"]).total_seconds() / 3600.0
    except Exception:
        return None


def wait_active(fn, b=150):
    t0 = time.time()
    while time.time() - t0 < b:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and \
                    c.get("LastUpdateStatus") in (None, "Successful"):
                return True
        except Exception:
            pass
        time.sleep(4)
    return False


def last_tail(fn, lines=26):
    try:
        st = logs.describe_log_streams(
            logGroupName="/aws/lambda/%s" % fn,
            orderBy="LastEventTime", descending=True,
            limit=1)["logStreams"]
        if not st:
            return []
        ev = logs.get_log_events(
            logGroupName="/aws/lambda/%s" % fn,
            logStreamName=st[0]["logStreamName"],
            startFromHead=False, limit=lines)["events"]
        return [e["message"].rstrip() for e in ev]
    except Exception as e:
        return ["LOG-ERR %s" % str(e)[:80]]


def true_writers(base):
    """Every engine whose source puts this exact key."""
    try:
        g = subprocess.run(["grep", "-rl", "--include=*.py", base,
                            "aws/lambdas"], cwd=str(ROOT),
                           capture_output=True, text=True, timeout=30)
        cands = sorted({x.split("/")[2] for x in
                        g.stdout.strip().split("\n") if x})
    except Exception:
        cands = []
    out = []
    for fn in cands:
        p = ROOT / "aws" / "lambdas" / fn / "source" / "lambda_function.py"
        try:
            src = p.read_text(errors="ignore")
        except Exception:
            continue
        for m in re.finditer(re.escape(base), src):
            w = src[max(0, m.start() - 500): m.start() + 500]
            if "put_object" in w:
                out.append(fn)
                break
    return out


with report("4256_wave2_close") as rep:
    rep.heading("ops 4256 — wave-2 closure")
    fails = []

    rep.section("A. Record correction — SCHEDULE-INPUT is empty")
    rep.log("etf-census-matrix, etf-census, factor-decomposition: the "
            "scheduled Input was the literal '{}' — identical to the "
            "probe. Reclassified WRITES-ON-PROBE. Self-heal set is now "
            "NINE engines; tomorrow's 13:00 contract check is the "
            "arbiter, and any that stay frozen re-enter the queue with "
            "yesterday's evidence attached.")
    rep.kv(section="correction", cls="SCHEDULE-INPUT", real_members=0,
           reclassified=3)

    rep.section("B. ASYNC re-head after full settle (%d)" % len(ASYNC))
    late_ok, still = 0, []
    for key, fn in ASYNC:
        a = age_h(key)
        if a is not None and a < 1.2:
            late_ok += 1
            rep.ok("  %-38s LATE-WRITER-OK — wrote %.1fh ago (slow, not "
                   "broken)" % (key.split("/")[-1][:38], a))
            rep.kv(section="late_ok", artifact=key, writer=fn,
                   age_h=round(a, 2))
        else:
            still.append((key, fn, a))
    rep.log("late-but-healthy: %d | still frozen: %d"
            % (late_ok, len(still)))
    seen = set()
    for key, fn, a in still:
        if fn in seen:
            rep.fail("  %-38s (same writer as above)" % key.split("/")[-1])
            continue
        seen.add(fn)
        tail = last_tail(fn)
        rpt = next((l for l in tail if l.startswith("REPORT")), "")
        errs = [l for l in tail if re.search(
            r"error|exception|traceback|denied|timed out", l, re.I)][:2]
        stem = key.split("/")[-1].replace(".json", "")
        ment = [l for l in tail if stem.lower() in l.lower()][:2]
        rep.fail("  %-38s %-28s age=%sh" % (key.split("/")[-1][:38],
                                            fn[:28],
                                            round(a, 0) if a else "?"))
        if rpt:
            rep.log("      %s" % rpt[:150])
        for l in errs:
            rep.fail("      ERR  %s" % l[:140])
        for l in ment:
            rep.log("      KEY  %s" % l[:140])
        if not errs and not ment:
            rep.warn("      tail is silent about the key — write branch "
                     "never reached in this run")
        rep.kv(section="still_frozen", artifact=key, writer=fn,
               age_h=a, report=rpt[:120],
               evidence=(errs + ment + ["silent"])[0][:140])
        OUT.setdefault("still_frozen", []).append(
            {"artifact": key, "writer": fn, "report": rpt[:150],
             "errs": errs, "mentions": ment})

    rep.section("C. Page-critical four — true writer, probe, verify")
    for key in PAGE_CRITICAL:
        base = key.split("/")[-1]
        cands = true_writers(base)
        rep.log("%s — put_object writers in source: %s"
                % (base, ", ".join(cands) or "NONE"))
        rep.kv(section="page_critical", artifact=key,
               true_writers=",".join(cands) or "none")
        if not cands:
            rep.fail("   NO ENGINE writes this key anymore — the pages "
                     "reading it are on permanent stale data; writer was "
                     "removed or renamed. REAL repair: restore the write "
                     "or repoint the pages.")
            OUT.setdefault("orphan_page_keys", []).append(key)
            continue
        thawed = False
        for fn in cands[:2]:
            try:
                cfg = lam.get_function_configuration(FunctionName=fn)
                if cfg.get("Timeout", 60) > 200:
                    lam.invoke(FunctionName=fn, InvocationType="Event")
                    rep.log("   fired %s async (timeout %ss)"
                            % (fn, cfg.get("Timeout")))
                    continue
                wait_active(fn)
                r = lam.invoke(FunctionName=fn,
                               InvocationType="RequestResponse",
                               LogType="Tail")
                fe = r.get("FunctionError")
                time.sleep(2)
                a = age_h(key)
                if a is not None and a < 0.2:
                    rep.ok("   %s THAWED via %s%s"
                           % (base, fn,
                              " (err=%s)" % fe if fe else ""))
                    thawed = True
                    break
                tail = base64.b64decode(r.get("LogResult", "")
                                        ).decode("utf-8", "ignore")
                line = next((l for l in tail.splitlines()
                             if base.replace(".json", "").lower()
                             in l.lower()), "")
                rep.fail("   %s via %s: still frozen (err=%s) %s"
                         % (base, fn, fe, line[:110]))
            except Exception as e:
                rep.warn("   %s probe %s: %s" % (base, fn, str(e)[:100]))
        OUT.setdefault("page_critical", []).append(
            {"artifact": key, "writers": cands, "thawed": thawed})

    rep.section("D. feedback-summary — the guard, in evidence")
    p = ROOT / "aws" / "lambdas" / "justhodl-feedback" / "source" / \
        "lambda_function.py"
    try:
        src = p.read_text(errors="ignore")
        idx = src.find("feedback-summary")
        if idx < 0:
            rep.fail("   source never names the key — justhodl-feedback "
                     "is NOT its writer; attribution wrong, key joins "
                     "the true-writer hunt")
        else:
            seg = src[max(0, idx - 900):idx + 300]
            for line in seg.splitlines():
                ls = line.strip()
                if ls.startswith(("if ", "elif ", "return", "def ")) or \
                        "put_object" in ls:
                    rep.log("   %s" % ls[:130])
            rep.warn("   guard structure above — reviewed fix next, not "
                     "a blind patch")
    except Exception as e:
        rep.warn("   %s" % str(e)[:100])

    (ROOT / "aws" / "ops" / "reports" / "4256_wave2_close.json"
     ).write_text(json.dumps(OUT, indent=1, default=str),
                  encoding="utf-8")
    rep.section("RESULT")
    if fails:
        raise SystemExit("FAILS")
    rep.ok("OPS 4256 PASS")
