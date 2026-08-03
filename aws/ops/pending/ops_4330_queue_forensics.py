"""ops_4330 -- queue forensics: (A) exact tracebacks for the two
revival holdouts (liquidity-flow, feed-catalog) from their 19:33
fires; (B) pump-radar-summary writer + byte signature (0x8b gzip);
(C) dead-leg compute sites for short_squeeze /
expected_to_outgrow_industry / out_tok (+ CAPEX_ACCEL tag origin for
buildout_threat)."""
import json, subprocess, sys, time
import boto3
from ops_report import report
logs = boto3.client("logs", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"

def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=25).stdout[:1100]
    except Exception as e:
        return "sh: %s" % e

def tail_errors(fn, mins=90):
    try:
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/" + fn,
            startTime=int((time.time() - mins * 60) * 1000),
            filterPattern='?Traceback ?ERROR ?Error ?"Task timed"')
        out = []
        for e in (ev.get("events") or [])[-14:]:
            out.append(e["message"].rstrip()[:200])
        return out or ["no error-pattern events in window"]
    except Exception as e:
        return ["logs: %s" % str(e)[:100]]
with report("4330_queue_forensics") as r:
    r.heading("ops 4330 -- the queue names its bugs")
    r.section("A. revival holdouts -- tracebacks")
    for fn in ("justhodl-liquidity-flow", "justhodl-feed-catalog"):
        r.log(fn + ":")
        for ln in tail_errors(fn):
            r.log("  " + ln)
    r.section("B. pump-radar-summary -- writer + bytes")
    head = s3.get_object(Bucket=B,
                         Key="data/pump-radar-summary.json",
                         Range="bytes=0-15")["Body"].read()
    r.log("first bytes: %s" % head.hex())
    r.log("writers: %s" % sh(["grep", "-rln",
                              "pump-radar-summary",
                              "aws/lambdas/"]).replace("\n", " "))
    who = [x for x in sh(["grep", "-rln", "pump-radar-summary",
                          "aws/lambdas/"]).splitlines()
           if x.endswith(".py")]
    if who:
        r.log(sh(["grep", "-n", "-B2", "-A4",
                  "pump-radar-summary", who[0]])[:900])
    r.section("C. dead-leg compute sites")
    for field, hint in (("short_squeeze", "ai-rerating"),
                        ("expected_to_outgrow_industry",
                         "opportunit"),
                        ("out_tok", "llm-cost"),
                        ("CAPEX_ACCEL", "best-setups")):
        files = sh(["grep", "-rln", field, "aws/lambdas/"])
        tgt = ""
        for ln in files.splitlines():
            if hint in ln:
                tgt = ln
                break
        tgt = tgt or (files.splitlines() or [""])[0]
        r.log("%s -> %s" % (field, tgt))
        if tgt:
            r.log(sh(["grep", "-n", "-B3", "-A6", field,
                      tgt])[:950])
    r.ok("forensics complete")
    if False:
        sys.exit(1)
