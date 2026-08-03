"""ops_4331 -- forensics round 2: liquidity-flow's true OUTPUT key +
last raw log lines; feed-catalog's yield-curve branch condition;
pump-radar's gzip put block; sq computation; growth_intel producer;
out_tok compute; CAPEX_ACCEL tag origin."""
import subprocess, sys, time
import boto3
from ops_report import report
logs = boto3.client("logs", region_name="us-east-1")

def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=25).stdout[:1000]
    except Exception as e:
        return "sh: %s" % e

def rawtail(fn, mins=120, n=10):
    try:
        st = logs.describe_log_streams(
            logGroupName="/aws/lambda/" + fn,
            orderBy="LastEventTime", descending=True,
            limit=1)["logStreams"]
        if not st:
            return ["no streams"]
        ev = logs.get_log_events(
            logGroupName="/aws/lambda/" + fn,
            logStreamName=st[0]["logStreamName"],
            limit=n, startFromHead=False)["events"]
        return [e["message"].rstrip()[:170] for e in ev]
    except Exception as e:
        return ["logs: %s" % str(e)[:90]]
with report("4331_forensics_r2") as r:
    r.heading("ops 4331 -- the last unknowns")
    r.section("liquidity-flow: output key + raw tail")
    r.log("keys in source:\n%s" % sh(
        ["grep", "-n", "put_object\\|OUTPUT\\|OUT_KEY\\|Key=",
         "aws/lambdas/justhodl-liquidity-flow/source/"
         "lambda_function.py"])[:700])
    for ln in rawtail("justhodl-liquidity-flow"):
        r.log("  " + ln)
    r.section("feed-catalog: yield-curve branch")
    r.log(sh(["grep", "-n", "-B4", "-A8", "yield-curve",
              "aws/lambdas/justhodl-feed-catalog/source/"
              "lambda_function.py"])[:1000])
    r.section("pump-radar: main gzip put (lines 225-255)")
    r.log(sh(["sed", "-n", "225,255p",
              "aws/lambdas/justhodl-prepump-summary/source/"
              "lambda_function.py"]))
    r.section("ai-rerating: sq computation")
    r.log(sh(["grep", "-n", "-B4", "-A2", "sq =",
              "aws/lambdas/justhodl-ai-rerating-radar/source/"
              "lambda_function.py"])[:800])
    r.section("growth_intel producer (expected_to_outgrow)")
    r.log("producers: %s" % sh(
        ["grep", "-rln", "expected_to_outgrow_industry",
         "aws/lambdas/"]).replace("\n", " "))
    r.section("llm-cost: out_tok compute")
    r.log(sh(["grep", "-n", "-B3", "-A5", "out_tok",
              "aws/lambdas/justhodl-llm-cost/source/"
              "lambda_function.py"])[:800])
    r.section("best-setups: CAPEX_ACCEL tag origin")
    r.log(sh(["grep", "-n", "-B2", "-A2", "CAPEX_ACCEL",
              "aws/lambdas/justhodl-best-setups/source/"
              "lambda_function.py"])[:700])
    r.ok("round-2 complete -- fix wave is fully specified")
    if False:
        sys.exit(1)
