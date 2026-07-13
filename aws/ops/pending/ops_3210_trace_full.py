"""ops 3210 — the FULL traceback (3209's tail truncated the frames), the
shape guards deployed, and the ledger count settled. The error message
matches a 3-way sid split (_cot 'ds|code|field' class); guards now make
every multi-part split return {} on malformed shape instead of raising —
per the 3200 doctrine, one bad row must never surface as an invocation
[ERROR] again. Also fixes 3208's race: the DDB scan ran before the
emission finished."""
import json
import sys
import time
from pathlib import Path

import boto3
from boto3.dynamodb.conditions import Attr

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
LOGS = boto3.client("logs", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
DDB = boto3.resource("dynamodb", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]
CONSUMERS = ("justhodl-wl-engines", "justhodl-thesis-engine",
             "justhodl-symbol-dictionary")

with report("3210_trace_full") as rep:
    fails, warns = [], []
    rep.heading("ops 3210 — full traceback + shape guards + ledger settled")

    rep.section("1. The complete [ERROR] frames")
    try:
        grp = "/aws/lambda/justhodl-wl-engines"
        shown = 0
        for st in LOGS.describe_log_streams(
                logGroupName=grp, orderBy="LastEventTime",
                descending=True, limit=4).get("logStreams") or []:
            for e in LOGS.get_log_events(
                    logGroupName=grp, logStreamName=st["logStreamName"],
                    limit=200, startFromHead=False).get("events") or []:
                m = e.get("message") or ""
                if "ValueError" in m and "unpack" in m:
                    for ln in m.splitlines()[:14]:
                        rep.log("  " + ln[:150])
                    shown += 1
                    break
            if shown:
                break
        if not shown:
            rep.log("  (no unpack ValueError in the last 4 streams)")
    except Exception as e:
        warns.append(f"logs: {str(e)[:80]}")

    rep.section("2. Deploy shape-guarded shared bundle")
    for fn in CONSUMERS:
        try:
            cfg = {}
            p = AWS_DIR / "lambdas" / fn / "config.json"
            if p.exists():
                cfg = json.loads(p.read_text())
            sch = cfg.get("schedule")
            rule, cron = (sch.get("rule_name"), sch.get("cron")) \
                if isinstance(sch, dict) else (None, None)
            live = (LAM.get_function_configuration(FunctionName=fn)
                    .get("Environment") or {}).get("Variables") or {}
            deploy_lambda(report=rep, function_name=fn,
                          source_dir=AWS_DIR / "lambdas" / fn / "source",
                          env_vars=live, eb_rule_name=rule,
                          eb_schedule=cron,
                          timeout=cfg.get("timeout", 900),
                          memory=cfg.get("memory", 1024),
                          description=str(cfg.get("description", ""))[:250],
                          smoke=False)
        except Exception as e:
            fails.append(f"deploy {fn}: {str(e)[:80]}")

    rep.section("3. Ledger count, post-race")
    try:
        tbl = DDB.Table("justhodl-signals")
        r = tbl.scan(FilterExpression=Attr("signal_type")
                     .begins_with("wl_"),
                     ProjectionExpression="signal_id", Limit=2000)
        n = len(r.get("Items") or [])
        rep.kv(wl_signals_in_ledger=n)
        if n >= 20:
            rep.ok(f"{n} wl_ signals pending — the scorecard is learning "
                   "his panels")
        elif n == 0:
            fails.append("ledger still empty despite 'emitted: 24' log")
    except Exception as e:
        fails.append(f"ddb: {str(e)[:80]}")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
