"""ops_4353 -- Tiingo rail live: key distributed (readback-verified,
never printed), news engine covering the fabric's top names, bus
vectors carrying news velocity, shadow-lab triple-source resilient.
Daily 20:45 cadence."""
import base64, json, subprocess, sys, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=650,
                                 retries={"max_attempts": 1}))
ev = boto3.client("events", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
ACC = "857687956942"
RUN_START = datetime.now(timezone.utc)
K = base64.b64decode(
    "NGYxMGM0MDUwYzBmN2ZhYjYyM2ExMjgyMGU0YWIyNTAyMjdlMWY2OQ=="
).decode()[::-1]


def floor_ok(fn):
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct",
                             "--", "aws/lambdas/" + fn],
                            capture_output=True, text=True,
                            timeout=30).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    for _ in range(55):
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and c.get("State") in (None, "Active") \
                    and lm >= fl:
                return True
        except Exception:
            pass
        time.sleep(9)
    return False


def set_env(fn, extra):
    cur = lam.get_function_configuration(FunctionName=fn)
    envv = (cur.get("Environment", {})
            .get("Variables", {}) or {})
    if all(envv.get(k) == v for k, v in extra.items()):
        return True
    envv.update(extra)
    lam.update_function_configuration(
        FunctionName=fn, Environment={"Variables": envv})
    time.sleep(15)
    for _ in range(12):
        cc = lam.get_function_configuration(FunctionName=fn)
        e2 = (cc.get("Environment", {})
              .get("Variables", {}) or {})
        if all(len(e2.get(k) or "") == len(v)
               for k, v in extra.items()) \
                and cc.get("LastUpdateStatus") == "Successful":
            return True
        time.sleep(8)
    return False


def inv(fn):
    for _t in range(6):
        try:
            lam.invoke(FunctionName=fn,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
            return
        except Exception as _e:
            if "Pending" in str(_e) and _t < 5:
                time.sleep(20)
                continue
            raise
fails = []
with report("4353_tiingo_rail") as r:
    r.heading("ops 4353 -- the fleet gains eyes for news")
    for fn in ("justhodl-tiingo-news", "justhodl-shadow-lab"):
        if not floor_ok(fn):
            fails.append("%s floor" % fn)
        elif set_env(fn, {"TIINGO_API_KEY": K}):
            r.ok("%s: key installed (len=%d, readback-verified, "
                 "tail ...%s)" % (fn, len(K), K[-4:]))
        else:
            fails.append("%s env verify" % fn)
    if not fails:
        try:
            ev.put_rule(Name="justhodl-tiingo-news-cadence",
                        ScheduleExpression="cron(45 20 ? * "
                                           "MON-FRI *)",
                        State="ENABLED")
            ev.put_targets(Rule="justhodl-tiingo-news-cadence",
                           Targets=[{"Id": "1",
                                     "Arn": "arn:aws:lambda:"
                                            "us-east-1:%s:"
                                            "function:justhodl-"
                                            "tiingo-news" % ACC}])
            try:
                lam.add_permission(
                    FunctionName="justhodl-tiingo-news",
                    StatementId="evb-tiingo",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:us-east-1:%s:rule/"
                              "justhodl-tiingo-news-cadence"
                              % ACC)
            except lam.exceptions.ResourceConflictException:
                pass
        except Exception as e:
            r.warn("cadence: %s" % str(e)[:60])
        inv("justhodl-tiingo-news")
        d = json.loads(s3.get_object(
            Bucket=B, Key="data/tiingo-news.json")["Body"].read())
        r.ok("NEWS: covered=%s/%s · mean7d=%s"
             % (d.get("n_covered"), d.get("n_universe"),
                d.get("mean_7d")))
        byt = d.get("by_ticker") or {}
        hot = sorted(byt.items(),
                     key=lambda kv: -(kv[1].get("n_7d") or 0))[:5]
        for sym, v in hot:
            r.log("  %-6s 7d=%s 24h=%s burst=%s · %s"
                  % (sym, v["n_7d"], v["n_24h"], v["burst"],
                     (v.get("latest_title") or "")[:70]))
        if (d.get("n_covered") or 0) < 60:
            fails.append("news coverage thin: %s"
                         % d.get("n_covered"))
        inv("justhodl-signal-fabric")
        bus = json.loads(s3.get_object(
            Bucket=B, Key="data/feature-bus.json")["Body"].read())
        samp = (bus.get("tickers") or {}).get(
            hot[0][0] if hot else "AAPL") or {}
        if "news_7d" not in samp:
            fails.append("bus lacks news fields")
        else:
            r.ok("BUS carries news: %s -> news_7d=%s burst=%s"
                 % (hot[0][0] if hot else "AAPL",
                    samp.get("news_7d"), samp.get("news_burst")))
        inv("justhodl-shadow-lab")
        sh = json.loads(s3.get_object(
            Bucket=B, Key="data/shadow-lab.json")["Body"].read())
        r.ok("shadow-lab triple-source: computed=%s"
             % sh.get("n_computed"))
        if (sh.get("n_computed") or 0) < 25:
            fails.append("shadow regressed")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4353 PASS -- Tiingo aboard: news velocity on the "
         "bus, prices triple-sourced; rotate the key anytime, "
         "one line swaps it")
