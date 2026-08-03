"""ops_4310 -- trend-reversal v2: census universe (~550 names) via
self-chain, 14-signal ensemble, weekly confirmation, stages, sector
breadth heatmap, movers. Gate: chain completes, universe>=420,
sectors>=8, breadth real, page v2 markers on edge; desk chips intact."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from ops_report import report
REGION, B = "us-east-1", "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
RUN_START = datetime.now(timezone.utc)

def git_floor(d):
    try:
        out = subprocess.run(["git", "log", "-1", "--format=%ct",
                              "--", "aws/lambdas/%s" % d],
                             capture_output=True, text=True,
                             timeout=30).stdout.strip()
        return datetime.fromtimestamp(int(out), tz=timezone.utc)
    except Exception:
        return None

fails = []
with report("4310_reversal_v2") as r:
    r.heading("ops 4310 -- reversal, exponentially")
    FN = "justhodl-trend-reversal"
    fl = git_floor(FN) or RUN_START
    ok = False
    for _ in range(55):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            lm = datetime.strptime(
                c["LastModified"].split(".")[0],
                "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                ok = True
                break
        except Exception:
            pass
        time.sleep(9)
    if not ok:
        fails.append("v2 never reached git floor")
    else:
        p = lam.invoke(FunctionName=FN,
                       InvocationType="RequestResponse",
                       Payload=b"{}")
        r.log("root: %s" % (p["Payload"].read() or b"")[:160]
              .decode("utf-8", "ignore"))
        doc = None
        for i in range(28):  # chain ~5 links
            time.sleep(22)
            try:
                doc = json.loads(s3.get_object(
                    Bucket=B,
                    Key="data/trend-reversal.json")["Body"].read())
                if doc.get("version") == "2.0" and \
                        (doc.get("universe_n") or 0) >= 420:
                    break
            except Exception:
                pass
        if not doc or doc.get("version") != "2.0":
            fails.append("v2 doc never finalized (last=%s n=%s)"
                         % ((doc or {}).get("version"),
                            (doc or {}).get("universe_n")))
        else:
            br = doc.get("breadth") or {}
            r.ok("UNIVERSE %s · hot %s · breadth top %s%% / "
                 "bottom %s%%"
                 % (doc.get("universe_n"), doc.get("hot_n"),
                    br.get("top_pct"), br.get("bottom_pct")))
            r.log("stages: %s" % doc.get("stages"))
            secs = doc.get("sectors") or []
            r.log("sectors (%d): %s"
                  % (len(secs),
                     [(s0["sector"], s0["top_pct"],
                       s0["bottom_pct"]) for s0 in secs[:6]]))
            r.log("top5: %s"
                  % [(x["ticker"], x["reversal_score"],
                      x.get("stage"), x.get("weekly_confirm"))
                     for x in (doc.get("rows") or [])[:5]])
            r.log("movers: %s" % (doc.get("movers") or [])[:5])
            if (doc.get("universe_n") or 0) < 420:
                fails.append("universe %s < 420"
                             % doc.get("universe_n"))
            if len(secs) < 8:
                fails.append("sectors %d < 8" % len(secs))
            if br.get("top_pct") is None:
                fails.append("breadth missing")
            wk_any = any(x.get("weekly_confirm")
                         for x in doc.get("rows") or [])
            r.log("weekly-confirm present on >=1 name: %s" % wk_any)
    if not fails:
        body = ""
        for _ in range(12):
            try:
                body = urllib.request.urlopen(
                    urllib.request.Request(
                        "https://justhodl.ai/trend-reversal.html",
                        headers={"User-Agent": "ops/4310",
                                 "Cache-Control": "no-cache"}),
                    timeout=25).read().decode("utf-8", "ignore")
                if "market-turn gauge" in body:
                    break
            except Exception:
                pass
            time.sleep(20)
        marks = ["market-turn gauge", "secgrid", "Movers",
                 "weekly ✓", "spark", "fstage"]
        miss = [x for x in marks if x not in body]
        if miss:
            fails.append("edge missing %s" % miss)
        else:
            r.ok("page v2 LIVE (%d bytes)" % len(body))
    r.section("RESULT")
    if fails:
        for f in fails:
            r.fail("  %s" % f)
    else:
        r.ok("OPS 4310 PASS -- reversal radar at census scale")
if fails:
    sys.exit(1)

# retrigger: universe sourcing fixed (census multi-key incl matrix{}, +NDX constituents, +ETF/FX/FUT/CRYPTO classes tagged through chain)

# retrigger: columnar census zip (tickers/sectors parallel lists) + finalize dedupe
