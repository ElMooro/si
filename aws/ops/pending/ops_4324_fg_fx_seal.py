"""ops_4324 -- FX seal on fundamental-graphs: TSM's ratio family must
read in USD terms (P/E ~27x, not 0.9x); AAPL is the untouched control."""
import json, subprocess, sys, time, urllib.request
from datetime import datetime, timezone
import boto3
from ops_report import report
lam = boto3.client("lambda", region_name="us-east-1")
API = ("https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy"
       ".lambda-url.us-east-1.on.aws/?symbol=%s&period=annual")
RUN_START = datetime.now(timezone.utc)
def floor_ok():
    try:
        ts = subprocess.run(["git", "log", "-1", "--format=%ct", "--",
                             "aws/lambdas/justhodl-fundamental-graphs"],
                            capture_output=True, text=True,
                            timeout=30).stdout.strip()
        fl = datetime.fromtimestamp(int(ts), tz=timezone.utc)
    except Exception:
        fl = RUN_START
    for _ in range(50):
        try:
            c = lam.get_function_configuration(
                FunctionName="justhodl-fundamental-graphs")
            lm = datetime.strptime(c["LastModified"].split(".")[0],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
            if c.get("LastUpdateStatus") in (None, "Successful") \
                    and lm >= fl:
                return True
        except Exception:
            pass
        time.sleep(9)
    return False
def pe_last(sym):
    d = json.loads(urllib.request.urlopen(API % sym,
                                          timeout=60).read())
    pts = ((d.get("points") or {}).get("pe_ttm")) or []
    return d, pts[-6:]
fails = []
with report("4324_fg_fx_seal") as r:
    r.heading("ops 4324 -- the chart learns currencies")
    if not floor_ok():
        fails.append("deploy floor")
    else:
        d, tail = pe_last("TSM")
        r.ok("TSM pe_ttm tail: %s" % json.dumps(tail))
        last = tail[-1][1] if tail else None
        if not (last and 15 <= last <= 45):
            fails.append("TSM pe last=%s not in [15,45]" % last)
        d2, tail2 = pe_last("AAPL")
        r.ok("AAPL control pe_ttm tail: %s" % json.dumps(tail2))
        l2 = tail2[-1][1] if tail2 else None
        if not (l2 and 20 <= l2 <= 55):
            fails.append("AAPL control off: %s" % l2)
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4324 PASS -- TSM finally charts like a 27x business")
