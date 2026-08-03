"""ops_4323 -- Fundamental-Graphs forensics: locate the API engine
(repo-wide), print its P/E-series construction lines, and call the
live API for TSM to confirm the 1.10x series + its field name."""
import json, subprocess, sys, urllib.request
from ops_report import report
def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=30).stdout[:1500]
    except Exception as e:
        return "sh: %s" % e
with report("4323_fg_forensics") as r:
    r.heading("ops 4323 -- the chart's 1.10x, sourced")
    who = sh(["grep", "-rln", "fundgraph", "aws/lambdas/"])
    r.log("fundgraph writers:\n%s" % who)
    eng = ""
    for ln in who.strip().splitlines():
        if ln.endswith("lambda_function.py"):
            eng = ln
            break
    if not eng:
        r.fail("no engine found")
        sys.exit(1)
    r.ok("ENGINE: %s" % eng)
    for pat, lbl in (("pe_ttm", "pe_ttm lines"),
                     ("eps", "eps usage"),
                     ("reportedCurrency\\|currency", "currency handling"),
                     ("def .*metric\\|METRICS\\|SERIES", "metric table")):
        r.log("%s:\n%s" % (lbl, sh(["grep", "-n", "-B2", "-A4",
                                    pat, eng])[:1200]))
    api = ("https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy"
           ".lambda-url.us-east-1.on.aws/?symbol=TSM&period=annual")
    try:
        d = json.loads(urllib.request.urlopen(api, timeout=30).read())
        keys = list(d)[:12]
        r.log("API top keys: %s" % keys)
        ser = None
        def dig(o, path="", depth=0):
            global ser
            if ser or depth > 3:
                return
            if isinstance(o, dict):
                for k, v2 in o.items():
                    if k == "pe_ttm" or k == "pe":
                        ser = (path + "/" + k, v2)
                        return
                    dig(v2, path + "/" + k, depth + 1)
        dig(d)
        if ser:
            p, v2 = ser
            tail = v2[-4:] if isinstance(v2, list) else v2
            r.ok("pe series at %s -- last points: %s"
                 % (p, json.dumps(tail)[:300]))
        else:
            r.log("pe key not found at depth<=3; sample: %s"
                  % json.dumps(d)[:400])
    except Exception as e:
        r.warn("API call: %s" % str(e)[:120])
    r.ok("forensics done")
