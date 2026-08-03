"""ops_4309 -- rehypo page v3 on edge: expandable crisis-annotated
chart w/ per-leg z lines + era ledger + keyids + magnitude formats.
Also asserts the long artifact carries everything the page renders."""
import json, sys, time, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
fails = []
with report("4309_page_v3") as r:
    r.heading("ops 4309 -- the whole engine on the page")
    L = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="data/treasury-rehypo-long.json")["Body"].read())
    wk = L.get("weekly") or []
    zsample = next((w for w in reversed(wk)
                    if (w.get("z") or {}).get("fails") is not None),
                   {})
    r.log("long: start %s · %d wk · legs_available=%s"
          % (L.get("actual_start"), len(wk),
             list((L.get("legs_available") or {}))))
    r.log("z-sample %s: %s" % (zsample.get("d"), zsample.get("z")))
    if len(wk) < 450 or not zsample:
        fails.append("long artifact thin")
    body = ""
    for _ in range(12):
        try:
            body = urllib.request.urlopen(urllib.request.Request(
                "https://justhodl.ai/treasury-rehypo.html",
                headers={"User-Agent": "ops/4309",
                         "Cache-Control": "no-cache"}),
                timeout=25).read().decode("utf-8", "ignore")
            if "Expand" in body and "LTCM" in body:
                break
        except Exception:
            pass
        time.sleep(20)
    marks = ["⤢ Expand", "LTCM", "Repo spike", "mchart",
             "era coverage", "keyids in use", "leg z scale"]
    miss = [x for x in marks if x not in body]
    if miss:
        fails.append("edge missing %s" % miss)
    else:
        r.ok("page v3 LIVE (%d bytes) -- all markers" % len(body))
    if fails:
        for f in fails:
            r.fail("  %s" % f)
        sys.exit(1)
    r.ok("OPS 4309 PASS")
