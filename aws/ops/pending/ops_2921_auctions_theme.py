#!/usr/bin/env python3
"""ops 2921 — treasury-auctions mirror live + Amber theme phase-1 live."""
import json, sys, time, urllib.request
from datetime import datetime, timezone
sys.path.insert(0, "aws/ops")
from ops_report import report
import boto3

def get(u, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(u, headers={"User-Agent": "Mozilla/5.0 jh"}), timeout=to)
        return r.getcode(), r.read()
    except Exception:
        return None, b""

ok_all = True
out = {}
with report("2921") as r:
    r.section("auctions mirror")
    lam = boto3.client("lambda", region_name="us-east-1")
    s3 = boto3.client("s3")
    t0 = datetime.now(timezone.utc)
    lam.invoke(FunctionName="justhodl-sovereign-fiscal", InvocationType="Event", Payload=b"{}")
    fresh = False
    for i in range(12):
        time.sleep(12)
        try:
            h = s3.head_object(Bucket="justhodl-dashboard-live", Key="data/treasury-auctions.json")
            if h["LastModified"] >= t0:
                fresh = True
                out["mirror_bytes"] = h["ContentLength"]
                break
        except Exception:
            pass
    ok_all &= fresh
    (r.ok if fresh else r.fail)(f"S3 mirror fresh after ~{(i+1)*12}s ({out.get('mirror_bytes')}B)")
    c, b = get(f"https://justhodl.ai/data/treasury-auctions.json?t={int(time.time())}")
    d = json.loads(b.decode()) if c == 200 else {}
    shape = isinstance(d.get("next"), str) and isinstance(d.get("upcoming"), list) and len(d["upcoming"]) > 0
    ok_all &= shape
    out["auctions"] = {"http": c, "next": d.get("next"), "n_upcoming": len(d.get("upcoming", []))}
    (r.ok if shape else r.fail)(f"live feed: {out['auctions']}")

    r.section("theme phase-1")
    c, b = get(f"https://justhodl.ai/jh-theme.css?t={int(time.time())}")
    th = c == 200 and b"--jh-amber" in b
    ok_all &= th
    out["theme_css"] = {"http": c, "bytes": len(b)}
    (r.ok if th else r.fail)(f"jh-theme.css {c} {len(b)}B tokens={th}")
    c, b = get(f"https://justhodl.ai/jh-nav-drawer.js?t={int(time.time())}")
    dj = b.decode("utf-8", "replace")
    inj = ('jh-theme' in dj) and ('/screener' in dj)
    ok_all &= inj
    out["drawer"] = {"http": c, "inject": 'jh-theme' in dj, "screener_guard": '/screener' in dj}
    (r.ok if inj else r.fail)(f"drawer: {out['drawer']}")
    for att in range(8):
        c, b = get(f"https://justhodl.ai/index.html?t={int(time.time())}")
        if b'JH_V="v1.2.5"' in b:
            break
        time.sleep(15)
    v = b'JH_V="v1.2.5"' in b
    ok_all &= v
    out["sw"] = {"v1.2.5_live": v, "attempt": att + 1}
    (r.ok if v else r.fail)(f"index v1.2.5 attempt {att+1}")

    json.dump(out, open("aws/ops/reports/2921.json", "w"), indent=2, default=str)
    r.ok("report -> 2921.json")
print("DONE 2921", "PASS" if ok_all else "FAIL")
sys.exit(0 if ok_all else 1)
