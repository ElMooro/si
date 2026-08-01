"""
ops_4259 -- probe the REAL shapes of asset-compass rows, best-setups
rows, and the router doc, before wiring v1.0.2. 4258 showed the ladder
scoring on the regime leg alone (all other legs None) and the money map
defaulting every name to US_LARGE -- both mean my field guesses missed.
Probe-then-wire; guessing already cost v1.0.0.
Read-only: prints structure into the report, mutates nothing.
"""
import json, sys
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"

def get(key):
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())

def tsummary(v, d=0):
    if isinstance(v, dict):
        return "{" + ",".join("%s:%s" % (k, tsummary(v[k], d+1))
                              for k in list(v)[:14]) + "}" if d < 2 \
            else "{...%d keys}" % len(v)
    if isinstance(v, list):
        return "[%d x %s]" % (len(v), tsummary(v[0], d+1) if v else "?")
    return type(v).__name__

with report("4259_quantum_probe") as r:
    r.heading("ops 4259 -- shape probe for quantum-desk v1.0.2")
    try:
        ac = get("data/asset-compass.json")
        r.section("asset-compass")
        r.log("top keys: %s" % list(ac)[:20])
        rows = ac.get("assets")
        r.log("assets type: %s" % tsummary(rows))
        row0 = (rows[0] if isinstance(rows, list) and rows else
                next(iter(rows.values())) if isinstance(rows, dict) else None)
        if row0 is not None:
            r.log("row0 keys: %s" % list(row0)[:30])
            r.log("row0: %s" % json.dumps(row0, default=str)[:900])
    except Exception as e:
        r.fail("asset-compass probe: %s" % str(e)[:200])

    try:
        bs = get("data/best-setups.json")
        r.section("best-setups")
        r.log("top keys: %s" % list(bs)[:20])
        for k in list(bs)[:20]:
            if isinstance(bs[k], list) and bs[k] \
                    and isinstance(bs[k][0], dict):
                r.log("list '%s': %s" % (k, tsummary(bs[k])))
        srow = None
        for k in ("setups", "top_setups", "board", "rows"):
            v = bs.get(k)
            if isinstance(v, list) and v:
                srow = v[0]
                break
        if srow is None:
            def find(d, depth=3):
                if depth < 0:
                    return None
                if isinstance(d, list) and d and isinstance(d[0], dict) \
                        and (d[0].get("ticker") or d[0].get("symbol")):
                    return d[0]
                if isinstance(d, dict):
                    for v in d.values():
                        x = find(v, depth-1)
                        if x is not None:
                            return x
                return None
            srow = find(bs)
        if srow:
            r.log("setup row keys: %s" % list(srow)[:40])
            keep = {k: srow[k] for k in srow
                    if k in ("ticker", "name", "sector", "industry",
                             "class", "gics", "conviction", "price",
                             "ma200", "sma200", "dma200",
                             "industry_flow_quadrant")}
            r.log("setup row sample: %s" % json.dumps(keep, default=str)[:700])
    except Exception as e:
        r.fail("best-setups probe: %s" % str(e)[:200])

    try:
        ro = get("data/regime-conditional-router.json")
        r.section("router")
        r.log("top keys: %s" % list(ro)[:24])
        hits = []
        def walk(d, path, depth=4):
            if depth < 0 or len(hits) > 10:
                return
            if isinstance(d, dict):
                for k, v in d.items():
                    if isinstance(v, str) and k.lower() in (
                            "regime", "phase", "state", "label",
                            "primary_regime") and len(v) < 40:
                        hits.append("%s.%s=%s" % (path, k, v))
                    else:
                        walk(v, "%s.%s" % (path, k), depth-1)
            elif isinstance(d, list):
                for i, v in enumerate(d[:6]):
                    walk(v, "%s[%d]" % (path, i), depth-1)
        walk(ro, "$")
        r.log("regime-ish strings: %s" % hits[:10])
    except Exception as e:
        r.fail("router probe: %s" % str(e)[:200])
    r.ok("probe complete -- wire v1.0.2 from these shapes")
