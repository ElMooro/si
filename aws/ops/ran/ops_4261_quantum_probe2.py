"""
ops_4261 -- micro-probe #2: the exact trend/breakout/asym payload of
risk-asset compass rows (4259 sampled row0=CASH, degenerate "N/A"), and
forward-returns' top shape. The discount leg -- Khalid's first lens --
did not wire in v1.0.2 (basis None on all 14 rows); parse the real
strings, don't guess them. Read-only.
"""
import json, sys
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
ok = True
with report("4261_quantum_probe2") as r:
    r.heading("ops 4261 -- trend/asym + forward-returns shape probe")
    try:
        ac = json.loads(s3.get_object(
            Bucket=B, Key="data/asset-compass.json")["Body"].read())
        for row in (ac.get("assets") or [])[:31]:
            if str(row.get("class", "")).lower() in ("cash", "tbill"):
                continue
            r.log("ticker=%s class=%s price=%s" %
                  (row.get("ticker"), row.get("class"), row.get("price")))
            for k in ("trend", "breakout", "asym", "horizon"):
                r.log("  %s = %s" % (k, json.dumps(row.get(k),
                                                   default=str)[:400]))
            break
        # and one more with a different class for contrast
        seen = None
        for row in (ac.get("assets") or []):
            c = str(row.get("class", "")).lower()
            if c not in ("cash", "tbill") and seen and c != seen:
                r.log("ticker=%s class=%s trend=%s asym=%s" %
                      (row.get("ticker"), c,
                       json.dumps(row.get("trend"), default=str)[:250],
                       json.dumps(row.get("asym"), default=str)[:250]))
                break
            if c not in ("cash", "tbill"):
                seen = c
    except Exception as e:
        ok = False
        r.fail("compass probe: %s" % str(e)[:200])
    try:
        fr = json.loads(s3.get_object(
            Bucket=B, Key="data/forward-returns.json")["Body"].read())
        r.log("forward-returns top keys: %s" % list(fr)[:20])
        for k in list(fr)[:20]:
            v = fr[k]
            if isinstance(v, list) and v and isinstance(v[0], dict):
                r.log("  list '%s' row0: %s"
                      % (k, json.dumps(v[0], default=str)[:500]))
            elif isinstance(v, dict) and v:
                k2 = next(iter(v))
                r.log("  dict '%s'['%s']: %s"
                      % (k, k2, json.dumps(v[k2], default=str)[:400]))
    except Exception as e:
        ok = False
        r.fail("forward-returns probe: %s" % str(e)[:200])
    r.ok("probe2 complete") if ok else r.fail("probe2 had errors")
if not ok:
    sys.exit(1)
