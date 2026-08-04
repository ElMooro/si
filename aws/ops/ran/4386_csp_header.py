"""ops 4386 — Cloudflare token verification: the CSP header lands.

Khalid widened the runner token (Zone > Transform Rules: Edit). Re-run the
exact path that 403'd in 4383: zone lookup -> response-headers-transform
entrypoint -> upsert rule 'a2a-csp-header' (host set covers apex + www),
then prove it with a live HEAD against the edge, post the fix turn on
thread 0003 with the captured headers for Perplexity's curl -I
confirm-close (invariant B — not mine to close), and fan out.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3
from botocore.config import Config

REGION = "us-east-1"
BUS = "justhodl-a2a-bus"
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=280, retries={"max_attempts": 0}))
R = {"ops": 4386, "started": datetime.now(timezone.utc).isoformat()}

CSP = ("default-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com;"
       " connect-src 'self' https://justhodl-dashboard-live.s3.amazonaws.com"
       " https://api.telegram.org; img-src 'self' data: https:; style-src"
       " 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'"
       " https://cdnjs.cloudflare.com; object-src 'none'; base-uri 'none';"
       " frame-ancestors 'self'")


def cf(path, method="GET", body=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    req = urllib.request.Request(
        "https://api.cloudflare.com/client/v4" + path, method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read().decode())


try:
    zid = cf("/zones?name=justhodl.ai")["result"][0]["id"]
    R["zone"] = zid[:8] + "…"
    phase = "http_response_headers_transform"
    rule = {"ref": "a2a-csp-header",
            "expression": 'http.host in {"justhodl.ai" "www.justhodl.ai"}',
            "description": "ops4386: CSP response header (closes thread "
                           "0003, critique dd761114)",
            "action": "rewrite",
            "action_parameters": {"headers": {
                "Content-Security-Policy": {"operation": "set",
                                            "value": CSP}}},
            "enabled": True}
    try:
        rs = cf(f"/zones/{zid}/rulesets/phases/{phase}/entrypoint")["result"]
    except Exception:
        rs = None
    if rs and rs.get("id"):
        rules = [x for x in (rs.get("rules") or [])
                 if x.get("ref") != "a2a-csp-header"
                 and "a2a-csp" not in (x.get("description") or "")]
        rules.append(rule)
        cf(f"/zones/{zid}/rulesets/{rs['id']}", "PUT", {"rules": rules})
        R["cf_rule"] = "upserted into existing entrypoint"
    else:
        cf(f"/zones/{zid}/rulesets", "POST",
           {"name": "a2a response headers", "kind": "zone", "phase": phase,
            "rules": [rule]})
        R["cf_rule"] = "created new entrypoint ruleset"
except Exception as e:
    R["cf_err"] = f"{type(e).__name__}: {str(e)[:250]}"

time.sleep(12)
R["probes"] = {}
for u in ("https://justhodl.ai/insiders.html", "https://justhodl.ai/"):
    try:
        req = urllib.request.Request(u, method="HEAD",
                                     headers={"User-Agent": "ops4386"})
        with urllib.request.urlopen(req, timeout=15) as r:
            csp = r.headers.get("Content-Security-Policy")
            R["probes"][u] = {"status": r.status,
                              "csp_present": bool(csp),
                              "csp_head": (csp or "")[:120],
                              "frame_ancestors":
                                  "frame-ancestors" in (csp or "")}
    except Exception as e:
        R["probes"][u] = {"err": str(e)[:100]}

live = any(p.get("csp_present") and p.get("frame_ancestors")
           for p in R["probes"].values())


def bus(payload):
    inv = lam.invoke(FunctionName=BUS, InvocationType="RequestResponse",
                     Payload=json.dumps(payload).encode())
    b = json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b, dict) and "body" in b \
        else b


if live:
    t = bus({"action": "post_turn", "thread_id": "0003-csp-meta",
             "from": "claude", "to": "perplexity", "kind": "propose",
             "content": "Blocker cleared: Khalid widened the token scope; "
                        "the Cloudflare transform rule a2a-csp-header is "
                        "installed (apex + www) and the REAL response "
                        "header is live at the edge — captured this run: "
                        f"{json.dumps(R['probes'])[:700]}. frame-ancestors "
                        "'self' now enforced at the header layer per your "
                        "critique dd761114. Your move: curl -I, then "
                        "verify verdict:confirmed so we can resolve — "
                        "invariant B says this close is yours, not mine.",
             "evidence": [{"kind": "url",
                           "ref": "https://justhodl.ai/insiders.html"}]})
else:
    t = bus({"action": "post_turn", "thread_id": "0003-csp-meta",
             "from": "claude", "to": "*", "kind": "block",
             "content": "Token widened but header still not observed at "
                        f"edge: {json.dumps(R.get('cf_err') or R['probes'])[:400]}"})
bus({"action": "fanout_pending"})
R["fix_turn"] = t.get("ok") or t.get("error")

R["verdict"] = ("PASS — CSP response header live at the edge, "
                "confirm-close handed to Perplexity"
                if live and t.get("ok") else "PARTIAL — see fields")
R["finished"] = datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports", exist_ok=True)
json.dump(R, open("aws/ops/reports/4386_csp_header.json", "w"),
          indent=1, default=str)
open("aws/ops/reports/4386_csp_header.md", "w").write(
    f"# ops 4386 — CSP header verification — {R['verdict']}\n"
    f"- rule: {R.get('cf_rule') or R.get('cf_err')}\n"
    f"- probes: {json.dumps(R['probes'], indent=1)}\n"
    f"- fix turn: {R['fix_turn']}\n")
print(json.dumps(R, indent=1, default=str)[:1800])
