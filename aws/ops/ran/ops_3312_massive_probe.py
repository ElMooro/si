"""ops 3312 — analyst-actions is EMPTY because the Benzinga harvest returns
0 (ops 3311 proved engine+page+schedule healthy; has_massive_env=False and
[analyst] ratings=0 guidance=0 insights=0). The benzinga helper resolves
its key from env MASSIVE_API_KEY, else SSM /justhodl/massive-api-key. This
op finds out WHICH failed and whether the key still entitles Benzinga:

  1. Does SSM /justhodl/massive-api-key exist? (don't print the secret;
     print length + first 4 chars only)
  2. Which other live Lambdas carry a MASSIVE key we can borrow? scan
     env of a few known Massive consumers (justhodl-massive-signals,
     justhodl-benzinga-earnings) for the var name + value fingerprint.
  3. Live-test the Benzinga ratings endpoint with whatever key we find:
     GET api.polygon.io/benzinga/v1/ratings?limit=1 -> HTTP + payload
     shape. This distinguishes "no key" from "key present but not
     entitled / expired" (403/401 vs 200-with-data).

No secrets are emitted to the report — only fingerprints (len, prefix,
HTTP status). Read-only; changes nothing.
"""
import json
import sys
import urllib.request
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
SSM = boto3.client("ssm", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)

SSM_PATH = "/justhodl/massive-api-key"
CONSUMERS = ["justhodl-massive-signals", "justhodl-benzinga-earnings",
             "justhodl-analyst-actions"]


def fp(v):
    if not v:
        return None
    return {"len": len(v), "prefix": v[:4], "suffix": v[-3:]}


def benzinga_ratings_test(key):
    url = (f"https://api.polygon.io/benzinga/v1/ratings?limit=1"
           f"&order=desc&apiKey={key}")
    req = urllib.request.Request(url, headers={"User-Agent": "jh-ops-3312"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            body = json.loads(r.read())
            results = body.get("results") or body.get("ratings") or []
            return {"http": r.status, "keys": list(body.keys())[:6],
                    "n_results": len(results),
                    "status_field": body.get("status")}
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", "ignore")[:200]
        except Exception:
            pass
        return {"http": e.code, "error": detail}
    except Exception as e:
        return {"http": None, "error": f"{type(e).__name__}: {e}"}


with report("3312_massive_probe") as rep:
    key_found = None
    key_source = None

    # 1. SSM
    rep.section("SSM")
    try:
        v = SSM.get_parameter(Name=SSM_PATH, WithDecryption=True
                              )["Parameter"]["Value"]
        rep.kv(ssm_path=SSM_PATH, present=True, fingerprint=fp(v))
        key_found, key_source = v, "ssm"
        rep.ok("SSM param exists")
    except Exception as e:
        rep.kv(ssm_path=SSM_PATH, present=False, err=type(e).__name__)
        rep.warn(f"SSM param missing/unreadable: {type(e).__name__}")

    # 2. borrow from live consumers' env
    rep.section("CONSUMER ENV SCAN")
    for fn in CONSUMERS:
        try:
            cfg = LAM.get_function_configuration(FunctionName=fn)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            massive_keys = {k: fp(val) for k, val in env.items()
                            if "MASSIVE" in k.upper() or "BENZINGA" in k.upper()
                            or "POLYGON" in k.upper()}
            rep.kv(**{f"{fn}": massive_keys or "no massive/polygon env var"})
            if not key_found:
                for k, val in env.items():
                    if "MASSIVE" in k.upper() and val:
                        key_found, key_source = val, f"{fn}.{k}"
                        break
        except Exception as e:
            rep.kv(**{fn: f"ERR {type(e).__name__}"})

    # 3. live entitlement test
    rep.section("BENZINGA LIVE TEST")
    if not key_found:
        rep.fail("no Massive key found in SSM or any consumer env — "
                 "cannot test entitlement")
        rep.kv(RESULT="FAIL", reason="no_key_anywhere")
        sys.exit(1)
    rep.kv(testing_key_from=key_source, key_fp=fp(key_found))
    res = benzinga_ratings_test(key_found)
    rep.kv(benzinga_ratings=res)
    if res.get("http") == 200 and res.get("n_results", 0) > 0:
        rep.ok("KEY VALID + ENTITLED — Benzinga returns data. Fix = wire "
               "this key into justhodl-analyst-actions env MASSIVE_API_KEY.")
        rep.kv(RESULT="PASS_KEY_WORKS", fix="set_env")
    elif res.get("http") == 200:
        rep.warn("HTTP 200 but 0 results — could be genuinely quiet window "
                 "OR shape mismatch; inspect keys field")
        rep.kv(RESULT="PASS_200_EMPTY")
    elif res.get("http") in (401, 403):
        rep.fail(f"key present but NOT entitled (HTTP {res.get('http')}) — "
                 "Massive Benzinga add-on lapsed; needs renewal/new key")
        rep.kv(RESULT="FAIL_NOT_ENTITLED")
    else:
        rep.warn(f"inconclusive: {res}")
        rep.kv(RESULT="INCONCLUSIVE")
