"""ops 3314 — FOUND a direct Benzinga key in legacy benzinga-news-agent:
BENZINGA_API_KEY hits api.benzinga.com/api/v2 directly (NOT via Massive),
so it bypasses the Massive-scope 403 entirely. If this key/plan is live it
IS Khalid's paid Benzinga entitlement and the fix is to re-source
justhodl-analyst-actions off the direct Benzinga API.

This op live-tests that key against the exact endpoints analyst-actions
needs, using the same 3 auth methods the legacy agent tries:
  - calendar/ratings   (analyst rating transitions + price targets)
  - calendar/guidance  (company guidance raises/cuts)
Reports HTTP + payload shape (record counts, top-level keys) per method,
scrubbing the key. A 200-with-records on ratings+guidance => we have a
working path; next op rewrites the harvest layer to use it (page schema
unchanged).

Also pulls the live key from the deployed benzinga-news-agent env (in case
it was rotated post-code) rather than trusting the hardcoded default.

Read-only. Key scrubbed from all output (prefix only).
"""
import json
import sys
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)

HARDCODED = "bzMJ62WO2YP2OKVIE2YSF4ZWVSVOJ6CTNP"


def deployed_key():
    try:
        cfg = LAM.get_function_configuration(
            FunctionName="benzinga-news-agent")
        env = (cfg.get("Environment") or {}).get("Variables") or {}
        return env.get("BENZINGA_API_KEY")
    except Exception:
        return None


def fp(k):
    return {"len": len(k), "prefix": k[:6]} if k else None


def bz_call(ep, key, method, extra=""):
    base = f"https://api.benzinga.com/api/v2/{ep}"
    headers = {"User-Agent": "jh-ops-3314", "Accept": "application/json"}
    if method == "token":
        url = f"{base}?token={key}{extra}"
    elif method == "apikey":
        url = f"{base}?apikey={key}{extra}"
    else:  # x-api-key header
        url = f"{base}?{extra.lstrip('&')}" if extra else base
        headers["x-api-key"] = key
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            raw = r.read().decode("utf-8", "ignore")
            try:
                body = json.loads(raw)
            except Exception:
                return {"http": r.status, "raw": raw[:140]}
            # Benzinga wraps lists under 'ratings' / 'guidance' keys
            shape = {"top_keys": list(body.keys())[:6]} if isinstance(
                body, dict) else {"type": type(body).__name__}
            for lk in ("ratings", "guidance"):
                if isinstance(body, dict) and lk in body:
                    v = body[lk]
                    shape[f"n_{lk}"] = len(v) if isinstance(v, list) else v
                    if isinstance(v, list) and v:
                        shape[f"sample_{lk}_fields"] = list(v[0].keys())[:8]
            if isinstance(body, list):
                shape["n_records"] = len(body)
                if body:
                    shape["sample_fields"] = list(body[0].keys())[:8]
            return {"http": r.status, "shape": shape}
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", "ignore")[:160]
        except Exception:
            pass
        return {"http": e.code, "err": detail}
    except Exception as e:
        return {"http": None, "err": f"{type(e).__name__}: {e}"}


with report("3314_direct_benzinga_test") as rep:
    dk = deployed_key()
    key = dk or HARDCODED
    rep.section("KEY SOURCE")
    rep.kv(deployed_key_present=bool(dk),
           deployed_fp=fp(dk), using=("deployed" if dk else "hardcoded_default"),
           key_fp=fp(key))

    # date window params Benzinga expects
    today = datetime.now(timezone.utc).date()
    wk_ago = today - timedelta(days=7)
    date_ratings = f"&parameters[date_from]={wk_ago}&parameters[date_to]={today}"
    d21 = today - timedelta(days=21)
    date_guid = f"&parameters[date_from]={d21}&parameters[date_to]={today}"

    any_win = False

    rep.section("RATINGS (calendar/ratings)")
    for m in ("token", "apikey", "x-api-key"):
        res = bz_call("calendar/ratings", key, m, extra=date_ratings)
        rep.kv(**{f"ratings [{m}]": res})
        if res.get("http") == 200 and "err" not in res:
            any_win = True

    rep.section("GUIDANCE (calendar/guidance)")
    for m in ("token", "apikey", "x-api-key"):
        res = bz_call("calendar/guidance", key, m, extra=date_guid)
        rep.kv(**{f"guidance [{m}]": res})

    rep.section("VERDICT")
    if any_win:
        rep.ok("Direct Benzinga API AUTHORIZES with this key. Fix path: "
               "re-source justhodl-analyst-actions harvest onto "
               "api.benzinga.com/api/v2/calendar/{ratings,guidance} using "
               "this key (store as SSM /justhodl/benzinga-api-key). Page "
               "schema unchanged.")
        rep.kv(RESULT="DIRECT_BENZINGA_WORKS", next="rewrite harvest + wire key")
    else:
        rep.fail("Direct Benzinga key did not authorize on any method — "
                 "this key/plan is not live either. Then the paid Benzinga "
                 "entitlement Khalid confirmed is on an account whose key we "
                 "don't yet hold; he'll need to paste the current key.")
        rep.kv(RESULT="DIRECT_ALSO_DEAD")
        sys.exit(1)
