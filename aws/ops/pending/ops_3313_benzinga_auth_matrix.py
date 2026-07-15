"""ops 3313 — Benzinga add-on IS paid + active (Khalid confirmed), yet
api.polygon.io/benzinga/v1/ratings?apiKey= returns 403 NOT_AUTHORIZED
(ops 3312). So the fault is in HOW we call it, not billing. This op tests
every plausible (host, path, auth) combination with the SSM key and, where
possible, asks the API what this key is actually entitled to — so we learn
the correct call shape without guessing.

Matrix tested against the SSM key /justhodl/massive-api-key:
  AUTH:  (a) ?apiKey=  (b) Authorization: Bearer <key>
  HOSTS/PATHS (ratings, limit=1):
    - https://api.polygon.io/benzinga/v1/ratings
    - https://api.polygon.io/v1/reference/benzinga/ratings
    - https://api.massive.com/benzinga/v1/ratings
    - https://api.massive.com/v1/benzinga/ratings
  ENTITLEMENTS / IDENTITY probes (what is this key allowed to see?):
    - https://api.polygon.io/v1/marketstatus/now       (base polygon alive?)
    - https://api.polygon.io/v3/reference/tickers?limit=1 (base entitlement)
    - https://api.massive.com/v1/account (if it exists)

For each: HTTP status + a short slice of the body (secrets scrubbed by the
reader). A 200 anywhere = the correct call shape -> we patch aws/shared/
benzinga.py to use it. Base-polygon 200 but benzinga 403 everywhere =
the key genuinely lacks the Benzinga scope (=> wrong key; the Benzinga
entitlement lives under a different key/account than this SSM value).

Read-only. No secrets in output.
"""
import json
import sys
import urllib.request
import urllib.error
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
SSM = boto3.client("ssm", region_name=REGION)
KEY = SSM.get_parameter(Name="/justhodl/massive-api-key",
                        WithDecryption=True)["Parameter"]["Value"]


def call(url, mode):
    """mode: 'q' = ?apiKey=, 'h' = Bearer header"""
    headers = {"User-Agent": "jh-ops-3313"}
    if mode == "q":
        sep = "&" if "?" in url else "?"
        full = f"{url}{sep}apiKey={KEY}"
    else:
        full = url
        headers["Authorization"] = f"Bearer {KEY}"
    req = urllib.request.Request(full, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode("utf-8", "ignore")
            try:
                body = json.loads(raw)
                shape = {"keys": list(body.keys())[:6]}
                for rk in ("status", "message", "count", "results", "ratings"):
                    if rk in body:
                        v = body[rk]
                        shape[rk] = (len(v) if isinstance(v, list) else v)
                return {"http": r.status, "shape": shape}
            except Exception:
                return {"http": r.status, "raw": raw[:120]}
    except urllib.error.HTTPError as e:
        detail = ""
        try:
            detail = e.read().decode("utf-8", "ignore")[:180]
        except Exception:
            pass
        return {"http": e.code, "err": detail}
    except Exception as e:
        return {"http": None, "err": f"{type(e).__name__}: {e}"}


BENZINGA_ENDPOINTS = [
    "https://api.polygon.io/benzinga/v1/ratings?limit=1&order=desc",
    "https://api.polygon.io/v1/reference/benzinga/ratings?limit=1",
    "https://api.massive.com/benzinga/v1/ratings?limit=1&order=desc",
    "https://api.massive.com/v1/benzinga/ratings?limit=1",
]
IDENTITY_ENDPOINTS = [
    "https://api.polygon.io/v1/marketstatus/now",
    "https://api.polygon.io/v3/reference/tickers?limit=1",
    "https://api.massive.com/v1/account",
]

with report("3313_benzinga_auth_matrix") as rep:
    winners = []

    rep.section("BENZINGA ENDPOINT MATRIX")
    for url in BENZINGA_ENDPOINTS:
        short = url.replace("https://", "").split("?")[0]
        for mode in ("q", "h"):
            res = call(url, mode)
            label = f"{short} [{'query' if mode=='q' else 'bearer'}]"
            rep.kv(**{label: res})
            if res.get("http") == 200 and "err" not in res:
                winners.append((url, mode, res))

    rep.section("IDENTITY / BASE ENTITLEMENT")
    base_ok = False
    for url in IDENTITY_ENDPOINTS:
        short = url.replace("https://", "").split("?")[0]
        res = call(url, "q")
        rep.kv(**{f"{short} [query]": res})
        if res.get("http") == 200:
            base_ok = True

    rep.section("VERDICT")
    if winners:
        for url, mode, res in winners:
            rep.ok(f"WORKS: {url} via {'query' if mode=='q' else 'bearer'} "
                   f"-> {res.get('shape')}")
        rep.kv(RESULT="FOUND_CORRECT_SHAPE", n_winners=len(winners),
               fix="patch aws/shared/benzinga.py _BASE/_get to this shape")
    elif base_ok:
        rep.fail("Base polygon/massive endpoints authorize (200) but EVERY "
                 "Benzinga path returns 403 -> this key lacks the Benzinga "
                 "scope. The paid Benzinga entitlement is under a DIFFERENT "
                 "key/account than /justhodl/massive-api-key. Need the key "
                 "tied to the Benzinga-entitled Massive account.")
        rep.kv(RESULT="WRONG_KEY_FOR_BENZINGA")
        sys.exit(1)
    else:
        rep.fail("Neither Benzinga nor base endpoints authorized — key or "
                 "host wrong across the board; inspect rows above.")
        rep.kv(RESULT="ALL_FAILED")
        sys.exit(1)
