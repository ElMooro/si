"""ops 3317 — Path 2 groundwork: verify the FMP /stable analyst endpoints
that will feed the re-sourced justhodl-analyst-actions actually return
LIVE data with Khalid's FMP key, and capture exact field names so the
harvest rewrite maps them correctly (no guessing).

Borrows FMP_KEY from the deployed justhodl-analyst-consensus env (proven
stable consumer). Tests the endpoints analyst-actions needs to reproduce
its output (rating transitions, PT moves, guidance-ish):
  - grades-news            (recent grade CHANGES = upgrade/downgrade feed)
  - price-target-news      (recent PT revisions with old/new)
  - grades-consensus       (roll-up per symbol; sanity)
  - price-target-consensus (target distribution; sanity)

For each: HTTP + record count + the field keys of the first record (so we
map old_grade/new_grade/price_target/etc correctly). Read-only.
"""
import json
import sys
import urllib.request
import urllib.error
from pathlib import Path

import boto3

from ops_report import report

REGION = "us-east-1"
LAM = boto3.client("lambda", region_name=REGION)


def fmp_key():
    for fn in ("justhodl-analyst-consensus", "justhodl-sellside-views",
               "justhodl-estimate-revisions"):
        try:
            cfg = LAM.get_function_configuration(FunctionName=fn)
            env = (cfg.get("Environment") or {}).get("Variables") or {}
            for k in ("FMP_KEY", "FMP_API_KEY"):
                if env.get(k):
                    return env[k], f"{fn}.{k}"
        except Exception:
            continue
    return None, None


def fmp_get(path, params, key):
    p = {**params, "apikey": key}
    qs = "&".join(f"{a}={b}" for a, b in p.items())
    url = f"https://financialmodelingprep.com/stable/{path}?{qs}"
    req = urllib.request.Request(url, headers={"User-Agent": "jh-ops-3317"})
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            body = json.loads(r.read())
            if isinstance(body, list):
                return {"http": r.status, "n": len(body),
                        "fields": list(body[0].keys()) if body else [],
                        "sample": body[0] if body else None}
            return {"http": r.status, "type": "dict",
                    "keys": list(body.keys())[:10], "sample": body}
    except urllib.error.HTTPError as e:
        d = ""
        try:
            d = e.read().decode("utf-8", "ignore")[:160]
        except Exception:
            pass
        return {"http": e.code, "err": d}
    except Exception as e:
        return {"http": None, "err": f"{type(e).__name__}: {e}"}


with report("3317_fmp_analyst_endpoints") as rep:
    key, src = fmp_key()
    rep.section("KEY")
    if not key:
        rep.fail("no FMP_KEY found on any known stable consumer env")
        rep.kv(RESULT="NO_FMP_KEY")
        sys.exit(1)
    rep.kv(key_source=src, key_fp={"len": len(key), "suffix": key[-4:]})

    rep.section("GRADES-NEWS (upgrade/downgrade feed)")
    gn = fmp_get("grades-news", {"page": 0, "limit": 100}, key)
    rep.kv(grades_news=gn)

    rep.section("PRICE-TARGET-NEWS (PT revisions)")
    ptn = fmp_get("price-target-news", {"page": 0, "limit": 100}, key)
    rep.kv(price_target_news=ptn)

    rep.section("GRADES-CONSENSUS (roll-up sanity, AAPL)")
    gc = fmp_get("grades-consensus", {"symbol": "AAPL"}, key)
    rep.kv(grades_consensus_AAPL=gc)

    rep.section("PRICE-TARGET-CONSENSUS (sanity, AAPL)")
    ptc = fmp_get("price-target-consensus", {"symbol": "AAPL"}, key)
    rep.kv(price_target_consensus_AAPL=ptc)

    rep.section("VERDICT")
    live = [("grades-news", gn), ("price-target-news", ptn)]
    ok = [n for n, r in live if r.get("http") == 200 and r.get("n", 0) > 0]
    if len(ok) == 2:
        rep.ok("Both core feeds return live data — sufficient to reproduce "
               "analyst-actions (rating transitions + PT moves). Guidance "
               "will be marked N/A or sourced from grades-news outlook text.")
        rep.kv(RESULT="FMP_READY", live_feeds=ok)
    elif ok:
        rep.warn(f"Only {ok} live; the other returned empty/err — check row.")
        rep.kv(RESULT="PARTIAL", live_feeds=ok)
    else:
        rep.fail("Core FMP analyst feeds not returning data — inspect rows.")
        rep.kv(RESULT="FMP_FEEDS_EMPTY")
        sys.exit(1)
