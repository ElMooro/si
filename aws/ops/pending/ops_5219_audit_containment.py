"""ops_5219 -- audit 2026-09-08 Release A containment (runs on the GitHub runner with AWS creds).

1. Managed configuration: make sure the canonical SSM SecureStrings exist for every provider
   credential the fleet had as source literals (values sourced from the LIVE Lambda envs, never from
   the repo): /justhodl/{fmp,polygon,fred,cmc,newsapi,census}/api-key, /justhodl/telegram/bot_token.
2. Wait for justhodl-data-proxy v2.1.0 (deploy-workers on this push), then with the SERVICE role:
     - bind the owner (Supabase account for OWNER_EMAILS) into KV owner:uids (fails LOUD if that
       account does not exist and there is more than one account -- no guessing);
     - migrate legacy anonymous userdata blobs (u:<uid> where Supabase says uid is not an account)
       into the anon:<uid> namespace (dry-run first, then real).
3. Re-dispatch deploy-workers for justhodl-data-proxy so ADMIN_TOKEN / POLYGON_KEY / FRED_KEY secrets
   attach even if their SSM parameters were created by step 1 after the first deploy fetched them.
Never prints secret values. sys.exit(1) on any hard failure.
"""
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"
OWNER_EMAILS = ["raafouis@gmail.com"]
PARAMS = {   # ssm name -> candidate env var names across the fleet (first match wins by frequency)
    "/justhodl/fmp/api-key": ["FMP_KEY", "FMP_API_KEY"],
    "/justhodl/polygon/api-key": ["POLYGON_API_KEY", "POLYGON_KEY", "POLY_KEY"],
    "/justhodl/fred/api-key": ["FRED_API_KEY", "FRED_KEY"],
    "/justhodl/cmc/api-key": ["CMC_KEY", "COINMARKETCAP_API_KEY"],
    "/justhodl/telegram/bot_token": ["TELEGRAM_BOT_TOKEN", "TELEGRAM_TOKEN"],
    "/justhodl/newsapi/api-key": ["NEWSAPI_KEY", "NEWS_API_KEY"],
    "/justhodl/census/api-key": ["CENSUS_API_KEY"],
}
FAILS = []


def http(method, url, headers=None, body=None, timeout=40):
    req = urllib.request.Request(url, method=method, headers=headers or {}, data=body)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:
        return 0, str(e).encode()


def mask(v):
    v = str(v or "")
    return (v[:2] + "…" + v[-2:]) if len(v) > 8 else "…"


def mask_email(e):
    e = str(e or "")
    if "@" not in e:
        return mask(e)
    a, b = e.split("@", 1)
    return a[:2] + "…@" + b


with report("ops_5219_audit_containment") as R:
    R.heading("ops 5219 -- audit 2026-09-08 Release A: containment (SSM managed config, owner binding, guest-namespace migration)")
    ssm = boto3.client("ssm", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)

    # ── 1. managed configuration from LIVE envs ─────────────────────────────
    R.section("1. canonical SSM parameters")
    votes = {name: {} for name in PARAMS}
    n_fn = 0
    for page in lam.get_paginator("list_functions").paginate():
        for fc in page.get("Functions", []):
            n_fn += 1
            env = ((fc.get("Environment") or {}).get("Variables") or {})
            for name, keys in PARAMS.items():
                for k in keys:
                    v = env.get(k)
                    if v and len(v) >= 16:
                        votes[name][v] = votes[name].get(v, 0) + 1
    R.log("scanned %d functions' environments" % n_fn)
    for name, tally in votes.items():
        existing = None
        try:
            existing = ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]
        except ssm.exceptions.ParameterNotFound:
            existing = None
        except Exception as e:
            R.warn("%s read failed: %s" % (name, str(e)[:80]))
        if not tally:
            R.log("%s: no live env value in the fleet (%s); %s" % (name, "exists" if existing else "absent", "left as is" if existing else "nothing to seed"))
            continue
        best, n = max(tally.items(), key=lambda kv: kv[1])
        if len(tally) > 1:
            R.warn("%s: %d distinct live values across the fleet (majority %d functions) -- rotation must reconcile these" % (name, len(tally), n))
        if existing == best:
            R.ok("%s exists and matches the fleet majority value (%s, %d functions)" % (name, mask(best), n))
        elif existing:
            R.warn("%s exists but DIFFERS from the fleet majority value (ssm %s vs env %s) -- not overwritten; reconcile during rotation" % (name, mask(existing), mask(best)))
        else:
            ssm.put_parameter(Name=name, Value=best, Type="SecureString", Overwrite=False,
                              Description="audit 2026-09-08 INST-06 managed credential (seeded from live Lambda env by ops 5219)")
            R.ok("%s CREATED from the live env majority value (%s, %d functions)" % (name, mask(best), n))

    # ── 2. worker service-role operations ───────────────────────────────────
    R.section("2. worker v2.1.0 + owner binding + guest migration")
    try:
        admin = ssm.get_parameter(Name="/justhodl/api-admin/token", WithDecryption=True)["Parameter"]["Value"]
    except Exception as e:
        admin = None
        FAILS.append("/justhodl/api-admin/token unreadable: %s" % str(e)[:80])
    ver = None
    t0 = time.time()
    while time.time() - t0 < 900:
        st, body = http("GET", WORKER + "/health")
        try:
            ver = json.loads(body).get("version")
        except Exception:
            ver = None
        if st == 200 and ver == "2.1.0":
            break
        time.sleep(20)
    if ver != "2.1.0":
        FAILS.append("data-proxy is %s after %ds (expected 2.1.0)" % (ver, int(time.time() - t0)))
    else:
        R.ok("data-proxy 2.1.0 live after %ds" % int(time.time() - t0))
    if admin and ver == "2.1.0":
        H = {"X-JH-Service-Token": admin}
        st, body = http("GET", WORKER + "/admin/users", H)
        users = []
        if st == 200:
            users = json.loads(body).get("users") or []
            R.log("supabase accounts: %d -> %s" % (len(users), ", ".join(mask_email(u.get("email")) for u in users[:12])))
        else:
            FAILS.append("/admin/users -> HTTP %s %s" % (st, body[:120]))
        owner = [u for u in users if (u.get("email") or "").lower() in OWNER_EMAILS]
        if not owner and len(users) == 1:
            owner = users
            R.warn("OWNER_EMAILS not found but exactly ONE account exists (%s) -- binding it as owner" % mask_email(users[0].get("email")))
        if owner:
            for u in owner:
                st, body = http("POST", WORKER + "/admin/owner", dict(H, **{"Content-Type": "application/json"}), json.dumps({"uid": u["id"]}).encode())
                if st == 200:
                    R.ok("owner bound: uid %s (%s)" % (mask(u["id"]), mask_email(u.get("email"))))
                else:
                    FAILS.append("/admin/owner bind -> HTTP %s %s" % (st, body[:120]))
        elif users:
            FAILS.append("no Supabase account matches OWNER_EMAILS %s and there are %d accounts -- refusing to guess; add the right email to OWNER_EMAILS" % (OWNER_EMAILS, len(users)))
        else:
            R.warn("no Supabase accounts returned -- owner binding skipped (OWNER_EMAILS env still binds by email at sign-in)")
        # service-role read of the owner Brain store must work (the Lambda mirror depends on it)
        st, body = http("GET", WORKER + "/brain?uid=brain-930ffa48-60a1-4b11-8726-8848d1b827f9", H)
        try:
            d = json.loads(body)
        except Exception:
            d = {}
        if st == 200 and d.get("scope") == "service":
            R.ok("service-role Brain read OK: %s notes, store %s" % (len(d.get("notes") or []), d.get("store")))
        else:
            FAILS.append("service-role Brain read failed: HTTP %s %s" % (st, body[:120]))
        # guest namespace migration: dry then real
        for dry in ("1", "0"):
            cursor = None
            tot = {"scanned": 0, "accounts": 0, "migrated": 0, "already": 0, "errors": 0}
            for _ in range(200):
                url = WORKER + "/admin/userdata-migrate?dry=" + dry + (("&cursor=" + cursor) if cursor else "")
                st, body = http("POST", url, H)
                if st != 200:
                    FAILS.append("userdata-migrate dry=%s -> HTTP %s %s" % (dry, st, body[:120])); break
                d = json.loads(body)
                for k in tot:
                    tot[k] += d.get(k, 0)
                cursor = d.get("next_cursor")
                if not cursor:
                    break
            R.log("userdata-migrate dry=%s: %s" % (dry, tot))
            R.kv(step="userdata-migrate", dry=dry, **tot)

    # ── 3. re-dispatch deploy-workers for the data-proxy ────────────────────
    R.section("3. re-dispatch deploy-workers (secrets from the parameters above)")
    tok = os.environ.get("BUS_GITHUB_PAT") or os.environ.get("GH_API_TOKEN") or ""
    if tok:
        st, body = http("POST", "https://api.github.com/repos/ElMooro/si/actions/workflows/deploy-workers.yml/dispatches",
                        {"Authorization": "Bearer " + tok, "Accept": "application/vnd.github+json", "Content-Type": "application/json", "User-Agent": "ops5219"},
                        json.dumps({"ref": "main", "inputs": {"worker": "justhodl-data-proxy"}}).encode())
        if st == 204:
            R.ok("deploy-workers re-dispatched for justhodl-data-proxy")
        else:
            R.warn("workflow dispatch -> HTTP %s %s (secrets attach on the next worker push instead)" % (st, body[:100]))
    else:
        R.warn("no GitHub token in env -- skipped re-dispatch")

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        R.log("RED: %d failure(s)" % len(FAILS))
        sys.exit(1)
    R.ok("GREEN: containment complete")
