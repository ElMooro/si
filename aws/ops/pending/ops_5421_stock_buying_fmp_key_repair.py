"""ops_5421 -- stock-buying: why do /stable/ calls return nothing? diagnose, repair the key, re-verify.

ops 5420 (2026-09-11 20:13 UTC): deployed v1.5.2 verified, engine ran and republished 614 rows, 66 rows took
the FMP path, 0 carry revisions_beats. fmp() swallows every exception, so a rejected key looks exactly like
"no data". The engine resolves managed_secret(("FMP_API_KEY","FMP_KEY"), SSM) -- env first -- and this function
predates the ops 5226/5227 rotation work (it never referenced managed_secret, so 5227 did not backfill it),
so a stale FMP_API_KEY in its env would win over the current SSM key.

Steps (no secret value is ever printed; keys are compared by sha256 prefix and length only):
  1. list the function's env var NAMES; hash-compare FMP_API_KEY / FMP_KEY against SSM /justhodl/fmp/api-key
  2. probe FMP from the runner with each distinct key: /stable/earnings?symbol=AAPL, /stable/income-statement,
     and the legacy /api/v3/earnings-surprises/AAPL -- report HTTP status + shape (list length, first-row keys)
  3. if the env key is stale/rejected and the SSM key is accepted: set FMP_API_KEY and FMP_KEY to the SSM value
     (merge; nothing else changes), wait for the update to settle
  4. invoke once (Event), wait for data/stock-buying.json to regenerate, assert rows now carry revisions_beats
"""
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-stock-buying"
BUCKET = "justhodl-dashboard-live"
KEY = "data/stock-buying.json"
PARAM = "/justhodl/fmp/api-key"
FAILS = []


def fp(v):
    v = str(v or "")
    return "sha256:%s len=%d" % (hashlib.sha256(v.encode()).hexdigest()[:10], len(v)) if v else "(empty)"


def probe(url_no_key, key):
    try:
        req = urllib.request.Request(url_no_key + ("&" if "?" in url_no_key else "?") + "apikey=" + key,
                                     headers={"User-Agent": "justhodl-fleet"})
        with urllib.request.urlopen(req, timeout=20) as h:
            body = h.read()
            st = h.status
    except urllib.error.HTTPError as e:
        return e.code, (e.read()[:160].decode("utf-8", "replace").replace(key, "***") if e.fp else ""), None
    except Exception as e:
        return None, str(e)[:120].replace(key, "***"), None
    try:
        d = json.loads(body)
    except Exception:
        return st, "non-JSON %dB" % len(body), None
    if isinstance(d, list):
        return st, "list[%d] keys=%s" % (len(d), sorted((d[0] or {}).keys())[:8] if d and isinstance(d[0], dict) else []), d
    if isinstance(d, dict):
        return st, "dict keys=%s" % sorted(d.keys())[:8], d
    return st, type(d).__name__, d


def read_feed(s3):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
    except Exception:
        return None


def parse_ts(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None


with report("ops_5421_stock_buying_fmp_key_repair") as R:
    R.heading("ops 5421 -- stock-buying FMP key diagnose / repair / re-verify")
    lam = boto3.client("lambda", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    ssm = boto3.client("ssm", region_name=REGION)

    R.section("1. keys: function env vs SSM (hash-compared)")
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = dict(((cfg.get("Environment") or {}).get("Variables") or {}))
    R.log("env var names: %s" % ", ".join(sorted(env.keys())) or "(none)")
    ssm_key = ssm.get_parameter(Name=PARAM, WithDecryption=True)["Parameter"]["Value"]
    R.log("SSM %s: %s" % (PARAM, fp(ssm_key)))
    env_api, env_key = env.get("FMP_API_KEY", ""), env.get("FMP_KEY", "")
    R.log("env FMP_API_KEY: %s  %s" % (fp(env_api), "== SSM" if env_api and env_api == ssm_key else "(differs from SSM)" if env_api else ""))
    R.log("env FMP_KEY:     %s  %s" % (fp(env_key), "== SSM" if env_key and env_key == ssm_key else "(differs from SSM)" if env_key else ""))
    effective = env_api or env_key or ssm_key      # what managed_secret(("FMP_API_KEY","FMP_KEY"), SSM) resolves
    R.log("key the engine effectively uses: %s (%s)" % (fp(effective), "env FMP_API_KEY" if env_api else "env FMP_KEY" if env_key else "SSM"))

    R.section("2. probes from the runner (HTTP status + shape; no values)")
    keys = {}
    for label, k in (("env-effective", effective), ("ssm", ssm_key)):
        if k and k not in keys.values():
            keys[label] = k
    urls = {
        "stable/earnings": "https://financialmodelingprep.com/stable/earnings?symbol=AAPL&limit=3",
        "stable/income-statement": "https://financialmodelingprep.com/stable/income-statement?symbol=AAPL&period=quarter&limit=3",
        "legacy v3 earnings-surprises": "https://financialmodelingprep.com/api/v3/earnings-surprises/AAPL",
    }
    ok_with = {}
    for label, k in keys.items():
        for name, u in urls.items():
            st, shape, d = probe(u, k)
            R.log("  [%s] %-30s -> HTTP %s  %s" % (label, name, st, shape))
            if name == "stable/earnings" and st == 200 and isinstance(d, list) and d and isinstance(d[0], dict):
                ok_with[label] = ("epsActual" in d[0] and "epsEstimated" in d[0])
                if not ok_with[label]:
                    R.warn("  /stable/earnings rows lack epsActual/epsEstimated -- keys were %s" % sorted(d[0].keys()))
            time.sleep(0.3)
    R.kv(step="probe", env_effective_ok=ok_with.get("env-effective"), ssm_ok=ok_with.get("ssm"),
         env_equals_ssm=bool(effective and effective == ssm_key))

    R.section("3. repair")
    repaired = False
    if ok_with.get("env-effective"):
        R.log("the key the engine uses is accepted by /stable/earnings -- the empty pillars are not a key problem; see section 4 and the ops log for the next hypothesis")
    elif ok_with.get("ssm"):
        new_env = dict(env)
        new_env["FMP_API_KEY"] = ssm_key
        new_env["FMP_KEY"] = ssm_key
        for attempt in range(6):
            try:
                lam.update_function_configuration(FunctionName=FN, Environment={"Variables": new_env})
                break
            except lam.exceptions.ResourceConflictException:
                time.sleep(5 * (attempt + 1))
        else:
            FAILS.append("env update kept conflicting")
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(4)
        repaired = True
        R.ok("env FMP_API_KEY + FMP_KEY set to the SSM value (was %s) -- update settled" % fp(effective))
    else:
        FAILS.append("neither the engine's key nor the SSM key is accepted by /stable/earnings -- the provider key itself needs Khalid (FMP dashboard -> Parameter Store %s)" % PARAM)

    R.section("4. re-run and re-check the feed")
    if not FAILS:
        before = read_feed(s3) or {}
        t0 = datetime.now(timezone.utc)
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b'{"source":"ops_5421_gate"}')
        R.log("invoked (Event) at %s; baseline generated_at=%s" % (t0.isoformat(timespec="seconds"), before.get("generated_at")))
        after, waited = None, 0
        while waited < 20 * 60:
            time.sleep(20)
            waited += 20
            cur = read_feed(s3) or {}
            g = parse_ts(cur.get("generated_at") or "")
            if g and g >= t0:
                after = cur
                break
        if after is None:
            FAILS.append("feed did not regenerate within 20 min")
        else:
            top = after.get("top") or []
            gated = sum(1 for r in top if (r.get("gates") or {}).get("below_sma"))
            with_beats = sum(1 for r in top if (r.get("pillars") or {}).get("revisions_beats") is not None)
            with_accel = sum(1 for r in top if (r.get("pillars") or {}).get("accel") is not None)
            R.log("regenerated after %ds: n_scored=%s fmp_key=%s gated=%d with_beats=%d with_accel=%d" % (
                waited, after.get("n_scored"), after.get("fmp_key"), gated, with_beats, with_accel))
            R.kv(step="rerun", repaired=repaired, waited_s=waited, gated=gated, with_beats=with_beats, with_accel=with_accel)
            for r in [x for x in top if (x.get("pillars") or {}).get("revisions_beats") is not None][:5]:
                R.log("  %-6s beats=%s accel=%s tier=%s" % (r.get("symbol"), r["pillars"].get("revisions_beats"), r["pillars"].get("accel"), r.get("tier")))
            if gated and not with_beats:
                FAILS.append("still 0 rows with revisions_beats after %s -- key is accepted, so the fault is in the engine's request/parse path; next: read the warm cache bodies data/warm/blackswan/sb_earn_*.json" % ("the env repair" if repaired else "an unchanged env"))

    R.section("verdict")
    for f in FAILS:
        R.fail(f)
    if FAILS:
        sys.exit(1)
    R.ok("GREEN -- stock-buying resolves an accepted FMP key and its /stable/ earnings pillars are populated in %s" % KEY)
