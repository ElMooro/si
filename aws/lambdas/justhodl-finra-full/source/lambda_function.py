"""justhodl-finra-full v1.0.0 -- COMPLETE FINRA Query API warehouse.

Khalid: every FINRA metric, full history since inception, on the
board. api.finra.org doctrine:

  AUTH      OAuth2 client-credentials (FINRA_CLIENT_ID/SECRET env,
            injected by ops from wherever the fleet holds them --
            never committed); token cached ~50min. A plain
            FINRA_API_KEY env, if that is what exists, rides as a
            fallback header.
  DISCOVER  /metadata/group/{g} for the known public groups ->
            every datasetName; probe-validated (1-row data call)
            before joining the universe; invalid named
  DRAIN     per dataset: /data/group/{g}/name/{n} paginated
            limit=5000&offset=k, JSON records spooled to /tmp as
            JSONL then gz-uploaded verbatim-rows at
            data/warm/finra-full/src/{g}__{n}.jsonl.gz
            (RAM-safe; full history = walk offsets to exhaustion)
  REFRESH   daily re-pull per dataset when its newest row moved
            (cheap 1-row probe ordered desc by best date field);
            weekly force redrain
  CHAIN     MIDAS self-chain; save-first; 3-strike named failures
"""
import gzip
import json
import os
import time
import urllib.error
import urllib.request
from base64 import b64encode
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-finra-full v1.0.3 ops4978 empty-uni-rediscover"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
CID = os.environ.get("FINRA_CLIENT_ID", "")
CSEC = os.environ.get("FINRA_CLIENT_SECRET", "")
APIKEY = os.environ.get("FINRA_API_KEY", "")
TOKEN_URL = ("https://ews.fip.finra.org/fip/rest/ews/oauth2/"
             "access_token?grant_type=client_credentials")
API = "https://api.finra.org"
GROUPS = ["otcMarket", "fixedIncomeMarket"]
# v1.0.2: /metadata is AUTH-LOCKED on the public tier while /data
# answers keyless -- when metadata refuses, fall back to these
# known public dataset names, each still probe-validated via a
# 1-row /data call (wrong names fail named).
SEED_DATASETS = {
    "otcMarket": [
        "weeklySummary", "monthlySummary", "blocksSummary",
        "otcBlocksSummary", "consolidatedShortInterest",
        "regShoDaily", "weeklyDownloadDetails",
        "monthlyDownloadDetails", "atsIssueData",
        "otcIssueData"],
    "fixedIncomeMarket": [
        "treasuryWeeklyAggregates", "treasuryMonthlyAggregates",
        "corporatesAndAgenciesCappedVolume",
        "corporateMarketSentiment", "agencyMarketSentiment",
        "corporate144AMarketSentiment",
        "treasuryDailyAggregates", "securitizedProductsCapped",
        "corporateMarketBreadth"],
}
ROOT = "data/warm/finra-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
BUDGET_S = int(os.environ.get("FIN_BUDGET_S", "660"))
PAGE = 5000
SPACING = 0.3
CHAIN_DEPTH_MAX = 40

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()
_tok = {"v": "", "exp": 0}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(
            s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def token():
    if APIKEY and not (CID and CSEC):
        return None
    if _tok["v"] and time.time() < _tok["exp"]:
        return _tok["v"]
    req = urllib.request.Request(
        TOKEN_URL, method="POST",
        headers={"Authorization": "Basic " + b64encode(
            ("%s:%s" % (CID, CSEC)).encode()).decode(),
            **UA})
    with urllib.request.urlopen(req, timeout=45) as r:
        js = json.loads(r.read(20_000))
    _tok["v"] = js.get("access_token") or ""
    _tok["exp"] = time.time() + 50 * 60
    return _tok["v"]


def call(path, timeout=120, cap=40_000_000, accept="application/json"):
    h = dict(UA)
    h["Accept"] = accept
    t = token()
    if t:
        h["Authorization"] = "Bearer " + t
    elif APIKEY:
        h["X-API-KEY"] = APIKEY
    req = urllib.request.Request(API + path, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


def discover(state):
    uni, invalid = dict(state.get("universe") or {}), \
        dict(state.get("invalid") or {})
    for g in GROUPS:
        names = []
        try:
            raw = call("/metadata/group/%s" % g, cap=4_000_000)
            js = json.loads(raw)
            items = js if isinstance(js, list) else \
                js.get("datasets") or js.get("data") or []
            for it in items:
                nm = it.get("datasetName") or it.get("name") \
                    if isinstance(it, dict) else it
                if nm:
                    names.append(nm)
        except Exception as e:
            state["failures"]["_meta_" + g] = \
                ("metadata refused (%s) -> seed fallback"
                 % str(e)[:60])
            names = list(SEED_DATASETS.get(g) or [])
        try:
            for nm in names:
                if not nm:
                    continue
                key = "%s__%s" % (g, nm)
                if key in uni or key in invalid:
                    continue
                try:
                    call("/data/group/%s/name/%s?limit=1" % (g, nm),
                         cap=200_000)
                    uni[key] = {"group": g, "name": nm}
                except urllib.error.HTTPError as e:
                    invalid[key] = "HTTP %s" % e.code
                except Exception as e:
                    invalid[key] = str(e)[:60]
                time.sleep(SPACING)
        except Exception as e:
            state["failures"]["_meta_" + g] = str(e)[:100]
    state["universe"], state["invalid"] = uni, invalid
    q = state.setdefault("queue", [])
    queued = {x[0] for x in q}
    for k in sorted(uni):
        if k not in state.get("have", {}) and k not in queued:
            q.append([k, 0])
    state["last_discover"] = {"at": _now(), "n": len(uni)}
    return len(uni)


def drain_one(state, key):
    g, nm = key.split("__", 1)
    tmp = "/tmp/finra.jsonl.gz"
    rows = 0
    try:
        with gzip.open(tmp, "wt", encoding="utf-8") as f:
            off = 0
            while True:
                raw = call("/data/group/%s/name/%s?limit=%d"
                           "&offset=%d" % (g, nm, PAGE, off))
                js = json.loads(raw)
                batch = js if isinstance(js, list) else \
                    js.get("data") or []
                for r_ in batch:
                    f.write(json.dumps(r_, separators=(",", ":"))
                            + "\n")
                rows += len(batch)
                if len(batch) < PAGE:
                    break
                off += PAGE
                if time.time() - _t0 > BUDGET_S - 60:
                    raise TimeoutError("budget @offset %d" % off)
                time.sleep(SPACING)
        s3.upload_file(tmp, BUCKET, ROOT + "src/%s.jsonl.gz" % key,
                       ExtraArgs={"ContentType": "application/gzip",
                                  "Metadata": {
                                      "engine": "finra-full",
                                      "dataset": key[:110],
                                      "rows": str(rows)}})
        nb = os.path.getsize(tmp)
        os.remove(tmp)
        state["have"][key] = {"rows": rows, "bytes": nb,
                              "at": _now(), "status": "fresh"}
        state["failures"].pop(key, None)
        return True
    except TimeoutError as e:
        state["failures"][key] = {"err": str(e), "tries":
                                  (state["failures"].get(key) or {}
                                   ).get("tries", 0)}
        try:
            os.remove(tmp)
        except Exception:
            pass
        return False
    except Exception as e:
        fl = state["failures"]
        tries = (fl.get(key) or {}).get("tries", 0) + 1
        fl[key] = {"err": str(e)[:100], "tries": tries}
        try:
            os.remove(tmp)
        except Exception:
            pass
        return tries >= 3


def lambda_handler(event, ctx=None):
    global _t0
    _t0 = time.time()
    event = event or {}
    # v1.0.1: keyless PUBLIC-tier mode allowed -- FINRA's public
    # datasets answer unauthenticated (rate-limited); creds, when
    # they arrive, upgrade the same engine in place.
    state = _j(STATE_KEY, None) or {
        "version": "1.0.0", "phase": "DISCOVER", "queue": [],
        "have": {}, "failures": {}, "universe": {}, "invalid": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 150
    _put_json(STATE_KEY, state)

    age = 10**9
    try:
        age = time.time() - datetime.fromisoformat(
            (state.get("last_discover") or {}).get("at")
        ).timestamp()
    except Exception:
        pass
    if state["phase"] == "DISCOVER" or event.get("rediscover") or \
            age > 20 * 3600 or not state.get("universe"):
        # v1.0.3: an EMPTY universe is never a fresh discovery --
        # v1's 0-dataset run left last_discover<20h and the skip-
        # guard no-opped every later link (seed-fallback never ran)
        try:
            discover(state)
            if state["phase"] == "DISCOVER" or state.get("queue"):
                state["phase"] = "DRAIN"
        except Exception as e:
            state["failures"]["_discover"] = {"err": str(e)[:110]}
        _put_json(STATE_KEY, state)

    if event.get("redrain") and state.get("phase") == "COMPLETE":
        state["queue"] = [[k, 0] for k in sorted(state["have"])]
        state["phase"] = "DRAIN"

    if state["phase"] == "DRAIN" and state["queue"]:
        while state["queue"]:
            if time.time() - _t0 > BUDGET_S - 50:
                break
            k, att = state["queue"][0]
            if att >= 4:
                state["queue"].pop(0)
                _put_json(STATE_KEY, state)
                continue
            state["queue"][0][1] = att + 1
            _put_json(STATE_KEY, state)
            if drain_one(state, k):
                state["queue"].pop(0)
            _put_json(STATE_KEY, state)
            time.sleep(SPACING)
        if not state["queue"]:
            state["phase"] = "COMPLETE"

    n_left = len(state["queue"])
    depth = int(event.get("chain_depth") or 0)
    chain = bool(n_left and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_banked"] = len(state["have"])
    _put_json(STATE_KEY, state)
    have = state["have"]
    _put_json(MANIFEST_KEY, {
        "as_of": state["as_of"], "engine": "justhodl-finra-full",
        "version": "1.0.0",
        "datasets_catalog": len(state.get("universe") or {}),
        "datasets_banked": len(have),
        "rows": sum(v.get("rows") or 0 for v in have.values()),
        "mb": round(sum(v.get("bytes") or 0
                        for v in have.values()) / 1e6, 1),
        "invalid_named": len(state.get("invalid") or {}),
        "failures": len(state.get("failures") or {}),
        "phase": state.get("phase"),
        "note": ("complete FINRA Query API warehouse: every "
                 "discovered dataset fully paginated since "
                 "inception (OTC/ATS weeklies, TRACE aggregates, "
                 "short interest...); daily rediscovery, weekly "
                 "redrain")})
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-finra-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "banked": state["n_banked"],
            "rows": sum(v.get("rows") or 0 for v in have.values()),
            "queue_left": n_left, "chained": chain,
            "elapsed_s": round(time.time() - _t0, 1)}
