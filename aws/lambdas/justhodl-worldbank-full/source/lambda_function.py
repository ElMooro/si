"""justhodl-worldbank-full v1.0.0 -- the COMPLETE World Bank warehouse.

Drain-queue #2 (depth-audit ops 4957). The existing worldbank card =
26 curated keys; the source is ~16,000 indicators x 260 economies x
60 years. This walker mirrors EVERY indicator's official CSV-zip
verbatim (data + metadata sheets, the Bank's own provenance format):

  discover : paginated /v2/indicator catalog (JSON) -> id universe,
             banked to catalog.json.gz
  mirror   : GET api.worldbank.org/v2/en/indicator/{id}
             ?downloadformat=csv  (302 -> zip), streamed
             upload_fileobj -- never r.read() (OOM doctrine)
  no-data  : indicators answering HTML instead of a zip are recorded
             have[id].status="no_data" -- named, never silent
  chain    : MIDAS self-chain, per-link ~690s / byte budget, save-
             first attempts counter -> quarantine at 4 dead tries
  refresh  : weekly Scheduler re-drain (event {"redrain": true})
             re-pulls every id -- the endpoint is dynamic (no
             Last-Modified), overwrite is idempotent and cheap at
             weekly cadence
State data/warm/worldbank-full/_state/state.json; manifest feeds the
provider card (wb-note-v2).
"""
import gzip
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-worldbank-full v1.0.0 ops4960 warehouse"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
API = "https://api.worldbank.org/v2"
ROOT = "data/warm/worldbank-full/"
STATE_KEY = ROOT + "_state/state.json"
CATALOG_KEY = ROOT + "catalog.json.gz"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "*/*"}
BUDGET_S = int(os.environ.get("WB_BUDGET_S", "690"))
BYTES_BUDGET = 2_000_000_000
SPACING = 0.2
CHAIN_DEPTH_MAX = 140

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()
_bytes_run = 0


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


def _get_json(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read(30_000_000).decode(
            "utf-8", "replace"))


def discover(state):
    """Full indicator catalog via paginated JSON."""
    page, total_pages, cat = 1, 1, []
    while page <= total_pages and page <= 40:
        js = _get_json(API + "/indicator?format=json&per_page=1000"
                       "&page=%d" % page)
        meta, rows = js[0], js[1] or []
        total_pages = int(meta.get("pages") or 1)
        for r in rows:
            iid = (r.get("id") or "").strip()
            if iid:
                cat.append({"id": iid, "name": (r.get("name")
                                                or "")[:140]})
        page += 1
        time.sleep(SPACING)
    ids = sorted({c["id"] for c in cat})
    have = state.setdefault("have", {})
    q = state.setdefault("queue", [])
    queued = {x[0] for x in q}
    for iid in ids:
        if iid not in have and iid not in queued:
            q.append([iid, 0])
    state["n_indicators"] = len(ids)
    s3.put_object(Bucket=BUCKET, Key=CATALOG_KEY,
                  Body=gzip.compress(json.dumps(
                      {"as_of": _now(), "n": len(cat),
                       "indicators": cat}).encode()),
                  ContentType="application/json",
                  ContentEncoding="gzip")
    return len(ids)


def mirror_one(state, iid):
    global _bytes_run
    url = (API + "/en/indicator/%s?downloadformat=csv"
           % urllib.parse.quote(iid))
    key = ROOT + "src/%s.zip" % iid
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=180) as r:
            ct = (r.headers.get("Content-Type") or "").lower()
            head = r.read(280)
            if b"<html" in head.lower() or b"<!doc" in head.lower() \
                    or "text/html" in ct:
                state["have"][iid] = {"status": "no_data", "bytes": 0}
                return True

            class _Chained:
                def __init__(s2, first, rest):
                    s2.first, s2.rest = first, rest
                def read(s2, amt=None):
                    if s2.first:
                        b, s2.first = s2.first, b""
                        return b
                    return s2.rest.read(amt)
            body = _Chained(head, r)
            s3.upload_fileobj(
                body, BUCKET, key,
                ExtraArgs={"ContentType": "application/zip",
                           "Metadata": {"engine": "worldbank-full",
                                        "indicator": iid[:120]}})
        nb = s3.head_object(Bucket=BUCKET, Key=key)["ContentLength"]
        state["have"][iid] = {"status": "fresh", "bytes": nb}
        _bytes_run += nb
        state["failures"].pop(iid, None)
        return True
    except urllib.error.HTTPError as e:
        fl = state["failures"]
        tries = (fl.get(iid) or {}).get("tries", 0) + 1
        fl[iid] = {"err": "HTTP %s" % e.code, "tries": tries}
        return tries >= 3
    except Exception as e:
        fl = state["failures"]
        tries = (fl.get(iid) or {}).get("tries", 0) + 1
        fl[iid] = {"err": str(e)[:80], "tries": tries}
        return tries >= 3


def write_manifest(state):
    have = state.get("have") or {}
    banked = [v for v in have.values() if v.get("status") == "fresh"]
    tot = sum(v.get("bytes") or 0 for v in banked)
    _put_json(MANIFEST_KEY, {
        "as_of": _now(), "engine": "justhodl-worldbank-full",
        "version": "1.0.0",
        "indicators_catalog": state.get("n_indicators"),
        "banked": len(banked),
        "no_data": sum(1 for v in have.values()
                       if v.get("status") == "no_data"),
        "bytes": tot, "gb": round(tot / 1e9, 2),
        "queue_left": len(state.get("queue") or []),
        "failures": len(state.get("failures") or {}),
        "phase": state.get("phase"),
        "note": ("complete api.worldbank.org indicator warehouse -- "
                 "every indicator's official CSV-zip (data+metadata)"
                 " verbatim; weekly full re-drain")})


def lambda_handler(event, ctx=None):
    global _bytes_run, _t0
    _t0, _bytes_run = time.time(), 0
    event = event or {}
    state = _j(STATE_KEY, None) or {
        "version": "1.0.0", "phase": "DISCOVER", "queue": [],
        "have": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 150
    _put_json(STATE_KEY, state)

    if event.get("redrain") and state.get("phase") == "COMPLETE":
        ids = sorted(state.get("have") or {})
        state["queue"] = [[i, 0] for i in ids]
        state["phase"] = "DRAIN"
        _put_json(STATE_KEY, state)

    if state["phase"] == "DISCOVER" or event.get("rediscover"):
        try:
            n = discover(state)
            if n:
                state["phase"] = "DRAIN"
            else:
                state["failures"]["_discover"] = "0 indicators"
        except Exception as e:
            state["failures"]["_discover"] = str(e)[:120]
        _put_json(STATE_KEY, state)

    if state["phase"] == "DRAIN" and state["queue"]:
        while state["queue"]:
            if time.time() - _t0 > BUDGET_S - 35 or \
                    _bytes_run > BYTES_BUDGET:
                break
            iid, att = state["queue"][0]
            if att >= 4:
                state["failures"][iid] = {
                    "err": "quarantined after %d dead attempts" % att,
                    "tries": att}
                state["queue"].pop(0)
                _put_json(STATE_KEY, state)
                continue
            state["queue"][0][1] = att + 1
            _put_json(STATE_KEY, state)      # save-first black box
            if mirror_one(state, iid):
                state["queue"].pop(0)
            _put_json(STATE_KEY, state)
            time.sleep(SPACING)
        if not state["queue"]:
            state["phase"] = "COMPLETE"

    n_missing = len(state["queue"])
    depth = int(event.get("chain_depth") or 0)
    chain = bool(n_missing and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    banked = [v for v in state["have"].values()
              if v.get("status") == "fresh"]
    state["n_banked"] = len(banked)
    state["bytes_total"] = sum(v.get("bytes") or 0 for v in banked)
    _put_json(STATE_KEY, state)
    write_manifest(state)
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-worldbank-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "banked": len(banked),
            "gb": round(state["bytes_total"] / 1e9, 2),
            "queue_left": n_missing, "chained": chain,
            "elapsed_s": round(time.time() - _t0, 1)}

