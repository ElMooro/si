"""justhodl-worldbank-full v1.1.0 -- the COMPLETE World Bank warehouse.

Drain-queue #2 (depth-audit ops 4957). The existing worldbank card =
26 curated keys; the source is ~16,000 indicators x 260 economies x
60 years. This walker mirrors EVERY indicator's official CSV-zip
verbatim (data + metadata sheets, the Bank's own provenance format):

  discover : paginated /v2/indicator catalog (JSON) -> id universe,
             banked to catalog.json.gz
  mirror   : GET api.worldbank.org/v2/en/indicator/{id}
             ?downloadformat=csv  (302 -> zip), spooled (memory up to
             SPOOL_MAX, then /tmp) -- never r.read() of the whole body
  no-data  : indicators answering HTML instead of a zip are recorded
             have[id].status="no_data" -- named, never silent
  chain    : MIDAS self-chain through chain_guard (bounded lineage,
             parked tickets resumed by justhodl-chain-resumer),
             per-link ~690s / byte budget, attempts counter ->
             quarantine at 4 dead tries
  refresh  : weekly Scheduler re-drain (event {"redrain": true})
             re-pulls every id; an indicator whose CSV content is
             unchanged is NOT rewritten (v1.1.0)
State data/warm/worldbank-full/_state/state.json; manifest feeds the
provider card (wb-note-v2).

v1.1.0 (2026-10-27, the S3 bytes-growth alarm: +82.5 GB in one day):
v1.0.0 wrote the WHOLE state document (indented JSON, the full `have`
map, ~2 MB) twice per indicator - "save-first black box" before the
download and again after - so a drain of ~25k indicators put ~75 GB a
day of state versions into the versioned bucket (ops 6698: 70.8 GB on
Oct 26, 75.6 GB in the first 19 h of Oct 27, 99% of the bucket's
growth), and the sealed archive replicates and keeps every version.
Now:
  * the per-indicator crash record is a ~100-byte marker
    (_state/inflight.json); a run that dies mid-indicator is still
    counted against that indicator on the next start (the quarantine
    at 4 dead tries is unchanged);
  * the state is checkpointed at most every CHECKPOINT_S seconds and at
    the end of a run, compact and gzip'd (readers accept plain JSON
    too, so the first v1.1.0 run reads the v1.0.0 state);
  * write-if-changed: each zip is hashed over its members' CSV content
    (zip timestamps ignored); an unchanged indicator is not re-uploaded
    - a weekly re-drain writes only what the Bank changed.
"""
import gzip
import hashlib
import json
import os
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3
import chain_guard  # ops 5260 — bounded-lineage self-chaining (aws/shared)

ENGINE_VERSION = "justhodl-worldbank-full v1.1.0 ops6700 checkpointed state, write-if-changed"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
API = "https://api.worldbank.org/v2"
ROOT = "data/warm/worldbank-full/"
STATE_KEY = ROOT + "_state/state.json"
INFLIGHT_KEY = ROOT + "_state/inflight.json"
CATALOG_KEY = ROOT + "catalog.json.gz"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "*/*"}
BUDGET_S = int(os.environ.get("WB_BUDGET_S", "690"))
CHECKPOINT_S = int(os.environ.get("WB_CHECKPOINT_S", "120"))
BYTES_BUDGET = 2_000_000_000
SPOOL_MAX = 64 * 1024 * 1024
SPACING = 0.2
CHAIN_DEPTH_MAX = 140

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()
_bytes_run = 0
_last_ckpt = 0.0
STATS = {"state_writes": 0, "uploaded": 0, "unchanged": 0}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    """JSON object at key - plain or gzip'd (v1.0.0 wrote plain, v1.1.0 writes gzip)."""
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if raw[:2] == b"\x1f\x8b":
            raw = gzip.decompress(raw)
        return json.loads(raw)
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def save_state(state):
    """The whole state, compact + gzip (one write)."""
    global _last_ckpt
    s3.put_object(Bucket=BUCKET, Key=STATE_KEY,
                  Body=gzip.compress(json.dumps(state, separators=(",", ":")).encode(), 6),
                  ContentType="application/json")
    _last_ckpt = time.time()
    STATS["state_writes"] += 1


def checkpoint(state):
    """Save the state only when CHECKPOINT_S has passed since the last save."""
    if time.time() - _last_ckpt >= CHECKPOINT_S:
        save_state(state)


def mark_inflight(iid, attempt):
    """The per-indicator crash record: ~100 bytes, not the whole state."""
    s3.put_object(Bucket=BUCKET, Key=INFLIGHT_KEY,
                  Body=json.dumps({"iid": iid, "attempt": attempt, "at": _now()}).encode(),
                  ContentType="application/json")


def recover_inflight(state):
    """A run that died mid-indicator left its marker: count that attempt (the
    last checkpoint may predate it), so a poison indicator still reaches the
    4-try quarantine."""
    mark = _j(INFLIGHT_KEY, None) or {}
    iid, att = mark.get("iid"), int(mark.get("attempt") or 0)
    if not iid:
        return None
    for item in state.get("queue") or []:
        if item[0] == iid and att > int(item[1]):
            item[1] = att
            return iid
    return None


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


def content_hash(fobj):
    """sha256 over the zip's members (name + CSV bytes, sorted by name): the
    Bank's data, not the zip container's timestamps.  16 hex chars."""
    h = hashlib.sha256()
    with zipfile.ZipFile(fobj) as z:
        for info in sorted(z.infolist(), key=lambda i: i.filename):
            h.update(info.filename.encode() + b"\0")
            with z.open(info) as m:
                for chunk in iter(lambda: m.read(1 << 20), b""):
                    h.update(chunk)
    return h.hexdigest()[:16]


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
            with tempfile.SpooledTemporaryFile(max_size=SPOOL_MAX) as spool:
                spool.write(head)
                for chunk in iter(lambda: r.read(1 << 20), b""):
                    spool.write(chunk)
                nb = spool.tell()
                spool.seek(0)
                sha = content_hash(spool)
                prev = state["have"].get(iid) or {}
                if prev.get("status") == "fresh" and prev.get("sha") == sha:
                    prev["checked"] = _now()
                    STATS["unchanged"] += 1
                    state["failures"].pop(iid, None)
                    return True
                spool.seek(0)
                s3.upload_fileobj(
                    spool, BUCKET, key,
                    ExtraArgs={"ContentType": "application/zip",
                               "Metadata": {"engine": "worldbank-full",
                                            "indicator": iid[:120],
                                            "content-sha": sha}})
        state["have"][iid] = {"status": "fresh", "bytes": nb, "sha": sha, "checked": _now()}
        _bytes_run += nb
        STATS["uploaded"] += 1
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
        "version": "1.1.0",
        "indicators_catalog": state.get("n_indicators"),
        "banked": len(banked),
        "no_data": sum(1 for v in have.values()
                       if v.get("status") == "no_data"),
        "bytes": tot, "gb": round(tot / 1e9, 2),
        "queue_left": len(state.get("queue") or []),
        "failures": len(state.get("failures") or {}),
        "phase": state.get("phase"),
        "last_run": dict(STATS),
        "note": ("complete api.worldbank.org indicator warehouse -- "
                 "every indicator's official CSV-zip (data+metadata)"
                 " verbatim; weekly full re-drain, unchanged indicators "
                 "are not rewritten")})


def lambda_handler(event, ctx=None):
    chain_guard.begin(event)  # ops 5260: bounded lineage, parks at hop 12 (AWS drops at 16)
    global _bytes_run, _t0, _last_ckpt
    _t0, _bytes_run, _last_ckpt = time.time(), 0, 0.0
    STATS.update(state_writes=0, uploaded=0, unchanged=0)
    event = event or {}
    state = _j(STATE_KEY, None) or {
        "version": "1.1.0", "phase": "DISCOVER", "queue": [],
        "have": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 150
    state["version"] = "1.1.0"
    recovered = recover_inflight(state)
    save_state(state)                                   # the lease, once

    if event.get("redrain") and state.get("phase") == "COMPLETE":
        ids = sorted(state.get("have") or {})
        state["queue"] = [[i, 0] for i in ids]
        state["phase"] = "DRAIN"
        save_state(state)

    if state["phase"] == "DISCOVER" or event.get("rediscover"):
        try:
            n = discover(state)
            if n:
                state["phase"] = "DRAIN"
            else:
                state["failures"]["_discover"] = "0 indicators"
        except Exception as e:
            state["failures"]["_discover"] = str(e)[:120]
        save_state(state)

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
                checkpoint(state)
                continue
            state["queue"][0][1] = att + 1
            mark_inflight(iid, att + 1)                 # the crash record, ~100 bytes
            if mirror_one(state, iid):
                state["queue"].pop(0)
            checkpoint(state)
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
    save_state(state)
    mark_inflight(None, 0)                              # clean exit: nothing in flight
    write_manifest(state)
    if chain:
        try:
            chain_guard.chain_invoke({"chain_depth": depth + 1}, context=ctx)
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "banked": len(banked),
            "gb": round(state["bytes_total"] / 1e9, 2),
            "queue_left": n_missing, "chained": chain,
            "recovered_inflight": recovered, "run": dict(STATS),
            "elapsed_s": round(time.time() - _t0, 1)}
