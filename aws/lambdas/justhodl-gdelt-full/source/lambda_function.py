"""justhodl-gdelt-full v1.0.0 -- COMPLETE GDELT v2 EVENTS warehouse.

Khalid priority lane #3 (4962 evidence: exact corpus ~38.6GB across
~132k export files; GKG/mentions stay SCOPED -- ~10x the bytes for
tone/graph annotation the fleet doesn't consume; stated, not silent).

Design kills the 127MB masterfilelist: v2 filenames are DETERMINISTIC
15-minute slots (YYYYMMDDHHMMSS.export.CSV.zip since 2015-02-18
23:00 UTC), so a date CURSOR replaces any queue --

  drain    iterate slots cursor->now-45min; GET -> streamed
           upload_fileobj (never r.read()); 404 = counted GAP
           (known-missing slots, sample kept), transient errors
           retry x3 inline then named
  state    tiny: {cursor, files, bytes, gaps, failures} -- no 132k
           enumeration, save-first each file
  chain    MIDAS self-chain, ~640s / 2.2GB per link, depth<=220
           (full ~38.6GB in ~15-18h autonomous)
  steady   files are immutable once posted -> refresh = cursor
           advance; Scheduler rate(30 minutes) keeps the live edge
  v1       1979-2015 daily archive = phase 2 after live-edge reached:
           index parsed once to _state/v1-queue.json.gz, own cursor
Keys data/warm/gdelt-full/v2/export/YYYY/MM/<slot>.export.CSV.zip;
manifest feeds the card (gd-note-v2).
"""
import gzip
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

ENGINE_VERSION = "justhodl-gdelt-full v1.0.1 ops4973 v1-index"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
V2 = "http://data.gdeltproject.org/gdeltv2/"
V1IDX = "http://data.gdeltproject.org/events/index.html"
V1 = "http://data.gdeltproject.org/events/"
ROOT = "data/warm/gdelt-full/"
STATE_KEY = ROOT + "_state/state.json"
V1Q_KEY = ROOT + "_state/v1-queue.json.gz"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)"}
EPOCH = datetime(2015, 2, 18, 23, 0, tzinfo=timezone.utc)
BUDGET_S = int(os.environ.get("GD_BUDGET_S", "640"))
BYTES_BUDGET = 2_200_000_000
SPACING = 0.05
CHAIN_DEPTH_MAX = 220
LIVE_LAG_MIN = 45

s3 = boto3.client("s3", region_name="us-east-1")
_t0 = time.time()
_bytes_run = 0


def _now():
    return datetime.now(timezone.utc)


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


def slot_str(dt):
    return dt.strftime("%Y%m%d%H%M%S")


def slot_dt(s_):
    return datetime.strptime(s_, "%Y%m%d%H%M%S").replace(
        tzinfo=timezone.utc)


def mirror_v2(state, dt):
    """One 15-min slot. Returns 'ok'|'gap'|'retry'."""
    global _bytes_run
    ss = slot_str(dt)
    url = V2 + ss + ".export.CSV.zip"
    key = (ROOT + "v2/export/%s/%s/%s.export.CSV.zip"
           % (ss[:4], ss[4:6], ss))
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=90) as r:
            ln = int(r.headers.get("Content-Length") or 0)
            s3.upload_fileobj(
                r, BUCKET, key,
                ExtraArgs={"ContentType": "application/zip",
                           "Metadata": {"engine": "gdelt-full",
                                        "slot": ss}})
        state["files"] = state.get("files", 0) + 1
        state["bytes"] = state.get("bytes", 0) + ln
        _bytes_run += ln
        return "ok"
    except urllib.error.HTTPError as e:
        if e.code == 404:
            state["gaps"] = state.get("gaps", 0) + 1
            gs = state.setdefault("gaps_sample", [])
            if len(gs) < 50:
                gs.append(ss)
            return "gap"
        fl = state.setdefault("failures", {})
        tries = (fl.get(ss) or {}).get("tries", 0) + 1
        fl[ss] = {"err": "HTTP %s" % e.code, "tries": tries}
        return "gap" if tries >= 3 else "retry"
    except Exception as e:
        fl = state.setdefault("failures", {})
        tries = (fl.get(ss) or {}).get("tries", 0) + 1
        fl[ss] = {"err": str(e)[:70], "tries": tries}
        return "gap" if tries >= 3 else "retry"


def load_v1_queue(state):
    try:
        raw = s3.get_object(Bucket=BUCKET,
                            Key=V1Q_KEY)["Body"].read()
        return json.loads(gzip.decompress(raw))
    except Exception:
        pass
    req = urllib.request.Request(V1IDX, headers=UA)
    with urllib.request.urlopen(req, timeout=120) as r:
        html = r.read(8_000_000).decode("utf-8", "replace")
    # v1.0.1 ops4973: live index hrefs didn't match the strict
    # quoted pattern (4962 counted 9,740 on this same page) --
    # capture bare filenames anywhere in the HTML instead
    names = sorted(set(re.findall(
        r'\b(\d{4}(?:\d{2})?(?:\d{2})?'
        r'(?:\.export\.CSV\.zip|\.zip))', html)))
    q = {"names": names, "idx": 0, "as_of": _now().isoformat(
        timespec="seconds")}
    s3.put_object(Bucket=BUCKET, Key=V1Q_KEY,
                  Body=gzip.compress(json.dumps(q).encode()),
                  ContentType="application/json",
                  ContentEncoding="gzip")
    state["v1_total"] = len(names)
    return q


def mirror_v1(state, name):
    global _bytes_run
    url = V1 + name
    key = ROOT + "v1/" + name
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=180) as r:
            ln = int(r.headers.get("Content-Length") or 0)
            s3.upload_fileobj(
                r, BUCKET, key,
                ExtraArgs={"ContentType": "application/zip",
                           "Metadata": {"engine": "gdelt-full",
                                        "v1": name[:40]}})
        state["v1_files"] = state.get("v1_files", 0) + 1
        state["v1_bytes"] = state.get("v1_bytes", 0) + ln
        _bytes_run += ln
        return True
    except urllib.error.HTTPError as e:
        state.setdefault("v1_failures", {})[name] = "HTTP %s" % e.code
        return True
    except Exception as e:
        fl = state.setdefault("v1_failures", {})
        prev = fl.get(name) or ""
        n = int(re.search(r"t(\d+)$", prev).group(1)) + 1 \
            if re.search(r"t\d+$", prev) else 1
        fl[name] = "%s t%d" % (str(e)[:50], n)
        return n >= 3


def write_manifest(state):
    _put_json(MANIFEST_KEY, {
        "as_of": _now().isoformat(timespec="seconds"),
        "engine": "justhodl-gdelt-full", "version": "1.0.0",
        "v2_files": state.get("files", 0),
        "v2_gb": round(state.get("bytes", 0) / 1e9, 2),
        "cursor": state.get("cursor"),
        "gaps": state.get("gaps", 0),
        "live_edge": state.get("phase") in ("LIVE", "V1"),
        "v1_files": state.get("v1_files", 0),
        "v1_total": state.get("v1_total"),
        "v1_gb": round(state.get("v1_bytes", 0) / 1e9, 2),
        "phase": state.get("phase"),
        "failures": len(state.get("failures") or {}),
        "note": ("complete GDELT v2 EVENTS mirror since 2015-02-18 "
                 "(deterministic 15-min cursor; 404s = counted "
                 "gaps) + v1 daily archive 1979-2015 phase-2; "
                 "GKG/mentions scoped by design (~10x bytes, "
                 "unconsumed)")})


def lambda_handler(event, ctx=None):
    global _t0, _bytes_run
    _t0, _bytes_run = time.time(), 0
    event = event or {}
    state = _j(STATE_KEY, None) or {
        "version": "1.0.0", "phase": "DRAIN",
        "cursor": slot_str(EPOCH), "files": 0, "bytes": 0,
        "gaps": 0, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 150
    _put_json(STATE_KEY, state)

    live_edge = _now() - timedelta(minutes=LIVE_LAG_MIN)
    cur = slot_dt(state["cursor"])
    saved = 0
    while cur <= live_edge:
        if time.time() - _t0 > BUDGET_S - 40 or \
                _bytes_run > BYTES_BUDGET:
            break
        res = mirror_v2(state, cur)
        if res != "retry":
            cur += timedelta(minutes=15)
            state["cursor"] = slot_str(cur)
        saved += 1
        if saved % 40 == 0:
            _put_json(STATE_KEY, state)   # save-first cadence
        time.sleep(SPACING)
    _v1_done = bool(state.get("v1_total")) and \
        state.get("v1_idx", 0) >= state["v1_total"]
    state["phase"] = "DRAIN" if cur <= live_edge else (
        "V1" if _v1_done else "LIVE")

    # phase 2: v1 backfill once the live edge is held ---------------
    if state["phase"] == "LIVE" and not event.get("no_v1") and \
            state.get("v1_idx", 0) < state.get("v1_total", 1) + 0:
        try:
            q = load_v1_queue(state)
            state["v1_total"] = len(q["names"])
            i = state.get("v1_idx", 0)
            while i < len(q["names"]):
                if time.time() - _t0 > BUDGET_S - 60 or \
                        _bytes_run > BYTES_BUDGET:
                    break
                if mirror_v1(state, q["names"][i]):
                    i += 1
                    state["v1_idx"] = i
                if i % 20 == 0:
                    _put_json(STATE_KEY, state)
                time.sleep(SPACING)
            if i >= len(q["names"]):
                state["phase"] = "V1"     # fully complete
        except Exception as e:
            state.setdefault("failures", {})["_v1_index"] = \
                str(e)[:100]

    behind = state["phase"] == "DRAIN" or (
        state["phase"] == "LIVE" and
        state.get("v1_idx", 0) < (state.get("v1_total") or 10**9))
    depth = int(event.get("chain_depth") or 0)
    chain = bool(behind and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now().isoformat(timespec="seconds")
    _put_json(STATE_KEY, state)
    write_manifest(state)
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME",
                    "justhodl-gdelt-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "cursor": state["cursor"],
            "files": state.get("files"), 
            "gb": round(state.get("bytes", 0) / 1e9, 2),
            "gaps": state.get("gaps"),
            "v1": "%s/%s" % (state.get("v1_idx", 0),
                             state.get("v1_total")),
            "chained": chain,
            "elapsed_s": round(time.time() - _t0, 1)}
