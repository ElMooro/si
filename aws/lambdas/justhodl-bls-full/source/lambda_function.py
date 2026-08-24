"""justhodl-bls-full v1.0.0 -- the COMPLETE BLS time.series warehouse.

Khalid (5th ask): import ALL historical data. The existing bls card =
25 curated hot keys (0.11MB). The real store is download.bls.gov/pub/
time.series/ -- every survey's flat files (cu CPI-U since 1913, ce
payrolls, jt JOLTS, la local-area, pc/wp PPI, pr productivity, ...):
series maps + FULL AllData history, multi-GB, no API caps.

Walker doctrine (census+MIDAS hybrid, all banked patterns):
  * discover survey dirs from the Apache-style listing; per survey
    list EVERY file (series/data.*/footnote/mapping -- docs matter)
  * mirror VERBATIM, streaming urlopen -> s3.upload_fileobj (never
    r.read(): the READ_CAP OOM lesson, files here reach GBs)
  * conditional refresh: HEAD Last-Modified+Length vs stored object
    metadata {src_lm, src_len}; unchanged -> skip (src-mirror lesson)
  * self-chain (MIDAS): per-link budget ~680s / ~2.3GB banked, then
    Event-invoke self, depth<=80; lease prevents overlap
  * save-first attempts counter -> crash quarantine (census lesson);
    failures NAMED never silent; single-file cap 4GB
  * state data/warm/bls-full/_state/state.json; manifest for the card
BLS requires a real User-Agent w/ contact (403 otherwise). Scheduler
rate(12h) re-walks listings + conditional refresh after COMPLETE.
"""
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-bls-full v1.0.1 ops4959 iis-case"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
BASE = "https://download.bls.gov/pub/time.series/"
ROOT = "data/warm/bls-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "JustHodl Research (raafouis@gmail.com)",
      "Accept": "*/*"}
BUDGET_S = int(os.environ.get("BLS_BUDGET_S", "690"))
BYTES_BUDGET = int(os.environ.get("BLS_BYTES_BUDGET",
                                  str(2_300_000_000)))
FILE_CAP = 4_200_000_000
SPACING = 0.25
CHAIN_DEPTH_MAX = 80

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


def _listing(url):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=60) as r:
        # listings are small HTML; bounded read is safe
        return r.read(4_000_000).decode("utf-8", "replace")


def _head(url):
    req = urllib.request.Request(url, headers=UA, method="HEAD")
    with urllib.request.urlopen(req, timeout=45) as r:
        return (r.headers.get("Last-Modified") or "",
                int(r.headers.get("Content-Length") or 0))


def discover_surveys(state):
    html = _listing(BASE)
    # BLS runs IIS: listings use uppercase <A HREF="..."> -- the
    # v1.0.0 case-sensitive regex parsed ZERO dirs silently (4958).
    dirs = sorted({d.lower() for d in re.findall(
        r'href="(?:/pub/time\.series/)?([A-Za-z]{2,3})/?"',
        html, re.I)})
    dirs = [d for d in dirs if d not in ("pub",)]
    if not dirs:
        # empty parse is a NAMED failure, never a silent no-op
        state["failures"]["_discover"] = (
            "0 dirs parsed; head=%r" %
            html[:200].replace("\n", " "))
        return []
    state["failures"].pop("_discover", None)
    state["surveys"] = state.get("surveys") or {}
    for d in dirs:
        state["surveys"].setdefault(d, {"n_files": 0, "bytes": 0,
                                        "listed": False})
    state["n_surveys"] = len(state["surveys"])
    return dirs


def list_survey(state, sv, recheck=False):
    html = _listing(BASE + sv + "/")
    files = sorted(set(re.findall(
        r'href="(?:/pub/time\.series/%s/)?(%s\.[A-Za-z0-9.\-_]+)"'
        % (re.escape(sv), re.escape(sv)), html, re.I)))
    if not files:
        state["failures"]["_list:" + sv] = (
            "0 files parsed; head=%r" %
            html[:160].replace("\n", " "))
    else:
        state["failures"].pop("_list:" + sv, None)
    q = state.setdefault("queue", [])
    have = state.setdefault("have", {})
    queued = {x[0] for x in q}
    added = 0
    for f in files:
        rel = sv + "/" + f
        if rel in queued:
            continue
        if rel in have and not recheck:
            continue
        q.append([rel, 0])
        queued.add(rel)
        added += 1
    meta = state["surveys"][sv]
    meta["listed"] = True
    meta["n_files"] = len(files)
    return added


def mirror_one(state, rel):
    """Stream one file; True = done (banked/skipped/failed-named)."""
    global _bytes_run
    url = BASE + rel
    key = ROOT + "src/" + rel
    try:
        lm, ln = _head(url)
    except urllib.error.HTTPError as e:
        state["failures"][rel] = "HEAD HTTP %s" % e.code
        return True
    except Exception as e:
        state["failures"][rel] = "HEAD %s" % str(e)[:80]
        return True
    if ln > FILE_CAP:
        state["failures"][rel] = "oversize %dB (cap)" % ln
        return True
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        md = h.get("Metadata") or {}
        if md.get("src_lm") == lm and \
                str(md.get("src_len")) == str(ln) and lm:
            state["have"][rel] = {"bytes": ln, "lm": lm,
                                  "status": "unchanged"}
            return True
    except Exception:
        pass
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=300) as r:
            s3.upload_fileobj(
                r, BUCKET, key,
                ExtraArgs={"ContentType": "text/plain",
                           "Metadata": {"engine": "bls-full",
                                        "src_lm": lm,
                                        "src_len": str(ln)}})
        prev = (state["have"].get(rel) or {}).get("bytes", 0)
        state["have"][rel] = {"bytes": ln, "lm": lm,
                              "status": "fresh"}
        sv = rel.split("/", 1)[0]
        state["surveys"][sv]["bytes"] = \
            state["surveys"][sv].get("bytes", 0) + ln - prev
        _bytes_run += ln
        state["failures"].pop(rel, None)
        return True
    except Exception as e:
        fl = state["failures"]
        prev = fl.get(rel) or ""
        tries = int(re.search(r"tries=(\d+)", prev).group(1)) + 1 \
            if "tries=" in prev else 1
        fl[rel] = "GET %s tries=%d" % (str(e)[:70], tries)
        return tries >= 3          # 3 strikes -> named failure, move


def write_manifest(state):
    have = state.get("have") or {}
    tot = sum(v.get("bytes") or 0 for v in have.values())
    _put_json(MANIFEST_KEY, {
        "as_of": _now(), "engine": "justhodl-bls-full",
        "version": "1.0.1",
        "surveys": state.get("n_surveys"),
        "files": len(have), "bytes": tot,
        "gb": round(tot / 1e9, 2),
        "queue_left": len(state.get("queue") or []),
        "failures": len(state.get("failures") or {}),
        "phase": state.get("phase"),
        "note": ("complete download.bls.gov/pub/time.series mirror "
                 "-- every survey, series maps + full AllData "
                 "history since 1913; conditional Last-Modified "
                 "refresh")})


def lambda_handler(event, ctx=None):
    global _bytes_run, _t0
    _t0, _bytes_run = time.time(), 0
    state = _j(STATE_KEY, None) or {
        "version": "1.0.1", "phase": "DISCOVER", "surveys": {},
        "queue": [], "have": {}, "failures": {}}
    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 150
    _put_json(STATE_KEY, state)

    if not state.get("surveys"):
        state["phase"] = "DISCOVER"      # self-heal empty state
    if state["phase"] == "DISCOVER" or event.get("rediscover"):
        if discover_surveys(state):
            state["phase"] = "LIST"
        _put_json(STATE_KEY, state)

    if state["phase"] in ("LIST", "DRAIN", "COMPLETE"):
        # ensure listings (also picks up NEW files after COMPLETE)
        for sv, meta in state["surveys"].items():
            if time.time() - _t0 > BUDGET_S - 90:
                break
            if meta.get("listed") and state["phase"] != "COMPLETE":
                continue
            if meta.get("listed") and not event.get("relist"):
                continue
            try:
                list_survey(state, sv,
                            recheck=(state["phase"] == "COMPLETE"
                                     or bool(event.get("relist"))))
                time.sleep(SPACING)
            except Exception as e:
                state["failures"]["_list:" + sv] = str(e)[:80]
        if all(m.get("listed") for m in
               state["surveys"].values()) and \
                state["phase"] == "LIST":
            state["phase"] = "DRAIN"
        _put_json(STATE_KEY, state)

    if state["phase"] in ("DRAIN", "COMPLETE") and state["queue"]:
        state["phase"] = "DRAIN"
        while state["queue"]:
            if time.time() - _t0 > BUDGET_S - 40 or \
                    _bytes_run > BYTES_BUDGET:
                break
            rel, att = state["queue"][0]
            if att >= 4:
                state["failures"][rel] = \
                    "quarantined after %d dead attempts" % att
                state["queue"].pop(0)
                _put_json(STATE_KEY, state)
                continue
            state["queue"][0][1] = att + 1
            _put_json(STATE_KEY, state)      # save-first black box
            if mirror_one(state, rel):
                state["queue"].pop(0)
            _put_json(STATE_KEY, state)
            time.sleep(SPACING)
        if not state["queue"]:
            state["phase"] = "COMPLETE"
    if state["phase"] == "DRAIN" and not state["queue"] and \
            state["surveys"] and \
            all(m.get("listed") for m in state["surveys"].values()):
        state["phase"] = "COMPLETE"

    n_missing = len(state["queue"])
    depth = int((event or {}).get("chain_depth") or 0)
    chain = bool(n_missing and depth < CHAIN_DEPTH_MAX
                 and not event.get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_files"] = len(state["have"])
    state["bytes_total"] = sum(v.get("bytes") or 0
                               for v in state["have"].values())
    _put_json(STATE_KEY, state)
    write_manifest(state)
    if chain:
        try:
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=os.environ.get(
                    "AWS_LAMBDA_FUNCTION_NAME", "justhodl-bls-full"),
                InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": depth + 1}).encode())
        except Exception:
            chain = False
    return {"ok": True, "phase": state["phase"],
            "files": len(state["have"]),
            "gb": round(state["bytes_total"] / 1e9, 2),
            "queue_left": n_missing, "chained": chain,
            "bytes_this_run": _bytes_run,
            "elapsed_s": round(time.time() - _t0, 1)}
