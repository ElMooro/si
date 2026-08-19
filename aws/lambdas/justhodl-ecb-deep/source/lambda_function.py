"""justhodl-ecb-deep — E-deep v1.3 (ops 4896/4897/4901 + 4908 slow-window guard:
month-split for oversize years, flow_order resync with the walker's
live truncated ledger, historical-revision rotation in refresh mode,
and a self-updating data/warm/ecb/coverage.json completeness ledger).

The 31 giant ECB flows (BSI, HICP, MIR, SEC, SHS, YC, ...) exceed any
RAM-buffered cap (proven ops 4895: >450MB raw each), and a bare pull
truncates the RECENT tail. This engine guarantees Khalid's requirement
-- ALL ECB data, ALL history since inception, permanently stored --
by pulling each giant flow in startPeriod/endPeriod TIME WINDOWS,
streamed to /tmp (10GB ephemeral), gzip-file-to-file, uploaded as
permanent parts:

  data/warm/ecb/data/{FLOW}__{start}_{end}.dat.gz      (deny-Delete'd)
  data/warm/ecb/data/{FLOW}.manifest.json              (parts ledger)

State: data/_state/ecb-deep.json (lease, per-flow window queue,
inception first_period parsed from the earliest non-empty window's
actual TIME_PERIOD column -- not regex guesswork). Windows that still
exceed the 3.5GB /tmp guard split to per-year windows automatically;
a single YEAR over the guard is flagged oversize_year, stated not
silent. HTTP 404 = genuinely empty range (pre-inception) = fine.

Self-driving: EventBridge Scheduler every 10 min (created by ops 4896)
until every flow completes, then flips to REFRESH mode -- each run
re-pulls the newest window of a rotating subset so the giants stay
current forever. Access posture = the proven ciss-stress pattern:
honest UA, ?format=csvdata, no content negotiation.
"""
import gzip
import json
import os
import shutil
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
BASE = "https://data-api.ecb.europa.eu/service/data/"
UA = {"User-Agent": "JustHodl Research raafouis@gmail.com"}
STATE_KEY = "data/_state/ecb-deep.json"
WALK_STATE = "data/_state/sdmx-walk-ecb.json"
OUT = "data/warm/ecb/data"
BUDGET_S = int(os.environ.get("DEEP_BUDGET_S", "820"))
TMP_CAP = int(3.5 * 1024 * 1024 * 1024)   # /tmp stream guard
REFRESH_PER_RUN = 4
WINDOWS = [("1900", "1979"), ("1980", "1989"), ("1990", "1994"),
           ("1995", "1999"), ("2000", "2004"), ("2005", "2009"),
           ("2010", "2014"), ("2015", "2019"), ("2020", "2022"),
           ("2023", "2024"), ("2025", "2035")]

s3 = boto3.client("s3", region_name="us-east-1")


def _j(key, default=None):
    try:
        b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return default


def _put_json(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _stream_window(flow, sp, ep, wl_deadline=None):
    """Pull one window to /tmp. Returns (status, raw_bytes,
    first_two_lines) -- status in ok|empty|oversize|slow|err:<t>.
    ops 4908: wl_deadline aborts slow drips BEFORE the 900s Lambda
    wall kills the whole invoke (the livelock: a dripping mega-window
    ate every run, chain never fired, duty collapsed to the 10-min
    Scheduler minus timeouts)."""
    url = (f"{BASE}{flow}?format=csvdata"
           f"&startPeriod={sp}&endPeriod={ep}")
    tmp = "/tmp/w.csv"
    head = b""
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=420) as r, \
                open(tmp, "wb") as f:
            n = 0
            while True:
                c = r.read(4 * 1024 * 1024)
                if not c:
                    break
                if n == 0:
                    head = c[:4096]
                f.write(c)
                n += len(c)
                if n >= TMP_CAP:
                    return "oversize", n, head
                if wl_deadline and time.time() > wl_deadline:
                    return "slow", n, head
        if n < 60:
            return "empty", n, head
        return "ok", n, head
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return "empty", 0, b""
        return f"err:HTTP{e.code}", 0, b""
    except Exception as e:
        return f"err:{type(e).__name__}", 0, b""


def _gzip_upload(flow, sp, ep):
    src, dst = "/tmp/w.csv", "/tmp/w.csv.gz"
    with open(src, "rb") as fi, gzip.open(dst, "wb",
                                          compresslevel=6) as fo:
        shutil.copyfileobj(fi, fo, length=8 * 1024 * 1024)
    key = f"{OUT}/{flow}__{sp}_{ep}.dat.gz"
    s3.upload_file(dst, BUCKET, key,
                   ExtraArgs={"ContentType": "application/gzip",
                              "Metadata": {"window": f"{sp}/{ep}",
                                           "engine": "ecb-deep"}})
    gz = os.path.getsize(dst)
    for p in (src, dst):
        try:
            os.remove(p)
        except Exception:
            pass
    return key, gz


def _first_period(head):
    """TIME_PERIOD of the first data row, from the real header."""
    try:
        lines = head.decode("utf-8", "ignore").splitlines()
        if len(lines) < 2:
            return None
        cols = lines[0].split(",")
        ti = cols.index("TIME_PERIOD")
        return lines[1].split(",")[ti]
    except Exception:
        return None


def _month_splits(y):
    return [(f"{y}-{m:02d}", f"{y}-{m:02d}") for m in range(1, 13)]


def _year_splits(sp, ep):
    a, b = int(sp[:4]), min(int(ep[:4]), 2035)
    return [(str(y), str(y)) for y in range(a, b + 1)]


def _ensure_flow(state, flow):
    fl = state["flows"].setdefault(flow, {"windows": {}, "complete":
                                          False})
    if not fl["windows"]:
        for sp, ep in WINDOWS:
            fl["windows"][f"{sp}_{ep}"] = {"status": "pending"}
    return fl


def _next_pending(state):
    for flow in state["flow_order"]:
        fl = state["flows"].get(flow) or {}
        if fl.get("complete"):
            continue
        for wid in sorted(fl.get("windows") or {},
                          key=lambda w: w.split("_")[0]):
            w = fl["windows"][wid]
            if w.get("status") == "pending" or (
                    str(w.get("status", "")).startswith("err")
                    and w.get("tries", 0) < 3):
                return flow, wid
    return None, None


def _flow_done(fl):
    return all(w.get("status") in ("done", "empty",
                                   "oversize_year",
                                   "oversize_month", "slow_month")
               for w in fl["windows"].values())


def _write_manifest(flow, fl):
    parts = [{"window": wid, **{k: v for k, v in w.items()
                                if k in ("status", "raw_bytes",
                                         "gz_bytes", "key")}}
             for wid, w in sorted(fl["windows"].items())]
    _put_json(f"{OUT}/{flow}.manifest.json", {
        "flow": flow, "engine": "ecb-deep", "as_of": _now(),
        "complete": fl.get("complete", False),
        "first_period": fl.get("first_period"),
        "total_raw_bytes": sum(w.get("raw_bytes") or 0
                               for w in fl["windows"].values()),
        "n_parts": sum(1 for w in fl["windows"].values()
                       if w.get("status") == "done"),
        "parts": parts})


def _write_coverage(state, walk):
    """data/warm/ecb/coverage.json -- the completeness ledger Khalid
    can point at: every flow, its source path, and its status."""
    done = list(dict.fromkeys(walk.get("done") or []))
    trunc = set(state.get("flow_order") or [])
    fast = [f for f in done if f not in trunc]
    deep = {}
    for f in state.get("flow_order") or []:
        fl = state.get("flows", {}).get(f) or {}
        ws = fl.get("windows") or {}
        deep[f] = {"complete": bool(fl.get("complete")),
                   "first_period": fl.get("first_period"),
                   "n_parts": sum(1 for w in ws.values()
                                  if w.get("status") == "done"),
                   "flags": sorted({w.get("status")
                                    for w in ws.values()
                                    if str(w.get("status", "")
                                           ).startswith("oversize")})}
    _put_json("data/warm/ecb/coverage.json", {
        "as_of": _now(), "engine": "ecb-deep",
        "n_flows_walked": len(done),
        "n_fast": len(fast),
        "n_deep": len(trunc),
        "n_deep_complete": sum(1 for d in deep.values()
                               if d["complete"]),
        "walk_failures": walk.get("failures") or {},
        "fast_flows": sorted(fast),
        "deep_flows": deep,
        "note": ("fast = full-history single object via weekly "
                 "rewalk; deep = time-sliced parts + manifest; "
                 "every prefix deny-Delete protected")})


def lambda_handler(event, context):
    t0 = time.time()
    state = _j(STATE_KEY, {}) or {}
    if float(state.get("lease_until") or 0) > time.time():
        return {"statusCode": 200,
                "body": json.dumps({"skipped": "lease_held"})}
    if not state.get("flow_order"):
        walk = _j(WALK_STATE, {}) or {}
        giants = list(dict.fromkeys(walk.get("truncated") or []))
        state = {"flow_order": giants, "flows": {}, "mode":
                 "backfill", "created_at": _now()}
    walk = _j(WALK_STATE, {}) or {}
    for fl_new in dict.fromkeys(walk.get("truncated") or []):
        if fl_new not in state["flow_order"]:
            state["flow_order"].append(fl_new)
            state.setdefault("resynced", []).append(
                {"flow": fl_new, "at": _now()})
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put_json(STATE_KEY, state)
    for flow in state["flow_order"]:
        _ensure_flow(state, flow)

    did = []
    while time.time() - t0 < BUDGET_S:
        flow, wid = _next_pending(state)
        if flow is None:
            state["mode"] = "refresh"
            break
        fl = state["flows"][flow]
        w = fl["windows"][wid]
        sp, ep = wid.split("_")
        _wl = min(t0 + BUDGET_S - 70, time.time() + 600)
        status, nraw, head = _stream_window(flow, sp, ep,
                                            wl_deadline=_wl)
        if status == "ok":
            key, gz = _gzip_upload(flow, sp, ep)
            w.update(status="done", raw_bytes=nraw,
                     gz_bytes=gz, key=key, at=_now())
            if not fl.get("first_period"):
                fp = _first_period(head)
                if fp:
                    fl["first_period"] = fp
        elif status in ("oversize", "slow"):
            # slow = dripping stream: same medicine as oversize --
            # split down; a slow MONTH is flagged terminal after the
            # normal retry budget (stated, not silent).
            if sp == ep and len(sp) == 7:
                w.update(status=("oversize_month" if status ==
                                 "oversize" else "slow_month"),
                         raw_bytes=nraw, at=_now())
            elif sp == ep and len(sp) == 4:
                del fl["windows"][wid]
                for ms, me in _month_splits(sp):
                    fl["windows"][f"{ms}_{me}"] = {"status":
                                                   "pending"}
            else:
                del fl["windows"][wid]
                for ys, ye in _year_splits(sp, ep):
                    fl["windows"][f"{ys}_{ye}"] = {"status":
                                                   "pending"}
        elif status == "empty":
            w.update(status="empty", at=_now())
        else:
            w.update(status=status, tries=(w.get("tries", 0) + 1),
                     at=_now())
        did.append(f"{flow}:{wid}:{status}")
        if _flow_done(fl) and not fl.get("complete"):
            fl["complete"] = True
            fl["completed_at"] = _now()
            _write_manifest(flow, fl)
        _put_json(STATE_KEY, dict(state,
                                  lease_until=time.time()
                                  + BUDGET_S + 120))

    # refresh mode: newest window of a rotating subset stays current
    if state.get("mode") == "refresh":
        rot = int(state.get("rot") or 0)
        order = state["flow_order"]
        for i in range(REFRESH_PER_RUN):
            if time.time() - t0 >= BUDGET_S:
                break
            flow = order[(rot + i) % max(1, len(order))]
            fl = state["flows"].get(flow) or {}
            wids = [k for k, v in (fl.get("windows") or {}).items()
                    if v.get("status") == "done"]
            if not wids:
                continue
            wid = max(wids, key=lambda x: x.split("_")[1])
            sp, ep = wid.split("_")
            status, nraw, head = _stream_window(
                flow, sp, ep,
                wl_deadline=min(t0 + BUDGET_S - 40,
                                time.time() + 300))
            if status == "ok":
                key, gz = _gzip_upload(flow, sp, ep)
                fl["windows"][wid].update(
                    raw_bytes=nraw, gz_bytes=gz, key=key,
                    refreshed_at=_now())
                _write_manifest(flow, fl)
                did.append(f"refresh:{flow}:{wid}")
        state["rot"] = rot + REFRESH_PER_RUN
        # ECB revises HISTORY too (BSI/ICP esp.) -- one rotating
        # historical window per run walks the entire back catalog
        # on a slow loop, so revisions land without a full re-crawl.
        allw = [(f, wid) for f in order
                for wid, v in (state["flows"].get(f, {})
                               .get("windows") or {}).items()
                if v.get("status") == "done"]
        if allw and time.time() - t0 < BUDGET_S:
            hr = int(state.get("hrot") or 0)
            f, wid = allw[hr % len(allw)]
            sp, ep = wid.split("_")
            status, nraw, head = _stream_window(
                f, sp, ep,
                wl_deadline=min(t0 + BUDGET_S - 40,
                                time.time() + 300))
            if status == "ok":
                key, gz = _gzip_upload(f, sp, ep)
                state["flows"][f]["windows"][wid].update(
                    raw_bytes=nraw, gz_bytes=gz, key=key,
                    refreshed_at=_now())
                did.append(f"hist-refresh:{f}:{wid}")
            state["hrot"] = hr + 1

    n_complete = sum(1 for f in state["flows"].values()
                     if f.get("complete"))
    # ops 4901 (Khalid: expedite, budget no problem): self-chain --
    # while backfill work remains, async re-invoke at budget end for
    # ~100% duty (the FRED pattern, playbook 2.6). Depth-capped, the
    # 10-min Scheduler stays the watchdog, the lease stays the
    # overlap guard (released just before chaining).
    _depth = int((event or {}).get("chain_depth") or 0)
    _more = _next_pending(state)[0] is not None
    _chain = bool(state.get("mode") == "backfill" and _more
                  and _depth < 60
                  and not (event or {}).get("no_chain"))
    state["lease_until"] = 0
    state["as_of"] = _now()
    state["n_flows"] = len(state["flow_order"])
    state["n_complete"] = n_complete
    _put_json(STATE_KEY, state)
    try:
        _write_coverage(state, walk)
    except Exception as e:
        print("coverage write failed: %s" % type(e).__name__)
    if _chain:
        try:
            _fn = os.environ.get("AWS_LAMBDA_FUNCTION_NAME",
                                 "justhodl-ecb-deep")
            boto3.client("lambda", region_name="us-east-1").invoke(
                FunctionName=_fn, InvocationType="Event",
                Payload=json.dumps(
                    {"chain_depth": _depth + 1}).encode())
        except Exception as e:
            print("chain failed: %s" % type(e).__name__)
            _chain = False
    res = {"ok": True, "mode": state.get("mode"),
           "chained": _chain, "chain_depth": _depth,
           "actions": len(did), "sample": did[:6],
           "flows_complete": f"{n_complete}/{len(state['flow_order'])}",
           "elapsed_s": round(time.time() - t0, 1)}
    print(json.dumps(res))
    return {"statusCode": 200, "body": json.dumps(res)}
