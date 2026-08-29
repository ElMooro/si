"""justhodl-series-extractor (ops 4547) — Perplexity's reference
implementation: parse EVERY series out of a provider's raw warm files,
not just list the files. Eurostat first (proves the pattern), StatCan
and OECD next with their own readers, same output contract.

GUARDRAILS (both explicit, both enforced):
1. series_extracted is counted honestly and kept SEPARATE from
   datasets_total until a flow is fully parsed — never folds the
   theoretical dimension cross-product in as if it were rows read.
2. Output is hard-paginated (500/page) and the UI is told to require
   a 2+ char search before rendering — this writes the data, it does
   not render it unfiltered.
"""
import csv
import gzip
import hashlib
import io
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
WRITERS = 32   # ops 5034: page writes were SERIAL. At ~40ms of S3
               # round-trip each that capped the engine near 4
               # pages/s while the parser sat idle. 32 in flight
               # moves the ceiling onto CPU where it belongs.
s3 = boto3.client("s3", region_name="us-east-1",
                  config=Config(max_pool_connections=WRITERS * 2,
                                retries={"max_attempts": 5,
                                         "mode": "adaptive"}))
PAGE = 500
BUDGET_S = 840  # 900s timeout, 60s reserved to drain + checkpoint
RESERVE_MS = 60000   # never enter another page write with less left than this
MAX_PAGES_PER_RUN = 200000  # effectively uncapped: the run is now
                          # bounded by TIME, not by an arbitrary count
STALL_ATTEMPTS = 3        # a flow that cannot advance is skipped, not
                          # retried forever (the Aug-09 failure mode)
ERROR_ATTEMPTS = 3        # a flow that RAISES is retired after 3 tries;
                          # AVIA_GOEXAC MemoryError blocked the other
                          # 8,018 flows behind it because the except
                          # branch recorded the error and retried forever


def _out_of_time(t0, context):
    """True when the run must checkpoint NOW.

    ops 5028: the original only tested the budget BETWEEN flows. One
    Eurostat flow larger than the whole budget therefore ran until the
    280s Lambda timeout killed the process -- so the state put_object at
    the end NEVER executed, flows_done never advanced past 79, and every
    5 minutes the same flow was re-extracted and the same ~1540 pages
    rewritten into a versioned bucket. 444k dead versions/day, 112GB/day,
    from 2026-08-09T02:40 until ops 5027 stopped it."""
    if time.time() - t0 > BUDGET_S:
        return True
    try:
        return context.get_remaining_time_in_millis() < RESERVE_MS
    except Exception:
        return False


def _flow_id_from_key(key):
    # data/warm/eurostat/data/NAMA_10_A64_P5.dat.gz -> NAMA_10_A64_P5
    base = key.rsplit("/", 1)[-1]
    for suf in (".dat.gz", ".gz", ".tsv.gz", ".dat"):
        if base.endswith(suf):
            return base[: -len(suf)]
    return base


def extract_eurostat(gz_bytes, flow_id, engine_index):
    """Eurostat TSV: header 'dim1,dim2,...,dimN\\TIME_PERIOD<TAB>p1<TAB>p2...'
    one row per unique dimension-combination = one series."""
    # ops 5033: gzip.decompress() materialised the WHOLE file as bytes and
    # then .decode() made a second full copy as str. A 42MB .gz of TSV
    # expands to several hundred MB, so AVIA_GOEXAC raised MemoryError on
    # every attempt and -- because the except branch retried forever --
    # blocked the other 8,018 flows behind it. Stream it instead: peak
    # memory is now one line, not the whole dataset.
    f = io.TextIOWrapper(
        gzip.GzipFile(fileobj=io.BytesIO(gz_bytes), mode="rb"),
        encoding="utf-8", errors="ignore", newline="")
    hdr = f.readline().rstrip("\n")
    dims_part, sep, periods_part = hdr.partition("\\TIME_PERIOD")
    if not sep:
        return
    dims = [d.strip() for d in dims_part.split(",")]
    periods = [p.strip() for p in periods_part.split("\t") if p.strip()]
    unit_ix = dims.index("unit") if "unit" in dims else None
    geo_ix = -1 if dims else None
    for line in f:
        line = line.rstrip("\n")
        if not line:
            continue
        keypart, tab, valpart = line.partition("\t")
        if not tab:
            continue
        dimvals = [d.strip() for d in keypart.split(",")]
        vals = [v.strip() for v in valpart.split("\t")]
        last_i = None
        for i in range(len(vals) - 1, -1, -1):
            if i < len(vals) and vals[i] not in (":", "", ": ", ":u",
                                                  ":c", ":e"):
                last_i = i
                break
        if last_i is None or last_i >= len(periods):
            continue
        try:
            lv = float(vals[last_i].split(" ")[0])
        except ValueError:
            continue
        sid = "eurostat:" + flow_id + ":" + ".".join(dimvals)
        yield {
            "id": sid,
            "flow": flow_id,
            "name": flow_id + " · " + " · ".join(dimvals[:3]) +
            (" …" if len(dimvals) > 3 else ""),
            "dims": dict(zip(dims, dimvals)),
            "unit": (dimvals[unit_ix] if unit_ix is not None and
                     unit_ix < len(dimvals) else None),
            "freq": dimvals[0] if dimvals else None,
            "geo": dimvals[geo_ix] if dimvals else None,
            "first_obs": periods[0] if periods else None,
            "last_obs": periods[last_i],
            "last_value": lv,
            "status": "LIVE",
            "source_url": ("https://ec.europa.eu/eurostat/databrowser"
                           "/view/" + flow_id),
            "raw_key": None,  # filled by caller
            "engines": engine_index.get("eurostat:" + flow_id, []),
        }


EXTRACTORS = {"eurostat": extract_eurostat}


def _load_engine_index():
    try:
        ov = s3.get_object(Bucket=BUCKET,
                           Key="data/audit/engine-writes-overrides.json")
        d = json.loads(ov["Body"].read())
        idx = {}
        for eng, ws in (d.get("writes") or {}).items():
            for w in ws:
                idx.setdefault(w, []).append(eng)
        return idx
    except Exception:
        return {}


def lambda_handler(event, context):
    t0 = time.time()
    provider = (event or {}).get("provider", "eurostat")
    extractor = EXTRACTORS.get(provider)
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    if not extractor:
        return {"statusCode": 400,
                "body": json.dumps({"error": f"no extractor for "
                                             f"{provider} yet"})}
    state_key = f"data/_state/series-extract-{provider}.json"
    try:
        state = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key=state_key)["Body"]
                           .read())
    except Exception:
        state = {"flows_done": [], "series_count": 0, "n_pages": 0,
                 "buffer": []}
    engine_index = _load_engine_index()
    prefix = f"data/warm/{provider}/data/"
    tok = None
    all_keys = []
    while True:
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        r = s3.list_objects_v2(**kw)
        all_keys.extend(o["Key"] for o in r.get("Contents", []))
        if not r.get("IsTruncated"):
            break
        tok = r.get("NextContinuationToken")
    todo = [k for k in all_keys
            if _flow_id_from_key(k) not in state["flows_done"]]
    buf = state.get("buffer", [])
    processed_flows = []
    progress = state.setdefault("flow_progress", {})
    hashes = state.setdefault("page_hashes", {})
    pages_this_run = 0
    stopped_early = False

    if not state.get("pages_seeded"):
        # ops 5040: everything written before this counter existed still
        # has to be counted, exactly once. Safe here because reserved
        # concurrency is 1 -- no other run can be mutating state.
        _sn, _sb, _tok = 0, 0, None
        try:
            while True:
                _kw = {"Bucket": BUCKET, "MaxKeys": 1000,
                       "Prefix": f"data/providers/{provider}/series/"}
                if _tok:
                    _kw["ContinuationToken"] = _tok
                _r = s3.list_objects_v2(**_kw)
                for _o in _r.get("Contents", []):
                    _sn += 1
                    _sb += _o["Size"]
                if not _r.get("IsTruncated"):
                    break
                _tok = _r.get("NextContinuationToken")
            state["pages_objects"] = _sn
            state["pages_bytes"] = _sb
            state["pages_seeded"] = now
        except Exception as _e:
            state["pages_seed_error"] = str(_e)[:90]

    pool = ThreadPoolExecutor(max_workers=WRITERS)
    pending = []
    write_errors = []

    def _collect(wait_all):
        """Harvest finished page writes. series_count and page_hashes
        advance ONLY for pages that actually landed, so a failed write
        can never be recorded as durable. A page that never lands is
        named in state['missing_pages'] for a targeted repair pass."""
        nonlocal pending
        still = []
        for fut, pkey, digest, n, nb in pending:
            if wait_all or fut.done():
                try:
                    fut.result()
                    hashes[pkey] = digest
                    state["series_count"] += n
                    # ops 5040: the writer already knows every page's
                    # size. Carrying the totals forward here is O(1) and
                    # exact; re-deriving them by LISTing 2M objects on a
                    # daily catalog run is what timed the catalog out.
                    state["pages_objects"] = int(
                        state.get("pages_objects", 0)) + 1
                    state["pages_bytes"] = int(
                        state.get("pages_bytes", 0)) + nb
                except Exception as e:
                    write_errors.append("%s: %s" % (pkey, str(e)[:70]))
                    state.setdefault("missing_pages", []).append(pkey)
            else:
                still.append((fut, pkey, digest, n, nb))
        pending = still

    def _put(pkey, body):
        s3.put_object(Bucket=BUCKET, Key=pkey, Body=body,
                      ContentType="application/json",
                      CacheControl="no-cache")

    def _flush(rows):
        """Queue one page write. A byte-identical rewrite is SKIPPED --
        on a versioned bucket an identical put still creates a new
        noncurrent version, which is what made this engine cost
        $2.49/day in PUT requests and 112GB/day in dead storage."""
        pkey = (f"data/providers/{provider}/series/"
                f"page-{state['n_pages']:04d}.json")
        body = json.dumps({"page": state["n_pages"], "count": len(rows),
                           "rows": rows}, default=str).encode()
        digest = hashlib.sha256(body).hexdigest()[:32]
        state["n_pages"] += 1
        if hashes.get(pkey) == digest:
            state["series_count"] += len(rows)
            return False
        pending.append((pool.submit(_put, pkey, body), pkey, digest,
                        len(rows), len(body)))
        if len(pending) >= WRITERS * 3:
            _collect(False)
        return True

    def _checkpoint():
        _collect(True)          # never persist state ahead of the writes
        state["buffer"] = buf
        state["updated_at"] = now
        if write_errors:
            state["write_errors"] = write_errors[-20:]
        s3.put_object(Bucket=BUCKET, Key=state_key,
                      Body=json.dumps(state, default=str).encode(),
                      ContentType="application/json")

    for key in todo:
        if _out_of_time(t0, context) or pages_this_run >= MAX_PAGES_PER_RUN:
            stopped_early = True
            break
        fid = _flow_id_from_key(key)
        prog = progress.get(fid) or {}
        skip_rows = int(prog.get("rows_done", 0))
        attempts = int(prog.get("attempts", 0)) + 1
        # ops 5030 circuit breaker: a flow is only allowed to be retried
        # while it is ADVANCING. rows_done grows every run thanks to the
        # resume offset, so this can only fire on a genuinely stuck flow
        # -- never on a merely large one. Flow #80 blocked the entire
        # 8,147-flow lane for 20 days because nothing enforced this.
        if (attempts > STALL_ATTEMPTS
                and skip_rows <= int(prog.get("rows_at_last_check", -1))):
            state.setdefault("errors", {})[fid] = (
                f"stalled after {attempts} attempts at row {skip_rows} "
                f"-- skipped to keep the lane moving")
            if fid not in state["flows_done"]:
                state["flows_done"].append(fid)
            progress.pop(fid, None)
            _checkpoint()
            continue
        progress[fid] = {"rows_done": skip_rows, "attempts": attempts,
                         "rows_at_last_check": skip_rows,
                         "error_count": int(prog.get("error_count", 0))}
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            raw = obj["Body"].read()
            n_added = 0
            row_i = 0
            finished = True
            for rec in extractor(raw, fid, engine_index):
                row_i += 1
                if row_i <= skip_rows:      # resume a part-done flow
                    continue
                rec["raw_key"] = key
                buf.append(rec)
                n_added += 1
                if len(buf) >= PAGE:
                    if _flush(buf):
                        pages_this_run += 1
                    buf = []
                    # budget is now checked INSIDE the flow: a flow bigger
                    # than one invocation is resumed, never restarted
                    if (_out_of_time(t0, context)
                            or pages_this_run >= MAX_PAGES_PER_RUN):
                        progress[fid] = {"rows_done": row_i,
                                         "attempts": attempts,
                                         "rows_at_last_check": skip_rows,
                                         "updated_at": now}
                        finished = False
                        stopped_early = True
                        break
            if finished:
                if fid not in state["flows_done"]:
                    state["flows_done"].append(fid)
                progress.pop(fid, None)
                processed_flows.append((fid, n_added))
                _checkpoint()      # progress survives any later timeout
            else:
                _checkpoint()
                break
        except Exception as e:
            ec = int((progress.get(fid) or {}).get("error_count", 0)) + 1
            state.setdefault("errors", {})[fid] = (
                f"{type(e).__name__}: {str(e)[:70]} (attempt {ec})")
            if ec >= ERROR_ATTEMPTS:
                # retire it: an identical input raising an identical
                # exception will not behave differently on attempt 4.
                # It stays listed in errors[] for a later targeted pass.
                if fid not in state["flows_done"]:
                    state["flows_done"].append(fid)
                state.setdefault("failed_flows", [])
                if fid not in state["failed_flows"]:
                    state["failed_flows"].append(fid)
                progress.pop(fid, None)
            else:
                pr = progress.get(fid) or {}
                pr["error_count"] = ec
                progress[fid] = pr
            _checkpoint()
    _collect(True)
    pool.shutdown(wait=True)
    state["buffer"] = buf
    state["stopped_early"] = stopped_early
    state["pages_this_run"] = pages_this_run
    state["write_errors_this_run"] = len(write_errors)
    state["updated_at"] = now
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(state, default=str).encode(),
                  ContentType="application/json")
    # manifest: honest, SEPARATE count. Never folds into datasets_total.
    manifest = {
        "provider": provider,
        "updated_at": now,
        "flows_total": len(all_keys),
        "flows_parsed": len(state["flows_done"]),
        "series_extracted": state["series_count"] + len(buf),
        "n_pages": state["n_pages"],
        "pages": int(state.get("pages_objects") or 0),
        "pages_bytes": int(state.get("pages_bytes") or 0),
        "page_size": PAGE,
        "note": ("series_extracted is a SEPARATE metric from the "
                "provider's datasets/datasets_total headline — it "
                "is not folded in until fully parsed (ops 4547)."),
        "ui_rule": "require 2+ char search before rendering rows"}
    s3.put_object(Bucket=BUCKET,
                  Key=f"data/providers/{provider}/series-manifest.json",
                  Body=json.dumps(manifest, default=str).encode(),
                  ContentType="application/json",
                  CacheControl="no-cache")
    res = {"ok": True, "provider": provider,
          "processed_flows": len(processed_flows),
          "series_this_run": sum(n for _, n in processed_flows),
          **manifest}
    print(json.dumps({k: res[k] for k in
                      ("processed_flows", "series_this_run",
                       "series_extracted", "flows_parsed",
                       "flows_total")}))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
