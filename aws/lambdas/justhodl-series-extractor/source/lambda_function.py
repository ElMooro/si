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
import re
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
MAX_ECB_SERIES = 4000000  # dims cache cap; keys still counted
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


def _ecb_flow_of(key):
    """data/warm/ecb/data/CSEC__2005_2009.dat.gz -> ("CSEC", 2005)"""
    base = key.rsplit("/", 1)[-1]
    if base.endswith(".manifest.json") or base.endswith(".json"):
        return None, 0
    name = base
    for suf in (".dat.gz", ".csv.gz", ".dat", ".csv", ".gz"):
        if name.endswith(suf):
            name = name[: -len(suf)]
            break
    fid, _, span = name.partition("__")
    fid = fid.replace("ECB.", "").split(":")[-1]
    yrs = re.findall(r"\d{4}", span)
    return fid, (int(yrs[0]) if yrs else 0)


def group_ecb(all_keys):
    """ECB is TIME-SLICED: one flow is spread over many files
    ({FLOW}__{start}_{end}.dat.gz), so a single series appears in every
    slice it spans. Extracting per FILE would emit the same series once
    per slice with a truncated date range. Group by flow, ordered
    chronologically, so first_obs/last_obs describe the real history."""
    groups = {}
    for k in all_keys:
        fid, y = _ecb_flow_of(k)
        if not fid:
            continue
        groups.setdefault(fid, []).append((y, k))
    return {f: [k for _, k in sorted(v)] for f, v in groups.items()}


GROUPERS = {"ecb": group_ecb}

_ECB_SKIP = {"TIME_PERIOD", "OBS_VALUE", "OBS_STATUS", "OBS_CONF",
             "OBS_PRE_BREAK", "OBS_COM", "KEY", "SERIES_KEY", "VALUE",
             "TIME_FORMAT", "COLLECTION", "DECIMALS", "TITLE",
             "TITLE_COMPL", "UNIT_MULT", "COMPILING_ORG", "DISS_ORG"}


def _split_csv(line):
    # fast path: ECB csvdata is unquoted except for TITLE-style fields
    if '"' not in line:
        return line.split(",")
    return next(csv.reader([line]))


def extract_ecb_flow(keys, flow_id, engine_index, get_bytes,
                     out_of_time, start_slice, stat):
    """ECB csvdata is LONG format: one row per OBSERVATION, the series
    identified by the KEY column. Eurostat TSV is WIDE (one row per
    series, columns are periods) -- so this cannot reuse the same parser.

    Accumulates per-series first/last observation, last value and an
    observation count across every slice of the flow, then yields one
    record per distinct series. If the budget expires part-way the
    accumulator is flushed as-is with the span it covers and slice_idx
    is recorded, so the next invocation resumes at the next slice
    instead of restarting the flow (the Aug-09 failure mode)."""
    acc = {}
    dims_of = {}
    ncols = None
    for si in range(start_slice, len(keys)):
        # ops 5044: the budget must be tested BETWEEN slices too. The
        # in-slice check only fires every 65k rows, so a flow made of
        # many small slices (CSEC has 37) could otherwise run right past
        # the budget and die at the Lambda timeout -- the Aug-09 failure
        # mode again. si > start_slice guarantees at least one slice of
        # forward progress per invocation, so this can never livelock.
        if si > start_slice and out_of_time():
            stat["partial"] = True
            stat["slice_idx"] = si
            break
        stat["slice_idx"] = si
        try:
            raw = get_bytes(keys[si])
        except Exception:
            continue
        fh = io.TextIOWrapper(
            gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb"),
            encoding="utf-8", errors="ignore", newline="")
        head = fh.readline().rstrip("\n")
        if not head:
            continue
        hdr = [c.strip().upper() for c in _split_csv(head)]
        ncols = len(hdr)
        ki = hdr.index("KEY") if "KEY" in hdr else (
            hdr.index("SERIES_KEY") if "SERIES_KEY" in hdr else 0)
        ti = next((i for i, c in enumerate(hdr)
                   if "TIME_PERIOD" in c), None)
        vi = next((i for i, c in enumerate(hdr)
                   if c in ("OBS_VALUE", "VALUE")), None)
        if ti is None or vi is None:
            continue
        dim_ix = [(i, c) for i, c in enumerate(hdr)
                  if c not in _ECB_SKIP]
        n = 0
        for line in fh:
            n += 1
            if not line.strip():
                continue
            p = _split_csv(line.rstrip("\n"))
            if len(p) <= max(ki, ti, vi):
                continue
            sk = p[ki]
            tp = p[ti]
            if not sk or not tp:
                continue
            a = acc.get(sk)
            if a is None:
                acc[sk] = a = [tp, tp, None, 0]
                if len(acc) <= MAX_ECB_SERIES:
                    dims_of[sk] = {c: p[i] for i, c in dim_ix
                                   if i < len(p) and p[i]}
            if tp < a[0]:
                a[0] = tp
            if tp >= a[1]:
                a[1] = tp
                v = p[vi].strip()
                if v:
                    try:
                        a[2] = float(v)
                    except ValueError:
                        pass
            a[3] += 1
            if (n & 0xFFFF) == 0 and out_of_time():
                stat["partial"] = True
                break
        if stat.get("partial"):
            stat["slice_idx"] = si + 1
            break
    else:
        stat["done"] = True
        stat["slice_idx"] = len(keys)

    src = ("https://data.ecb.europa.eu/data/datasets/" + flow_id)
    eng = engine_index.get("ecb:" + flow_id, [])
    for sk, a in acc.items():
        d = dims_of.get(sk) or {}
        parts = sk.split(".")
        yield {
            "id": "ecb:" + flow_id + ":" + sk,
            "flow": flow_id,
            "name": flow_id + " · " + " · ".join(parts[:3]) +
            (" …" if len(parts) > 3 else ""),
            "dims": d,
            "unit": d.get("UNIT") or d.get("UNIT_MEASURE"),
            "freq": d.get("FREQ") or (parts[0] if parts else None),
            "geo": d.get("REF_AREA") or d.get("COUNTRY"),
            "first_obs": a[0],
            "last_obs": a[1],
            "last_value": a[2],
            "n_obs": a[3],
            "status": "LIVE",
            "source_url": src,
            "raw_key": None,
            "engines": eng,
        }


EXTRACTORS = {"eurostat": extract_eurostat}
GROUP_EXTRACTORS = {"ecb": extract_ecb_flow}


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
    extractor = EXTRACTORS.get(provider) or GROUP_EXTRACTORS.get(
        provider)
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

    def _get_bytes(k):
        return s3.get_object(Bucket=BUCKET, Key=k)["Body"].read()

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

    grouper = GROUPERS.get(provider)
    if grouper:
        # ---- grouped path (ECB): the unit of work is a FLOW, not a file
        groups = grouper(all_keys)
        state["flows_total_grouped"] = len(groups)
        units = [(f, ks) for f, ks in sorted(groups.items())
                 if f not in set(state["flows_done"])]
        for fid, ks in units:
            if _out_of_time(t0, context) or \
                    pages_this_run >= MAX_PAGES_PER_RUN:
                stopped_early = True
                break
            pr = progress.get(fid) or {}
            attempts = int(pr.get("attempts", 0)) + 1
            si0 = int(pr.get("slice_idx", 0))
            if (attempts > STALL_ATTEMPTS
                    and si0 <= int(pr.get("slice_at_last_check", -1))):
                state.setdefault("errors", {})[fid] = (
                    f"stalled at slice {si0}/{len(ks)} after "
                    f"{attempts} attempts -- skipped")
                if fid not in state["flows_done"]:
                    state["flows_done"].append(fid)
                state.setdefault("failed_flows", []).append(fid)
                progress.pop(fid, None)
                _checkpoint()
                continue
            progress[fid] = {"slice_idx": si0, "attempts": attempts,
                             "slice_at_last_check": si0,
                             "slices": len(ks),
                             "error_count": int(pr.get("error_count", 0))}
            stat = {"slice_idx": si0, "done": False}
            try:
                for rec in GROUP_EXTRACTORS[provider](
                        ks, fid, engine_index, _get_bytes,
                        lambda: _out_of_time(t0, context), si0, stat):
                    rec["raw_key"] = ks[0] if ks else None
                    buf.append(rec)
                    if len(buf) >= PAGE:
                        if _flush(buf):
                            pages_this_run += 1
                        buf = []
                if stat.get("done"):
                    if fid not in state["flows_done"]:
                        state["flows_done"].append(fid)
                    progress.pop(fid, None)
                    processed_flows.append((fid, len(ks)))
                else:
                    stopped_early = True
                    progress[fid] = {"slice_idx": stat["slice_idx"],
                                     "attempts": attempts,
                                     "slice_at_last_check": si0,
                                     "slices": len(ks),
                                     "updated_at": now}
                _checkpoint()
                if not stat.get("done"):
                    break
            except Exception as e:
                ec = int((progress.get(fid) or {}).get(
                    "error_count", 0)) + 1
                state.setdefault("errors", {})[fid] = (
                    f"{type(e).__name__}: {str(e)[:70]} (attempt {ec})")
                if ec >= ERROR_ATTEMPTS:
                    if fid not in state["flows_done"]:
                        state["flows_done"].append(fid)
                    state.setdefault("failed_flows", []).append(fid)
                    progress.pop(fid, None)
                else:
                    pr2 = progress.get(fid) or {}
                    pr2["error_count"] = ec
                    progress[fid] = pr2
                _checkpoint()
        todo = []

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
        # ops 5046: on a grouped provider the unit of work is a FLOW,
        # not a file. ECB's 207 flows live in 574 sliced files, so
        # len(all_keys) rendered "207/574" on the card -- reading like
        # 36% when the lane was 100% complete. Same class of mislabel as
        # counting dataflows and calling them series.
        "flows_total": (state.get("flows_total_grouped")
                        or len(all_keys)),
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
