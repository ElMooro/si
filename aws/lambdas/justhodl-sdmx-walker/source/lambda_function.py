"""justhodl-sdmx-walker — the E10 data-walks (ops 4503). Four agencies,
one cursor machine: full-dataset pulls flow-by-flow from the catalogs
already banked. Priority-ordered (BIS all first; OECD CLI/MEI/QNA family;
Eurostat macro prefixes before the long tail; StatCan priority cubes),
2 flows/agency/run hourly, 40MB byte-cap per flow (truncated flagged,
never silently), per-agency state with failures ledger. COMPLETE is a
computed fact, not a claim."""
import gzip
import io
import json
import time
import os
import urllib.request
import zipfile
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
# ops 4538 (Khalid: expedite): 2/agency/hour was ~170 days for Eurostat.
# 12/agency + 15-min cadence + elapsed budget = ~50x, still polite.
PER = int(os.environ.get("FLOWS_PER_AGENCY", "12"))
BUDGET_S = int(os.environ.get("WALK_BUDGET_S", "700"))
CAP = 40 * 1024 * 1024
s3 = boto3.client("s3", region_name="us-east-1")

EURO_PRI = ("ei_", "nama_", "namq_", "une_", "prc_hicp", "sts_",
            "irt_", "gov_", "bop_", "ext_")
OECD_PRI = ("CLI", "MEI", "QNA", "KEI", "STLABOUR", "PRICES", "FIN")


def _get_json(key):
    b = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    if key.endswith(".gz"):
        b = gzip.decompress(b)
    return json.loads(b)


def _fetch_capped(u, headers=None, timeout=240):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "Chrome/126 Safari/537.36", "Accept": "*/*"}
    h.update(headers or {})
    req = urllib.request.Request(u, headers=h)
    buf = io.BytesIO()
    truncated = False
    with urllib.request.urlopen(req, timeout=timeout) as r:
        while True:
            c = r.read(4 * 1024 * 1024)
            if not c:
                break
            buf.write(c)
            if buf.tell() >= CAP:
                truncated = True
                break
    b = buf.getvalue()
    if b[:2] == b"\x1f\x8b":
        try:
            b = gzip.decompress(b)
        except Exception:
            pass
    return b, truncated


def _state(agency):
    k = f"data/_state/sdmx-walk-{agency}.json"
    try:
        return k, json.loads(s3.get_object(Bucket=BUCKET,
                                           Key=k)["Body"].read())
    except Exception:
        return k, {"done": [], "failures": {}}


def _save(k, st, total):
    nd = len(set(st["done"]))
    st["as_of"] = datetime.now(timezone.utc).isoformat(
        timespec="seconds")
    st["n_total"] = total
    st["progress_pct"] = round(100 * nd / total, 2) if total else 0
    st["status"] = "COMPLETE" if total and nd >= total else "converging"
    s3.put_object(Bucket=BUCKET, Key=k,
                  Body=json.dumps(st, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return st["progress_pct"], st["status"]


def _order(ids, pri_prefixes):
    pri = [i for i in ids if str(i).upper().startswith(
        tuple(p.upper() for p in pri_prefixes))]
    rest = [i for i in ids if i not in set(pri)]
    return pri + rest


def _walk_generic(agency, ids, url_fn, out_prefix, S):
    k, st = _state(agency)
    done = set(st["done"])
    todo = [i for i in ids if i not in done][:PER]
    _wt0 = time.time()
    # ops 4539: 4-way parallel fetch — Eurostat flows are 5-50MB TSVs;
    # serial pulls wasted the budget on wire time. Fetch in threads,
    # ledger sequentially (state stays race-free).
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _dl_one(_fid):
        return _fetch_capped(url_fn(_fid))
    _pool = ThreadPoolExecutor(max_workers=4)
    _futs = {_pool.submit(_dl_one, f2): f2 for f2 in todo}
    _results = {}
    for _fu in as_completed(_futs, timeout=BUDGET_S):
        _fid2 = _futs[_fu]
        try:
            _results[_fid2] = ("ok", _fu.result())
        except Exception as _e:
            _results[_fid2] = ("err", _e)
        if time.time() - _wt0 > BUDGET_S:
            break
    _pool.shutdown(wait=False, cancel_futures=True)
    got = 0
    for fid in todo:
        if fid not in _results:
            break  # budget cut the tail; resumes next run
        u = url_fn(fid)
        try:
            _tag, _payload = _results[fid]
            if _tag == "err":
                raise _payload
            raw, trunc = _payload
            if len(raw) < 200:
                raise ValueError(f"tiny {len(raw)}b")
            s3.put_object(
                Bucket=BUCKET,
                Key=f"{out_prefix}/{str(fid).replace('/', '_')}"
                    f".dat.gz",
                Body=gzip.compress(raw),
                Metadata={"truncated": str(trunc),
                          "source": u[:200]},
                ContentType="application/gzip")
            if trunc:
                st.setdefault("truncated", []).append(fid)
            st["done"].append(fid)
            got += 1
        except Exception as e:
            st.setdefault("failures", {})[str(fid)] = \
                f"{type(e).__name__}: {str(e)[:60]}"
            st["done"].append(fid)  # advance; failures ledger keeps it
    pct, status = _save(k, st, len(ids))
    S[agency] = {"pulled": got, "of_run": len(todo),
                 "progress_pct": pct, "status": status,
                 "n_total": len(ids),
                 "n_failures": len(st.get("failures", {}))}


AGENTS = ("bis", "eurostat", "oecd", "statcan")


def lambda_handler(event, context):
    S = {"as_of": datetime.now(timezone.utc).isoformat(
        timespec="seconds")}
    ag = (event or {}).get("agency")
    if not ag:
        # ops 4539: cron fans out one async invoke PER agency — each
        # gets its own full 700s budget instead of sharing one.
        import boto3 as _b3
        _lam = _b3.client("lambda", region_name="us-east-1")
        for _a in AGENTS:
            _lam.invoke(FunctionName=context.function_name,
                        InvocationType="Event",
                        Payload=json.dumps({"agency": _a}).encode())
        return {"statusCode": 200,
                "body": json.dumps({"fanout": list(AGENTS)})}
    S["agency_mode"] = ag
    try:
        bis_ids = [f["id"] for f in _get_json(
            "data/warm/bis/catalog.json.gz")["dataflows"]]
        if ag == "bis":
          _walk_generic(
            "bis", bis_ids,
            lambda f: (f"https://stats.bis.org/api/v1/data/{f}/all/all"
                       f"?format=csv"),
            "data/warm/bis/data", S)
    except Exception as e:
        S["bis"] = {"data_unavailable": True,
                    "reason": f"{type(e).__name__}: {str(e)[:60]}"}
    try:
        eu_ids = _order([f["id"] for f in _get_json(
            "data/warm/eurostat/catalog.json.gz")["dataflows"]],
            EURO_PRI)
        if ag == "eurostat":
          _walk_generic(
            "eurostat", eu_ids,
            lambda f: ("https://ec.europa.eu/eurostat/api/"
                       "dissemination/sdmx/2.1/data/"
                       f"{f}?format=TSV&compressed=true"),
            "data/warm/eurostat/data", S)
    except Exception as e:
        S["eurostat"] = {"data_unavailable": True,
                         "reason": f"{type(e).__name__}: "
                                   f"{str(e)[:60]}"}
    try:
        oe_ids = _order([f["id"] for f in _get_json(
            "data/warm/oecd/catalog.json.gz")["dataflows"]], OECD_PRI)
        if ag == "oecd":
          _walk_generic(
            "oecd", oe_ids,
            lambda f: (f"https://sdmx.oecd.org/public/rest/data/{f}"
                       f"/all?format=csvfile"),
            "data/warm/oecd/data", S)
    except Exception as e:
        S["oecd"] = {"data_unavailable": True,
                     "reason": f"{type(e).__name__}: {str(e)[:60]}"}
    try:
        cubes = _get_json("data/warm/statcan/cube-catalog.json.gz")
        pl = cubes.get("payload", cubes)
        pids = [str(c.get("productId")) for c in
                (pl if isinstance(pl, list) else [])
                if c.get("productId")]
        def sc_url(pid):
            meta = json.loads(_fetch_capped(
                "https://www150.statcan.gc.ca/t1/wds/rest/"
                f"getFullTableDownloadCSV/{pid}/en",
                timeout=60)[0])
            return meta.get("object")
        def sc_fetch(pid):
            return sc_url(pid)
        if ag == "statcan":
          _walk_generic(
            "statcan", pids,
            lambda pid: sc_fetch(pid),
            "data/warm/statcan/data", S)
    except Exception as e:
        S["statcan"] = {"data_unavailable": True,
                        "reason": f"{type(e).__name__}: "
                                  f"{str(e)[:60]}"}
    s3.put_object(Bucket=BUCKET,
                  Key="data/warm/sdmx-walker-summary.json",
                  Body=json.dumps(S, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    print(json.dumps(S, default=str)[:500])
    return {"statusCode": 200, "body": json.dumps(S, default=str)}
