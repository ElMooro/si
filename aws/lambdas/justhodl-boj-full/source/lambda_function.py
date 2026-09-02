"""justhodl-boj-full v1.1.1 -- Bank of Japan full warehouse.

Lanes:
  ZIPS  portal flat-file zips (16/16 banked ops 4985), page
        harvest + sha-conditional
  API   the FULL series universe -- getMetadata?db=X lists every
        code (22 dbs / 120,394 series discovered ops 4987);
        getDataCode pulls values. The API 400s wide windows
        (full-window single-code PROVEN refused), so values pull
        in CHUNK_Y-year slices from API_START, 40-code batches,
        single-code fallback on batch-400. Per-db shard state
        (data/warm/boj-full/_state/api_{db}.json) so ops can
        drive 6 parallel lanes. Chains are AWS-terminated;
        ops sync-drives, rate(24h) keeps fresh.
"""
import gzip
import hashlib
import json
import os
import re
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import boto3

ENGINE_VERSION = "justhodl-boj-full v1.1.3 ops4987 manifest-totals"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
SITE = "https://www.stat-search.boj.or.jp"
PAGE = SITE + "/info/dload_en.html"
API = SITE + "/api/v1/"
ROOT = "data/warm/boj-full/"
STATE_KEY = ROOT + "_state/state.json"
MANIFEST_KEY = ROOT + "manifest.json"
UA = {"User-Agent": "Mozilla/5.0 JustHodl Research "
      "(raafouis@gmail.com)"}
BUDGET_S = int(os.environ.get("BOJ_BUDGET_S", "640"))
CHUNK_Y = int(os.environ.get("BOJ_CHUNK_Y", "10"))
API_START = 1955
BATCH = 60
GUARD = 60
DB_CANDS = ["MD01", "MD02", "MD11", "MD12", "MD13", "CO", "IR01",
            "IR02", "IR03", "IR04", "FM01", "FM02", "FM03",
            "FM08", "BP01", "BP02", "PR01", "PR02", "PR03",
            "BS01", "BS02", "DL01", "FF", "FFYS", "TK", "ST01",
            "PS01", "CA01", "LA01", "SK01"]

s3 = boto3.client("s3", region_name="us-east-1")


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _j(key, default=None):
    try:
        return json.loads(s3.get_object(
            Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return default


def _put(key, obj):
    s3.put_object(Bucket=BUCKET, Key=key,
                  Body=json.dumps(obj, indent=1).encode(),
                  ContentType="application/json")


def api_fetch(path, timeout=60, cap=64_000_000):
    req = urllib.request.Request(
        API + path, headers={**UA, "Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read(cap)


# ── API lane ────────────────────────────────────────────────────
def api_discover(state, t0):
    ap = state.setdefault("api", {"dbs": {}, "invalid": {}})
    for db in DB_CANDS:
        if db in ap["dbs"] or db in ap["invalid"]:
            continue
        if time.time() - t0 > BUDGET_S - GUARD:
            return
        try:
            raw = api_fetch("getMetadata?format=json&lang=en&db="
                            + db)
            js = json.loads(raw)
            codes = []

            def walk(o):
                if isinstance(o, dict):
                    for k, v in o.items():
                        if k.upper() in ("SERIES_CODE", "CODE") \
                                and isinstance(v, str) and \
                                len(v) >= 4:
                            codes.append(v)
                        else:
                            walk(v)
                elif isinstance(o, list):
                    for it in o:
                        walk(it)
            walk(js)
            codes = sorted(set(codes))
            if codes:
                ap["dbs"][db] = {"codes": codes}
            else:
                ap["invalid"][db] = "no-codes"
        except urllib.error.HTTPError as e:
            ap["invalid"][db] = "HTTP %s" % e.code
        except Exception as e:
            ap["invalid"][db] = str(e)[:70]
        time.sleep(0.4)


def _dbstate_key(db):
    return ROOT + "_state/api_%s.json" % db


def api_drain_db(db, budget_end, state):
    st = _j(_dbstate_key(db), None)
    if not st:
        agg = (state.get("api") or {}).get("dbs") or {}
        st = {"db": db, "codes": (agg.get(db) or {}
                                  ).get("codes") or [],
              "done": 0, "parts": 0, "rows": 0, "fail": None}
    # ops 5069: PER-DB LEASE. The fanout target sits on a rate(5
    # minutes) rule but a db run may take the full 780s, so a second
    # wave arrives while the first is still draining and both read the
    # same `done` index -- re-fetching identical codes and racing to
    # write api_{db}.json. The lane has no lease on the api_only path
    # (the main path's lease check is bypassed by the early return), so
    # the lease has to live per db, here.
    if float(st.get("lease_until") or 0) > time.time():
        st["skipped_leased"] = int(st.get("skipped_leased", 0)) + 1
        return st
    st["lease_until"] = budget_end + 60
    _put(_dbstate_key(db), st)   # ops 5110: was api_key(db) -- undefined -> NameError on every per-db child run
    codes = st.get("codes") or []
    now_y = datetime.now(timezone.utc).year
    wins, y = [], API_START
    while y <= now_y:
        wins.append(("%d01" % y,
                     "%d12" % min(y + CHUNK_Y - 1, now_y)))
        y += CHUNK_Y

    def bank(raw, w0):
        st["parts"] += 1
        s3.put_object(
            Bucket=BUCKET,
            Key=ROOT + "api/%s/part%05d_%s.json.gz" % (
                db, st["parts"], w0[:4]),
            Body=gzip.compress(raw),
            ContentType="application/gzip",
            Metadata={"engine": "boj-full", "db": db,
                      "win": w0})
        st["rows"] += raw.count(b"SURVEY_DATES")

    while st["done"] < len(codes):
        if time.time() > budget_end:
            break
        batch = codes[st["done"]:st["done"] + BATCH]
        for w0, w1 in wins:
            if time.time() > budget_end:
                break
            try:
                raw = api_fetch(
                    "getDataCode?format=json&lang=en&db=%s"
                    "&startDate=%s&endDate=%s&code=%s"
                    % (db, w0, w1, ",".join(batch)), timeout=90)
                if b"SURVEY_DATES" in raw:
                    bank(raw, w0)
            except urllib.error.HTTPError as e:
                if e.code == 400 and len(batch) > 1:
                    for c in batch:
                        try:
                            raw = api_fetch(
                                "getDataCode?format=json&lang=en"
                                "&db=%s&startDate=%s&endDate=%s"
                                "&code=%s" % (db, w0, w1, c),
                                timeout=60)
                            if b"SURVEY_DATES" in raw:
                                bank(raw, w0)
                        except Exception:
                            pass
                        time.sleep(0.08)
                else:
                    st["fail"] = "HTTP %s @%d %s" % (
                        e.code, st["done"], w0)
            except Exception as e:
                st["fail"] = "@%d %s" % (st["done"], str(e)[:60])
            time.sleep(0.1)
        st["done"] += len(batch)
        _put(_dbstate_key(db), st)
    _put(_dbstate_key(db), st)
    return st


# ── ZIP lane (unchanged from v1.0.0) ────────────────────────────
def zips_lane(state, t0):
    zl = state.setdefault("zips", {})
    hrefs = []
    try:
        req = urllib.request.Request(PAGE, headers=UA)
        with urllib.request.urlopen(req, timeout=60) as r:
            html = r.read(4_000_000).decode("utf-8", "replace")
        hrefs = sorted(set(re.findall(
            r'href="([^"]+?\.zip)"', html, re.I)))
        state["universe"] = len(hrefs)
    except Exception as e:
        state["failures"]["_page"] = str(e)[:90]
        hrefs = [v.get("href") for v in zl.values()
                 if v.get("href")]
    for h in hrefs:
        if time.time() - t0 > BUDGET_S - GUARD:
            break
        url = h if h.startswith("http") else (
            SITE + h if h.startswith("/") else
            SITE + "/info/" + h)
        name = re.sub(r"[^A-Za-z0-9._-]+", "_",
                      h.rsplit("/", 1)[-1])[:120]
        prev = zl.get(name) or {}
        if prev.get("ok") and \
                time.time() - float(prev.get("epoch") or 0) \
                < 11 * 3600:
            continue
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=180) as r:
                raw = r.read(300_000_000)
            if len(raw) < 500 or raw[:2] != b"PK":
                raise RuntimeError("not-a-zip (%dB)" % len(raw))
            dig = hashlib.sha256(raw).hexdigest()[:16]
            if prev.get("sha") != dig:
                s3.put_object(Bucket=BUCKET, Key=ROOT + name,
                              Body=raw,
                              ContentType="application/zip",
                              Metadata={"engine": "boj-full",
                                        "src": url[:110]})
            zl[name] = {"ok": True, "bytes": len(raw),
                        "sha": dig, "href": h,
                        "status": "fresh"
                        if prev.get("sha") != dig
                        else "unchanged",
                        "epoch": time.time(), "at": _now()}
            state["failures"].pop(name, None)
        except Exception as e:
            zl[name] = {"ok": False, "epoch": time.time(),
                        "href": h}
            state["failures"][name] = str(e)[:90]
        time.sleep(0.5)


def write_manifest(state):
    zl = state.get("zips") or {}
    ok = {k: v for k, v in zl.items() if v.get("ok")}
    ap = state.get("api") or {}
    dbs = ap.get("dbs") or {}
    tot_done = tot_parts = tot_rows = 0
    for db in dbs:
        ds = _j(_dbstate_key(db)) or {}
        tot_done += ds.get("done", 0)
        tot_parts += ds.get("parts", 0)
        tot_rows += ds.get("rows", 0)
    _put(MANIFEST_KEY, {
        "as_of": state.get("as_of") or _now(),
        "engine": "justhodl-boj-full", "version": "1.1.1",
        "zips": len(ok), "universe": state.get("universe"),
        "mb": round(sum(v.get("bytes") or 0
                        for v in ok.values()) / 1e6, 1),
        "api_dbs": len(dbs),
        "api_invalid": len(ap.get("invalid") or {}),
        "api_series": sum(
            len((_j(_dbstate_key(d)) or {}).get("codes")
                or (m.get("codes") or [])) for d, m in
            dbs.items()),
        "api_series_done": tot_done,
        "api_parts": tot_parts,
        "api_rows": tot_rows,
        "failures": len(state.get("failures") or {}),
        "note": ("BOJ full warehouse: portal flat-file zips + "
                 "API universe (22 dbs · 120k series via "
                 "getMetadata; window-chunked full-history "
                 "values); invalid dbs named")})


def lambda_handler(event, ctx=None):
    global BUDGET_S
    t0 = time.time()
    event = event or {}
    if event.get("budget_s"):
        BUDGET_S = int(event["budget_s"])
    state = _j(STATE_KEY, None) or {"version": "1.1.1",
                                    "zips": {}, "failures": {}}

    if event.get("fanout"):
        # ops 5068: the api lane walks every db SEQUENTIALLY inside one
        # 780s invocation, so a 22-db universe drains one db at a time
        # and the later ones never get reached before the budget ends.
        # db_filter is already a natural shard key -- fan out one
        # invocation per db and each gets the full budget to itself.
        # Fan-out, not a chain: a fanout never fans out.
        if not ((state.get("api") or {}).get("dbs")):
            api_discover(state, t0)
            _put(STATE_KEY, state)
        dbs = sorted((state.get("api") or {}).get("dbs") or {})
        _l = boto3.client("lambda", region_name="us-east-1")
        fn = os.environ.get("AWS_LAMBDA_FUNCTION_NAME",
                            "justhodl-boj-full")
        sent = 0
        for db in dbs:
            try:
                _l.invoke(FunctionName=fn, InvocationType="Event",
                          Payload=json.dumps({"api_only": True,
                                              "db_filter": [db]}
                                             ).encode())
                sent += 1
            except Exception:
                pass
        return {"ok": True, "mode": "fanout", "dbs": len(dbs),
                "invoked": sent}

    if event.get("api_only"):
        if not ((state.get("api") or {}).get("dbs")):
            api_discover(state, t0)
            _put(STATE_KEY, state)
        end = t0 + BUDGET_S - GUARD
        res = {}
        targets = event.get("db_filter") or \
            sorted((state.get("api") or {}).get("dbs") or {})
        for db in targets:
            if time.time() > end:
                break
            ds = api_drain_db(db, end, state)
            res[db] = [ds["done"], len(ds.get("codes") or []),
                       ds["parts"]]
        state["as_of"] = _now()
        _put(STATE_KEY, state)
        write_manifest(state)
        return {"ok": True, "mode": "api", "res": res,
                "elapsed_s": round(time.time() - t0, 1)}

    if float(state.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held"}
    state["lease_until"] = time.time() + BUDGET_S + 120
    _put(STATE_KEY, state)
    zips_lane(state, t0)
    api_discover(state, t0)
    state["lease_until"] = 0
    state["as_of"] = _now()
    _put(STATE_KEY, state)
    write_manifest(state)
    ok = {k: v for k, v in state["zips"].items() if v.get("ok")}
    return {"ok": True, "zips": len(ok),
            "universe": state.get("universe"),
            "api_dbs": len(((state.get("api") or {})
                            .get("dbs") or {})),
            "failures": len(state["failures"]),
            "elapsed_s": round(time.time() - t0, 1)}
