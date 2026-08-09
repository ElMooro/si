"""justhodl-fred-catalog (ops 4552) — Khalid: what about FRED's remaining
data? FRED has no discovery mechanism (unlike the SDMX four which publish
a flat dataflow list) — it's a CATEGORY TREE, ~800k+ series total.

Two-phase, budget-bounded, resumable:
  Phase 1: recursive category-tree crawl (few thousand nodes, fast,
           cheap) -> data/_state/fred-categories.json
  Phase 2: per-category series-LIST banking (metadata only: id, title,
           freq, units, popularity, obs range — NOT full observation
           histories yet, that's a distinct, much bigger phase decided
           only after Phase 1 gives Khalid a real total to react to)
           -> data/warm/fred-catalog/series-meta/page-NNNN.json

ADDITIVE ONLY: writes to brand-new prefixes, never touches the existing
298 curated canary-macro keys.
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
s3 = boto3.client("s3", region_name="us-east-1")
BUDGET_S = 180  # ops 4556: more headroom before next cron tick
PAGE = 500


# ops 4556 (Khalid: respect the rate limit — the 429->403 incident):
# FRED's documented limit is ~120 req/min. Run well under it: 90/min =
# 0.667s minimum gap, enforced via a module-level last-call timestamp
# (persists across the sequential calls within one invoke). Real 429
# backoff (exponential, capped, limited retries). 403 = STOP touching
# FRED for the rest of THIS invocation — never retry into a block.
_MIN_INTERVAL = 60.0 / 60  # ops 4560: 60/min — headroom for fleet
_last_call = [0.0]
_blocked_this_invoke = [False]


class FredBlocked(Exception):
    pass


def _get(url, tries=4):
    if _blocked_this_invoke[0]:
        raise FredBlocked("403 seen this invoke — halting FRED calls")
    wait = _MIN_INTERVAL - (time.time() - _last_call[0])
    if wait > 0:
        time.sleep(wait)
    backoff = 5.0
    for i in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent":
                              "JustHodl research admin@justhodl.ai"})
            with urllib.request.urlopen(req, timeout=25) as r:
                _last_call[0] = time.time()
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            _last_call[0] = time.time()
            if e.code == 403:
                _blocked_this_invoke[0] = True
                raise FredBlocked(f"403 on {url[:80]}") from e
            if e.code == 429 and i < tries - 1:
                time.sleep(min(backoff, 60))
                backoff *= 2
                continue
            raise
        except Exception:
            if i == tries - 1:
                raise
            time.sleep(1.5)



# ops 4553 (Khalid: exact category IDs, Money/Banking/Finance) — scoped,
# freshness-filtered import. Only series with last_updated within
# FRESH_DAYS get their real observations pulled; everything older is
# explicitly excluded and logged, never silently dropped.
PRIORITY_ROOTS = {
    22: "Interest Rates", 15: "Exchange Rates", 24: "Monetary Data",
    46: "Financial Indicators", 23: "Banking",
    32360: "Business Lending", 32145: "Foreign Exchange Intervention"}
FRESH_DAYS = 90


def _expand_roots():
    """ops 4560 (Perplexity): fred/category/series returns only DIRECT
    children — FRED's site counts are recursive. The finished tree walk
    already sits in data/_state/fred-categories.json (4,169 nodes with
    parent_id). Expand the 7 roots through it. No re-crawl."""
    tree = json.loads(s3.get_object(
        Bucket=BUCKET,
        Key="data/_state/fred-categories.json")["Body"].read())
    cats = tree.get("cats", {})
    children = {}
    for cid_s, meta in cats.items():
        pid = meta.get("parent_id")
        if pid is not None:
            children.setdefault(int(pid), []).append(int(cid_s))
    out = {}  # cid -> (root_name, cat_name)
    for root, rname in PRIORITY_ROOTS.items():
        stack = [root]
        while stack:
            cid = stack.pop()
            cname = (cats.get(str(cid), {}) or {}).get("name")                 if cid != root else rname
            out[cid] = (rname, cname or rname)
            stack.extend(children.get(cid, []))
    return out


def _is_fresh(series_meta, now):
    lu = series_meta.get("last_updated")
    if not lu:
        return False
    try:
        d = datetime.fromisoformat(lu[:19])
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return (now - d).days <= FRESH_DAYS
    except Exception:
        return False


def _fetch_observations(sid):
    d = _get("https://api.stlouisfed.org/fred/series/observations"
             f"?series_id={sid}&api_key={FRED_KEY}&file_type=json")
    obs = [o for o in d.get("observations", [])
          if o.get("value") not in (".", "", None)]
    return obs


def _run_scoped_import(t0, now):
    state_key = "data/_state/fred-scoped-import.json"
    _etag = None
    try:
        _o = s3.get_object(Bucket=BUCKET, Key=state_key)
        st = json.loads(_o["Body"].read())
        _etag = _o.get("ETag")
    except Exception:
        st = {"cats_done": [], "series_seen": 0,
             "series_excluded_stale": 0, "series_imported": 0,
             "excluded_ids": [], "imported_ids": [], "n_pages": 0,
             "buffer": []}
    # ops 4562: single-writer guarantee (the 4561 counter-salad was a
    # read-modify-write race between cron rounds and ops writes).
    if (st.get("lease_until") or 0) > time.time():
        return {"skipped": "lease_held",
               "lease_until": st["lease_until"]}
    st["lease_until"] = time.time() + BUDGET_S + 60
    try:
        _kw = {"Bucket": BUCKET, "Key": state_key,
              "Body": json.dumps(st, default=str).encode(),
              "ContentType": "application/json"}
        if _etag:
            _kw["IfMatch"] = _etag
        s3.put_object(**_kw)
    except Exception:
        return {"skipped": "lost_lease_race"}
    expanded = _expand_roots()
    st["n_categories_expanded"] = len(expanded)
    already = set(st.get("imported_ids") or [])
    cutoff = (now - __import__("datetime").timedelta(
        days=FRESH_DAYS)).strftime("%Y-%m-%d")
    todo_cats = [c for c in expanded if c not in st["cats_done"]]
    buf = st.get("buffer", [])
    for cid in todo_cats:
        if time.time() - t0 > BUDGET_S:
            break
        rname, cname = expanded[cid]
        fresh_batch = []
        offset = 0
        page_broke_on_stale = False
        try:
            while True:
                if _blocked_this_invoke[0]:
                    raise FredBlocked("halted before category/series")
                # ops 4560 (Perplexity): order by last_updated desc and
                # BREAK paging on the first stale row — skips the bulk
                # of every stale subtree.
                d = _get(
                    "https://api.stlouisfed.org/fred/category/series"
                    f"?category_id={cid}&api_key={FRED_KEY}"
                    "&file_type=json&limit=1000"
                    "&order_by=last_updated&sort_order=desc"
                    f"&offset={offset}")
                serieslist = d.get("seriess", [])
                for s in serieslist:
                    st["series_seen"] += 1
                    lu = (s.get("last_updated") or "")[:10]
                    if lu and lu < cutoff:
                        st["series_excluded_stale"] += 1
                        page_broke_on_stale = True
                        break
                    title = (s.get("title") or "").upper()
                    if "DISCONTINUED" in title:
                        st["series_excluded_discontinued"] = \
                            st.get("series_excluded_discontinued",
                                   0) + 1
                        continue
                    if s.get("id") in already:
                        st["series_skipped_already"] = \
                            st.get("series_skipped_already", 0) + 1
                        continue
                    if _is_fresh(s, now):
                        fresh_batch.append(s)
                        st["series_queued"] = \
                            st.get("series_queued", 0) + 1
                    else:
                        st["series_excluded_stale"] += 1
                if page_broke_on_stale or len(serieslist) < 1000:
                    break
                offset += 1000
                if time.time() - t0 > BUDGET_S:
                    break
        except FredBlocked as e:
            st["blocked_at"] = str(e)
            st["blocked_ts"] = now.isoformat(timespec="seconds")
            break
        except Exception as e:
            st.setdefault("errors", {})[str(cid)] = str(e)[:80]
            continue
        # ops 4556: SEQUENTIAL, rate-limited fetch — the 16-thread
        # pool is exactly what triggered the 429->403 incident.
        drained = True
        for sm in fresh_batch:
            if time.time() - t0 > BUDGET_S:
                drained = False
                break
            try:
                obs = _fetch_observations(sm["id"])
            except FredBlocked as e:
                st["blocked_at"] = str(e)
                st["blocked_ts"] = now.isoformat(timespec="seconds")
                drained = False
                break  # stop THIS category, save, and stop the run
            except Exception as e:
                st.setdefault("errors", {})[sm.get("id", "?")] = \
                    str(e)[:60]
                st["series_queued"] = max(
                    0, st.get("series_queued", 0) - 1)
                continue
            if not obs:
                st["series_excluded_stale"] += 1
                st["series_queued"] = max(
                    0, st.get("series_queued", 0) - 1)
                continue
            buf.append({
                "id": sm.get("id"), "title": sm.get("title"),
                "freq": sm.get("frequency_short"),
                "units": sm.get("units_short"),
                "category": cname, "root": rname, "category_id": cid,
                "last_updated": sm.get("last_updated"),
                "last_obs": obs[-1]["date"],
                "last_value": obs[-1]["value"],
                "n_observations": len(obs),
                "source_url": ("https://fred.stlouisfed.org/series/"
                               + str(sm.get("id"))),
            })
            s3.put_object(
                Bucket=BUCKET,
                Key=(f"data/warm/fred-scoped/"
                     f"{rname.replace(' ', '_')}/{sm.get('id')}.json"),
                Body=json.dumps({"meta": sm, "observations": obs},
                               default=str).encode(),
                ContentType="application/json")
            st["series_imported"] += 1
            st["series_queued"] = max(0, st.get("series_queued", 0) - 1)
            if len(st["imported_ids"]) < 2000:
                st["imported_ids"].append(sm.get("id"))
            if len(buf) >= PAGE:
                s3.put_object(
                    Bucket=BUCKET,
                    Key=("data/providers/fred-scoped/series/"
                         f"page-{st['n_pages']:04d}.json"),
                    Body=json.dumps({"page": st["n_pages"],
                                    "rows": buf},
                                   default=str).encode(),
                    ContentType="application/json",
                    CacheControl="no-cache")
                st["n_pages"] += 1
                buf = []
        if drained:
            st["cats_done"].append(cid)
        if _blocked_this_invoke[0] or time.time() - t0 > BUDGET_S:
            break
    all_done = len(st["cats_done"]) >= len(expanded)
    if all_done and buf:
        # ops 4560 (Perplexity defect 1): never COMPLETE with an
        # unflushed buffer — flush the final partial page first.
        s3.put_object(
            Bucket=BUCKET,
            Key=("data/providers/fred-scoped/series/"
                 f"page-{st['n_pages']:04d}.json"),
            Body=json.dumps({"page": st["n_pages"], "rows": buf},
                           default=str).encode(),
            ContentType="application/json", CacheControl="no-cache")
        st["n_pages"] += 1
        buf = []
    st["buffer"] = buf
    st["updated_at"] = now.isoformat(timespec="seconds")
    # ops 4560 (Perplexity defect 2): strict accounting — every seen
    # series must land in exactly one bucket, or the run is NOT ok.
    n_err = len(st.get("errors") or {})
    base = st.get("imported_baseline", 0)
    accounted = ((st["series_imported"] - base)
                + st["series_excluded_stale"]
                + st.get("series_excluded_discontinued", 0)
                + st.get("series_skipped_already", 0)
                + st.get("series_queued", 0)  # ops 4566: pending fetch
                + n_err)
    st["accounting"] = {"seen": st["series_seen"],
                       "accounted": accounted,
                       "reconciles": accounted == st["series_seen"]}
    st["status"] = ("COMPLETE" if (all_done and not buf and
                                   st["accounting"]["reconciles"])
                    else ("COMPLETE_WITH_LEAKS" if all_done
                          else "walking"))
    st["lease_until"] = 0
    s3.put_object(Bucket=BUCKET, Key=state_key,
                  Body=json.dumps(st, default=str).encode(),
                  ContentType="application/json")
    manifest = {"updated_at": st["updated_at"],
               "categories": PRIORITY_ROOTS,
               "categories_done": len(st["cats_done"]),
               "categories_total": len(expanded),
               "series_seen": st["series_seen"],
               "series_excluded_stale": st["series_excluded_stale"],
               "series_excluded_discontinued":
                   st.get("series_excluded_discontinued", 0),
               "series_skipped_already":
                   st.get("series_skipped_already", 0),
               "accounting": st.get("accounting"),
               "n_categories_expanded":
                   st.get("n_categories_expanded"),
               "series_imported": st["series_imported"],
               "freshness_rule": f"last_updated within {FRESH_DAYS} "
                                 "days; older = excluded, never "
                                 "silently imported",
               "status": st["status"]}
    s3.put_object(Bucket=BUCKET,
                  Key="data/providers/fred-scoped/manifest.json",
                  Body=json.dumps(manifest, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    return manifest


def lambda_handler(event, context):
    t0 = time.time()
    phase = (event or {}).get("phase", "categories")
    if phase == "scoped_import":
        m = _run_scoped_import(t0, datetime.now(timezone.utc))
        print(json.dumps({k: m[k] for k in
                          ("categories_done", "series_seen",
                           "series_excluded_stale",
                           "series_imported", "status")}))
        return {"statusCode": 200, "body": json.dumps(m, default=str)}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if phase == "categories":
        # ---- Phase 1: recursive category-tree crawl ----
        try:
            st = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key="data/_state/fred-categories.json")["Body"].read())
        except Exception:
            st = {"visited": [], "frontier": [0], "cats": {}}
        visited = set(st["visited"])
        frontier = st["frontier"] or [0]
        while frontier and time.time() - t0 < BUDGET_S:
            cid = frontier.pop(0)
            if cid in visited:
                continue
            try:
                d = _get(
                    "https://api.stlouisfed.org/fred/category/children"
                    f"?category_id={cid}&api_key={FRED_KEY}"
                    "&file_type=json")
                kids = d.get("categories", [])
                for k in kids:
                    st["cats"][str(k["id"])] = {
                        "name": k.get("name"),
                        "parent_id": k.get("parent_id")}
                    if k["id"] not in visited:
                        frontier.append(k["id"])
            except Exception as e:
                st.setdefault("errors", {})[str(cid)] = str(e)[:80]
            visited.add(cid)
        st["visited"] = sorted(visited)
        st["frontier"] = frontier
        st["updated_at"] = now
        st["n_categories"] = len(st["cats"])
        st["status"] = "COMPLETE" if not frontier else "walking"
        s3.put_object(Bucket=BUCKET,
                      Key="data/_state/fred-categories.json",
                      Body=json.dumps(st, default=str).encode(),
                      ContentType="application/json")
        res = {"phase": "categories", "n_categories": st["n_categories"],
              "frontier_left": len(frontier), "status": st["status"]}
        print(json.dumps(res))
        return {"statusCode": 200, "body": json.dumps(res)}

    # ---- Phase 2: per-category series-list metadata banking ----
    try:
        cst = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_state/fred-categories.json")["Body"].read())
    except Exception:
        return {"statusCode": 400,
                "body": json.dumps({"error": "run phase=categories "
                                             "first"})}
    try:
        pst = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/_state/fred-series-meta.json")["Body"].read())
    except Exception:
        pst = {"cats_done": [], "n_series": 0, "n_pages": 0,
              "buffer": []}
    cat_ids = [int(c) for c in cst.get("cats", {}).keys()]
    todo = [c for c in cat_ids if c not in pst["cats_done"]]
    buf = pst.get("buffer", [])
    done_this_run = 0
    for cid in todo:
        if time.time() - t0 > BUDGET_S:
            break
        try:
            offset = 0
            while True:
                d = _get(
                    "https://api.stlouisfed.org/fred/category/series"
                    f"?category_id={cid}&api_key={FRED_KEY}"
                    f"&file_type=json&limit=1000&offset={offset}")
                serieslist = d.get("seriess", [])
                for s in serieslist:
                    buf.append({
                        "id": s.get("id"), "title": s.get("title"),
                        "freq": s.get("frequency_short"),
                        "units": s.get("units_short"),
                        "seasonal_adj": s.get("seasonal_adjustment_short"),
                        "popularity": s.get("popularity"),
                        "obs_start": s.get("observation_start"),
                        "obs_end": s.get("observation_end"),
                        "last_updated": s.get("last_updated"),
                        "category_id": cid,
                        "source_url": ("https://fred.stlouisfed.org/"
                                       "series/" + str(s.get("id"))),
                    })
                    if len(buf) >= PAGE:
                        s3.put_object(
                            Bucket=BUCKET,
                            Key=("data/warm/fred-catalog/series-meta/"
                                 f"page-{pst['n_pages']:04d}.json"),
                            Body=json.dumps({"page": pst["n_pages"],
                                            "count": len(buf),
                                            "rows": buf},
                                           default=str).encode(),
                            ContentType="application/json",
                            CacheControl="no-cache")
                        pst["n_pages"] += 1
                        pst["n_series"] += len(buf)
                        buf = []
                if len(serieslist) < 1000:
                    break
                offset += 1000
            pst["cats_done"].append(cid)
            done_this_run += 1
        except Exception as e:
            pst.setdefault("errors", {})[str(cid)] = str(e)[:80]
        if time.time() - t0 > BUDGET_S:
            break
    pst["buffer"] = buf
    pst["updated_at"] = now
    s3.put_object(Bucket=BUCKET, Key="data/_state/fred-series-meta.json",
                  Body=json.dumps(pst, default=str).encode(),
                  ContentType="application/json")
    manifest = {
        "provider": "fred", "updated_at": now,
        "categories_total": len(cat_ids),
        "categories_parsed": len(pst["cats_done"]),
        "series_discovered": pst["n_series"] + len(buf),
        "n_pages": pst["n_pages"], "page_size": PAGE,
        "note": ("Metadata only (id/title/freq/units/popularity/date "
                "range) — full observation histories are a separate, "
                "much larger phase, not yet started."),
    }
    s3.put_object(Bucket=BUCKET,
                  Key="data/providers/fred/catalog-manifest.json",
                  Body=json.dumps(manifest, default=str).encode(),
                  ContentType="application/json", CacheControl="no-cache")
    res = {"phase": "series_meta", "cats_processed_this_run":
          done_this_run, **manifest}
    print(json.dumps({k: res[k] for k in
                      ("cats_processed_this_run", "series_discovered",
                       "categories_parsed", "categories_total")}))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}
