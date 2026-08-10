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
import gzip
import json
import os
import time
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
# ==== ops 4572: priority drain v2 ====
# Khalid's directives: (1) drain by FRED popularity so the highest-
# signal series land first and the tail is cuttable anytime; (2) go as
# fast as possible WITHOUT hitting the rate limit. Design:
#   - key is SSM-first (/justhodl/fred-api-key) — the old hardcoded
#     default lived in the PUBLIC repo and fed stranger 429s. No key
#     literal exists in this file anymore.
#   - DISCOVERY builds a one-time popularity-sorted queue ledger
#     (data/_state/fred-queue.json.gz). The old model re-paged giant
#     categories from offset 0 every 180s invoke, burning most of the
#     FRED budget on re-listing — discovery now consumes each category
#     list exactly once.
#   - DRAIN walks the ledger by popularity desc via a cursor; every
#     FRED call in this phase is a real observations fetch.
#   - Adaptive AIMD pacing: start 70/min, +6/min after each clean
#     120-call window, x0.6 (floor 24) + 20s cool on any 429, ceiling
#     from SSM /justhodl/fred/rate-ceiling (default 100; FRED's
#     documented cap is 120).
#   - Self-chain: while walking and making progress, the invoke
#     re-invokes itself async at budget end — ~100% duty cycle. The
#     5-min cron becomes the watchdog. The ETag lease + reserved
#     concurrency (set by ops) keep single-flight guaranteed.
#   - Live knobs, zero redeploys: /justhodl/fred/paused=1 halts;
#     /justhodl/fred/min-popularity=N cuts the tail at popularity < N.
_ssm = boto3.client("ssm", region_name="us-east-1")
_lambda = boto3.client("lambda", region_name="us-east-1")


def _fred_key():
    try:
        v = _ssm.get_parameter(Name="/justhodl/fred-api-key",
                               WithDecryption=True
                               )["Parameter"]["Value"]
        if v and len(v) >= 16 and v != "PLACEHOLDER":
            return v
    except Exception:
        pass
    try:  # String-type mirror — no KMS dependency
        v = _ssm.get_parameter(Name="/justhodl/fred/api-key"
                               )["Parameter"]["Value"]
        if v and len(v) >= 16 and v != "PLACEHOLDER":
            return v
    except Exception:
        pass
    return os.environ.get("FRED_API_KEY", "")


FRED_KEY = _fred_key()


def _knob(name, default):
    try:
        return _ssm.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception:
        return default


BUDGET_S = 780  # timeout 850 in config; chain covers the rest
PAGE = 500

_rate = {"rpm": 70.0, "floor": 24.0, "ceiling": 100.0,
         "clean": 0, "throttled": 0}
_last_call = [0.0]
_blocked_this_invoke = [False]
_deadline = [None]        # ops 4575: hard wall — no sleeps past this
_last_ckpt = [0.0]


class FredBlocked(Exception):
    pass


class FredKeyInvalid(Exception):
    """HTTP 400 naming api_key — the key is dead/rotated. Halt all FRED
    calls this invoke; never burn the rate budget on a dead key."""
    pass


class BudgetExhausted(Exception):
    """ops 4575 (v2 crash): retry backoffs used to sleep minutes past
    BUDGET_S, blowing through the 850s hard timeout mid-drain — state
    unsaved, lease wedged, chain dead. Raised instead of sleeping."""
    pass


def _budget_left():
    return (None if _deadline[0] is None
            else _deadline[0] - time.time())


def _sleep_within_budget(want):
    left = _budget_left()
    if left is not None and left <= 1.0:
        raise BudgetExhausted("budget wall at sleep(%.1fs)" % want)
    time.sleep(want if left is None else min(want, max(0.0, left - 1.0)))


def _get(url, tries=4):
    if _blocked_this_invoke[0]:
        raise FredBlocked("403 seen this invoke — halting FRED calls")
    left = _budget_left()
    if left is not None and left <= 2.0:
        raise BudgetExhausted("budget wall before request")
    wait = (60.0 / max(_rate["rpm"], 1.0)) - (time.time()
                                              - _last_call[0])
    if wait > 0:
        _sleep_within_budget(wait)
    backoff = 5.0
    for i in range(tries):
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent":
                              "JustHodl research admin@justhodl.ai"})
            with urllib.request.urlopen(req, timeout=25) as r:
                _last_call[0] = time.time()
                _rate["clean"] += 1
                if _rate["clean"] >= 120:
                    _rate["rpm"] = min(_rate["ceiling"],
                                       _rate["rpm"] + 6.0)
                    _rate["clean"] = 0
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            _last_call[0] = time.time()
            if e.code == 403:
                _blocked_this_invoke[0] = True
                raise FredBlocked(f"403 on {url[:80]}") from e
            if e.code == 400:
                try:
                    _body = e.read().decode("utf-8", "replace")[:300]
                except Exception:
                    _body = ""
                if "api_key" in _body.lower():
                    _blocked_this_invoke[0] = True
                    raise FredKeyInvalid(_body[:150]) from e
                raise
            if e.code == 429 and i < tries - 1:
                _rate["rpm"] = max(_rate["floor"],
                                   _rate["rpm"] * 0.6)
                _rate["clean"] = 0
                _rate["throttled"] += 1
                if _rate["throttled"] >= 12:
                    # sustained 429 storm — stop for THIS invoke, chain
                    # restarts at the floored, persisted rpm
                    raise BudgetExhausted(
                        "429 storm: %d throttles this invoke"
                        % _rate["throttled"])
                _sleep_within_budget(max(20.0, min(backoff, 60)))
                backoff *= 2
                continue
            raise
        except (FredBlocked, FredKeyInvalid, BudgetExhausted):
            raise
        except Exception:
            if i == tries - 1:
                raise
            _sleep_within_budget(1.5)


# ops 4553 (Khalid: exact category IDs, Money/Banking/Finance) — scoped,
# freshness-filtered import. Only series with last_updated within
# FRESH_DAYS get their real observations pulled; everything older is
# explicitly excluded and logged, never silently dropped.
PRIORITY_ROOTS = {
    22: "Interest Rates", 15: "Exchange Rates", 24: "Monetary Data",
    46: "Financial Indicators", 23: "Banking",
    32360: "Business Lending", 32145: "Foreign Exchange Intervention"}
FRESH_DAYS = 90
QUEUE_KEY = "data/_state/fred-queue.json.gz"


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
            cname = (cats.get(str(cid), {}) or {}).get("name") \
                if cid != root else rname
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


def _load_queue():
    try:
        b = s3.get_object(Bucket=BUCKET, Key=QUEUE_KEY)["Body"].read()
        return json.loads(gzip.decompress(b))
    except Exception:
        return {"rows": [], "cursor": 0, "built_at": None,
                "seen_ids": []}


def _save_queue(q):
    s3.put_object(Bucket=BUCKET, Key=QUEUE_KEY,
                  Body=gzip.compress(json.dumps(
                      q, default=str).encode(), 6),
                  ContentType="application/json",
                  ContentEncoding="gzip")


def _flush_page(st, buf):
    s3.put_object(
        Bucket=BUCKET,
        Key=("data/providers/fred-scoped/series/"
             f"page-{st['n_pages']:04d}.json"),
        Body=json.dumps({"page": st["n_pages"], "rows": buf},
                        default=str).encode(),
        ContentType="application/json", CacheControl="no-cache")
    st["n_pages"] += 1


def _run_scoped_import(t0, now):
    state_key = "data/_state/fred-scoped-import.json"
    if _knob("/justhodl/fred/paused", "0") == "1":
        return {"skipped": "paused", "categories_done": 0,
                "series_seen": 0, "series_excluded_stale": 0,
                "series_imported": 0, "status": "paused"}
    if not FRED_KEY:
        return {"skipped": "no_key", "categories_done": 0,
                "series_seen": 0, "series_excluded_stale": 0,
                "series_imported": 0, "status": "no_key"}
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
    # ops 4575: a chain that meets a lease in its final seconds used to
    # die silently — now it waits out a short tail and takes over, so
    # the walk survives its predecessor's crash window.
    _lu = st.get("lease_until") or 0
    if _lu > time.time():
        rem = _lu - time.time()
        if rem > 150:
            return {"skipped": "lease_held", "lease_until": _lu}
        time.sleep(rem + 3)
        try:
            _o = s3.get_object(Bucket=BUCKET, Key=state_key)
            st = json.loads(_o["Body"].read())
            _etag = _o.get("ETag")
        except Exception:
            pass
        if (st.get("lease_until") or 0) > time.time():
            return {"skipped": "lease_held",
                    "lease_until": st.get("lease_until")}
    _deadline[0] = t0 + BUDGET_S + 55   # hard wall < 850s config timeout
    _last_ckpt[0] = time.time()
    try:
        _rate["ceiling"] = max(24.0, float(
            _knob("/justhodl/fred/rate-ceiling", "100") or 100))
    except Exception:
        pass
    # ops 4575: rpm persists across invokes — a cold start used to reset
    # to 70 right after a 429 storm floored it, re-storming every chain.
    try:
        _rate["rpm"] = min(_rate["ceiling"], max(
            _rate["floor"], float(st.get("rate_rpm") or 70.0)))
    except Exception:
        pass
    # ops 4575: one cheap preflight — a rotated/dead key means every call
    # is a 400; never take the lease or burn rate budget on a dead key.
    try:
        _get("https://api.stlouisfed.org/fred/category"
             f"?category_id=125&api_key={FRED_KEY}&file_type=json")
    except FredKeyInvalid as e:
        st["status"] = "KEY_INVALID"
        st["key_error"] = str(e)[:150]
        st["key_checked_at"] = now.isoformat(timespec="seconds")
        st["lease_until"] = 0
        try:
            s3.put_object(Bucket=BUCKET, Key=state_key,
                          Body=json.dumps(st, default=str).encode(),
                          ContentType="application/json")
        except Exception:
            pass
        return {"skipped": "key_invalid", "status": "KEY_INVALID",
                "key_error": str(e)[:150]}
    except Exception:
        pass  # transient — the real calls will surface it
    st["lease_until"] = time.time() + BUDGET_S + 90
    try:
        _kw = {"Bucket": BUCKET, "Key": state_key,
               "Body": json.dumps(st, default=str).encode(),
               "ContentType": "application/json"}
        if _etag:
            _kw["IfMatch"] = _etag
        s3.put_object(**_kw)
    except Exception:
        return {"skipped": "lost_lease_race"}
    # ── ops 4575 (v2 crash, F4): everything past lease acquisition used
    # to be unprotected — one exception wedged the lease ~14.5 min and
    # vaporized all counters. Now ANY crash releases the lease, records
    # itself into st["errors"], and persists state before re-raising.
    try:
        expanded = _expand_roots()
        st["n_categories_expanded"] = len(expanded)
        cutoff = (now - __import__("datetime").timedelta(
            days=FRESH_DAYS)).strftime("%Y-%m-%d")
        q = _load_queue()
        seen_ids = set(q.get("seen_ids") or [])
        progress = 0

        # ---- DISCOVERY: consume each category list exactly once ----
        todo_cats = [c for c in expanded if c not in st["cats_done"]]
        for cid in todo_cats:
            if time.time() - t0 > BUDGET_S or _blocked_this_invoke[0]:
                break
            rname, cname = expanded[cid]
            offset = 0
            cat_ok = True
            try:
                while True:
                    d = _get(
                        "https://api.stlouisfed.org/fred/category/series"
                        f"?category_id={cid}&api_key={FRED_KEY}"
                        "&file_type=json&limit=1000"
                        "&order_by=last_updated&sort_order=desc"
                        f"&offset={offset}")
                    serieslist = d.get("seriess", [])
                    stale_break = False
                    for s in serieslist:
                        st["series_seen"] += 1
                        lu = (s.get("last_updated") or "")[:10]
                        if lu and lu < cutoff:
                            st["series_excluded_stale"] += 1
                            stale_break = True
                            break
                        title = (s.get("title") or "").upper()
                        if "DISCONTINUED" in title:
                            st["series_excluded_discontinued"] = \
                                st.get("series_excluded_discontinued",
                                       0) + 1
                            continue
                        sid = s.get("id")
                        if not sid or sid in seen_ids:
                            st["series_skipped_already"] = \
                                st.get("series_skipped_already", 0) + 1
                            continue
                        if _is_fresh(s, now):
                            seen_ids.add(sid)
                            q["rows"].append(
                                [sid, int(s.get("popularity") or 0),
                                 rname, cname,
                                 (s.get("last_updated") or "")[:10]])
                            st["series_queued"] = \
                                st.get("series_queued", 0) + 1
                        else:
                            st["series_excluded_stale"] += 1
                    if stale_break or len(serieslist) < 1000:
                        break
                    offset += 1000
                    if time.time() - t0 > BUDGET_S:
                        cat_ok = False
                        break
            except FredBlocked as e:
                st["blocked_at"] = str(e)
                st["blocked_ts"] = now.isoformat(timespec="seconds")
                cat_ok = False
            except FredKeyInvalid as e:
                st["status"] = "KEY_INVALID"
                st["key_error"] = str(e)[:150]
                cat_ok = False
            except BudgetExhausted:
                cat_ok = False   # half-listed ≠ done — resume next invoke
            except Exception as e:
                _errs = st.setdefault("errors", {})
                if len(_errs) < 500:
                    _errs[str(cid)] = str(e)[:80]
                cat_ok = True  # category listed as far as possible
            if cat_ok and not _blocked_this_invoke[0] and \
                    time.time() - t0 <= BUDGET_S:
                st["cats_done"].append(cid)
                progress += 1
            else:
                break
            if time.time() - _last_ckpt[0] > 90:
                st["lease_until"] = time.time() + BUDGET_S + 90
                st["phase2"] = "discovery"
                st["rate_rpm"] = round(_rate["rpm"], 1)
                st["updated_at"] = now.isoformat(timespec="seconds")
                try:
                    s3.put_object(Bucket=BUCKET, Key=state_key,
                                  Body=json.dumps(st, default=str).encode(),
                                  ContentType="application/json")
                except Exception:
                    pass
                _last_ckpt[0] = time.time()
        discovery_complete = len(st["cats_done"]) >= len(expanded)
        if discovery_complete and not q.get("sorted"):
            q["rows"].sort(key=lambda r2: -r2[1])
            q["sorted"] = True
            q["built_at"] = now.isoformat(timespec="seconds")
        q["seen_ids"] = sorted(seen_ids)

        # ---- DRAIN: popularity-desc via cursor ----
        buf = st.get("buffer", [])
        cur = int(q.get("cursor") or 0)
        rows = q.get("rows") or []
        try:
            min_pop = int(_knob("/justhodl/fred/min-popularity", "-1"))
        except Exception:
            min_pop = -1
        cut_hit = False
        if discovery_complete and q.get("sorted"):
            while cur < len(rows):
                if time.time() - t0 > BUDGET_S or \
                        _blocked_this_invoke[0]:
                    break
                sid, pop, rname, cname, lu = rows[cur]
                if min_pop >= 0 and pop < min_pop:
                    cut_hit = True
                    break
                _dk = ("data/warm/fred-scoped/"
                       f"{rname.replace(' ', '_')}/{sid}.json")
                try:
                    _h = s3.head_object(Bucket=BUCKET, Key=_dk)
                    if ((now - _h["LastModified"])
                            .total_seconds() / 86400) < 7:
                        st["series_skipped_already"] = \
                            st.get("series_skipped_already", 0) + 1
                        st["series_queued"] = max(
                            0, st.get("series_queued", 0) - 1)
                        cur += 1
                        continue
                except Exception:
                    pass
                try:
                    obs = _fetch_observations(sid)
                except FredBlocked as e:
                    st["blocked_at"] = str(e)
                    st["blocked_ts"] = now.isoformat(timespec="seconds")
                    break
                except (FredKeyInvalid, BudgetExhausted) as e:
                    if isinstance(e, FredKeyInvalid):
                        st["status"] = "KEY_INVALID"
                        st["key_error"] = str(e)[:150]
                    break        # stop the drain — do NOT cursor past series
                except Exception as e:
                    _errs = st.setdefault("errors", {})
                    if len(_errs) < 500:
                        _errs[sid] = str(e)[:60]
                    st["series_queued"] = max(
                        0, st.get("series_queued", 0) - 1)
                    cur += 1
                    continue
                if not obs:
                    st["series_excluded_stale"] += 1
                    st["series_queued"] = max(
                        0, st.get("series_queued", 0) - 1)
                    cur += 1
                    continue
                buf.append({
                    "id": sid, "title": None, "freq": None,
                    "units": None, "popularity": pop,
                    "category": cname, "root": rname,
                    "category_id": None, "last_updated": lu,
                    "last_obs": obs[-1]["date"],
                    "last_value": obs[-1]["value"],
                    "n_observations": len(obs),
                    "source_url": ("https://fred.stlouisfed.org/series/"
                                   + str(sid)),
                })
                s3.put_object(
                    Bucket=BUCKET, Key=_dk,
                    Body=json.dumps({"meta": {"id": sid,
                                              "popularity": pop,
                                              "category": cname,
                                              "root": rname,
                                              "last_updated": lu},
                                     "observations": obs},
                                    default=str).encode(),
                    ContentType="application/json")
                st["series_imported"] += 1
                st["last_pop_drained"] = pop
                st["series_queued"] = max(
                    0, st.get("series_queued", 0) - 1)
                if len(st["imported_ids"]) < 2000:
                    st["imported_ids"].append(sid)
                progress += 1
                cur += 1
                if len(buf) >= PAGE:
                    _flush_page(st, buf)
                    buf = []
                if time.time() - _last_ckpt[0] > 90:
                    st["lease_until"] = time.time() + BUDGET_S + 90
                    st["phase2"] = "drain"
                    st["queue_cursor"] = cur
                    st["rate_rpm"] = round(_rate["rpm"], 1)
                    st["buffer"] = buf
                    st["updated_at"] = now.isoformat(timespec="seconds")
                    q["cursor"] = cur
                    try:
                        s3.put_object(Bucket=BUCKET, Key=state_key,
                                      Body=json.dumps(st, default=str).encode(),
                                      ContentType="application/json")
                        _save_queue(q)
                    except Exception:
                        pass
                    _last_ckpt[0] = time.time()
        q["cursor"] = cur
        _save_queue(q)
        all_done = discovery_complete and (
            cur >= len(rows) or cut_hit)
        if all_done and buf:
            _flush_page(st, buf)
            buf = []
        st["buffer"] = buf
        st["updated_at"] = now.isoformat(timespec="seconds")
        st["engine_version"] = "2.1"
        st["phase2"] = ("drain" if discovery_complete else "discovery")
        st["queue_total"] = len(rows)
        st["queue_cursor"] = cur
        st["rate_rpm"] = round(_rate["rpm"], 1)
        st["throttled_429"] = st.get("throttled_429", 0) + \
            _rate["throttled"]
        st["min_popularity_cut"] = min_pop if min_pop >= 0 else None
        if rows and cur < len(rows):
            st["next_popularity"] = rows[cur][1]
        n_err = len(st.get("errors") or {})
        base = st.get("imported_baseline", 0)
        accounted = ((st["series_imported"] - base)
                     + st["series_excluded_stale"]
                     + st.get("series_excluded_discontinued", 0)
                     + st.get("series_skipped_already", 0)
                     + st.get("series_queued", 0)
                     + n_err)
        st["accounting"] = {"seen": st["series_seen"],
                            "accounted": accounted,
                            "reconciles":
                                accounted == st["series_seen"]}
        st["status"] = (("COMPLETE_CUT" if cut_hit else "COMPLETE")
                        if all_done and st["accounting"]["reconciles"]
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
                    "phase2": st["phase2"],
                    "queue_total": st["queue_total"],
                    "queue_cursor": st["queue_cursor"],
                    "rate_rpm": st["rate_rpm"],
                    "next_popularity": st.get("next_popularity"),
                    "min_popularity_cut": st.get("min_popularity_cut"),
                    "freshness_rule": f"last_updated within {FRESH_DAYS} "
                                      "days; older = excluded, never "
                                      "silently imported",
                    "priority_rule": "drain order = FRED popularity desc"
                                     " (priority drain v2.1, ops 4575)",
                    "engine_version": "2.1",
                    "status": st["status"]}
        s3.put_object(Bucket=BUCKET,
                      Key="data/providers/fred-scoped/manifest.json",
                      Body=json.dumps(manifest, default=str).encode(),
                      ContentType="application/json",
                      CacheControl="no-cache")
        # ---- SELF-CHAIN: ~100% duty cycle while there is work ----
        if (st["status"] == "walking" and progress > 0
                and not _blocked_this_invoke[0]):
            try:
                _lambda.invoke(
                    FunctionName=os.environ.get(
                        "AWS_LAMBDA_FUNCTION_NAME",
                        "justhodl-fred-catalog"),
                    InvocationType="Event",
                    Payload=json.dumps(
                        {"phase": "scoped_import",
                         "chain": True}).encode())
                manifest["chained"] = True
            except Exception:
                manifest["chained"] = False
        return manifest
    except BaseException as e:
        st["lease_until"] = 0
        st["updated_at"] = datetime.now(timezone.utc).isoformat(
            timespec="seconds")
        _errs = st.setdefault("errors", {})
        if len(_errs) < 500:
            _errs["_crash_" + type(e).__name__] = str(e)[:150]
        if isinstance(e, FredKeyInvalid):
            st["status"] = "KEY_INVALID"
            st["key_error"] = str(e)[:150]
        try:
            s3.put_object(Bucket=BUCKET, Key=state_key,
                          Body=json.dumps(st, default=str).encode(),
                          ContentType="application/json")
        except Exception:
            pass
        if isinstance(e, FredKeyInvalid):
            return {"skipped": "key_invalid", "status": "KEY_INVALID",
                    "key_error": str(e)[:150]}
        raise


def lambda_handler(event, context):
    t0 = time.time()
    # ops 4575 (v2 crash, F1): the EventBridge cron sends no payload, so
    # the old default ran the phase-1 category crawl (COMPLETE since
    # 4553) every 5 minutes while the v2 scoped import never ran on
    # schedule at all. The work IS the scoped import — default to it;
    # phase-1 stays reachable explicitly.
    phase = (event or {}).get("phase", "scoped_import")
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
