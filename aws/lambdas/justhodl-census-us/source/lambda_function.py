"""justhodl-census-us v1.1.1 -- US Census Bureau timeseries walker, FULL universe.

v1.0.0 (ops 4944-45) drained the EITS family complete since inception
(21 datasets, 2.08M rows). Khalid (2026-08-23): "there is no way all of
it is just 11mb" -- correct instinct: EITS is the aggregate-indicator
family; Census's timeseries universe is ~94 datasets. v1.1.0 turns the
documented tier-2 knob: EVERY timeseries dataset in data.json is now in
scope -- BDS firm dynamics (1978->), ASM manufacturing detail, QWI
workforce indicators, SAIPE poverty, SAHIE health-insurance, PSEO and
the rest -- full history since each dataset's inception.

Deliberate exclusions (named, not silent):
  intltrade/*  customs detail -- lives in justhodl-import-canary
               (#1 canary, memory card 5); never duplicate.
  idb/*        international population projections to 2100 --
               demographic, no platform edge (value-gate, memory 26).
The multi-TB geographic cross-section API (ACS/decennial/CBP cell
grids) remains out of timeseries scope by design.

New mechanics on top of the proven v1.0 ladder:
  - per-dataset time grammar from variables.json: tp = "time" when the
    dataset speaks time predicates, else "YEAR" (BDS-style) -> the
    walker never guesses a grammar the source didn't declare
  - geo-variant chain "" -> us:* -> state:* in BOTH full and year modes
    (QWI-class datasets only answer with an explicit for=)
  - wide datasets without cell_value get up to 20 vars (BDS/QWI carry
    their payload across many named indicators, not a cell grid)
  - oversized full responses (>45MB text) escalate to year slicing
  - y0/y1 read from the HEADER time column, not r[-1] (v1.0 cosmetic
    bug: with for= variants the last column is the geo code -> "1..1")
  - event {"recatalog": true} forces immediate re-discovery + DRAIN
phases CATALOG -> DRAIN -> COMPLETE unchanged; state schema is a
superset of v1.0 (every v1.0 key still written -- G0_KEY_CONTRACT).

S3 layout under data/warm/census-us/ unchanged:
  _state/state.json  catalog.json.gz  <slug>/full.json.gz
  <slug>/slices/<YYYY>.json.gz  <slug>/manifest.json
Non-EITS slugs are family-prefixed ("bds", "qwi-sa", "asm-...") so the
21 live EITS slugs and their banked history are untouched.
"""
import gzip
import io
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
KEY = os.environ.get("CENSUS_API_KEY", "")
ROOT = "data/warm/census-us/"
STATE_KEY = ROOT + "_state/state.json"
CATALOG_KEY = ROOT + "catalog.json.gz"
DATA_JSON = "https://api.census.gov/data.json"
SPACING = float(os.environ.get("SPACING", "0.55"))
MAX_CALLS = int(os.environ.get("MAX_CALLS", "260"))
KEEP_MS = 90_000          # stop starting work under 90s remaining
CATALOG_REFRESH_D = 7     # weekly re-discovery of data.json
YEAR_FLOOR = 1900
EMPTY_STOP = 4            # consecutive empty years after data = inception
BIG_TEXT = 45_000_000     # full payload above this -> slice by year
READ_CAP = 48_000_000     # hard chunked-read cap -> synthetic 413
EXCLUDE_FAMILIES = {"intltrade", "idb"}

PREFERRED_VARS = ["cell_value", "data_type_code", "category_code",
                  "seasonally_adj", "time_slot_id", "error_data",
                  "geo_level_code", "program_code"]
VARIANTS = ["", "us:*", "state:*"]
MODE_BY_VI = {0: ("full", "year"), 1: ("full_for", "year_for"),
              2: ("full_state", "year_state")}
VI_BY_MODE = {"full": 0, "year": 0, "full_for": 1, "year_for": 1,
              "full_state": 2, "year_state": 2}

s3 = boto3.client("s3", region_name="us-east-1")
_calls = 0


def _now():
    return datetime.now(timezone.utc)


def gj(key, default=None):
    try:
        raw = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        if key.endswith(".gz"):
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)
    except Exception:
        return default


def pj(key, obj):
    body = json.dumps(obj, default=str).encode()
    kw = {"Bucket": BUCKET, "Key": key, "ContentType": "application/json",
          "CacheControl": "no-cache"}
    if key.endswith(".gz"):
        body = gzip.compress(body)
        kw["ContentEncoding"] = "gzip"
    s3.put_object(Body=body, **kw)


def http_get(url, timeout=45, tries=2):
    """GET with pacing + retry. Returns (status, text) -- never raises
    for HTTP errors; the caller classifies."""
    global _calls
    last = (0, "")
    for i in range(tries):
        _calls += 1
        time.sleep(SPACING)
        try:
            req = urllib.request.Request(url, headers={
                "User-Agent": "justhodl-census-us/1.1 (raafouis@gmail.com)"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                # chunked read with a hard cap: county-grained sets can
                # return multi-hundred-MB dumps on the bare geo variant;
                # an unbounded read() OOM-kills the 1GB Lambda before
                # BIG_TEXT is ever consulted (the 4947 crash-loop).
                # Oversize -> synthetic 413 the mode ladder treats as
                # "wrong variant, try the next rung".
                buf, got = [], 0
                while True:
                    chunk = r.read(4 * 1024 * 1024)
                    if not chunk:
                        break
                    got += len(chunk)
                    if got > READ_CAP:
                        return 413, ""
                    buf.append(chunk)
                return r.status, b"".join(buf).decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            try:
                last = (e.code, e.read().decode("utf-8", "replace")[:400])
            except Exception:
                last = (e.code, "")
            if e.code in (400, 404, 204):
                return last          # semantic -- do not retry
        except Exception as e:
            last = (0, str(e)[:200])
        time.sleep(1.2 * (i + 1))
    return last


def parse_rows(text):
    """Census returns a JSON array-of-arrays (header first). Empty
    responses arrive as '', '[]', or a 204. Returns rows or None on
    non-JSON (error page)."""
    t = (text or "").strip()
    if not t or t == "[]":
        return []
    if not t.startswith("["):
        return None
    try:
        rows = json.loads(t)
        return rows if isinstance(rows, list) else None
    except Exception:
        return None


def year_span(rows, tp):
    """y0/y1 from the HEADER-located time column (v1.1 fix -- r[-1] is
    the geo code under for= variants)."""
    if not rows or len(rows) < 2:
        return None, None
    hdr = rows[0]
    ti = -1
    for cand in (tp, "time", "YEAR"):
        if cand and cand in hdr:
            ti = hdr.index(cand)
            break
    ys = set()
    for r in rows[1:]:
        try:
            y = str(r[ti])[:4]
        except Exception:
            continue
        if y.isdigit() and 1900 <= int(y) <= 2100:
            ys.add(y)
    return (min(ys), max(ys)) if ys else (None, None)


# ── discovery ─────────────────────────────────────────────────────────

def discover(state):
    """Pull data.json, include EVERY timeseries dataset outside
    EXCLUDE_FAMILIES, bank the catalog, extend the drain queue. EITS
    slugs keep their v1.0 names; other families are family-prefixed."""
    st, text = http_get(DATA_JSON, timeout=90)
    if st != 200:
        state["failures"]["_catalog"] = "data.json HTTP %s" % st
        return False
    try:
        doc = json.loads(text)
        dsets = doc.get("dataset") or []
    except Exception as e:
        state["failures"]["_catalog"] = "data.json parse: %s" % str(e)[:120]
        return False
    inc, universe, excluded = [], 0, {}
    for d in dsets:
        cd = d.get("c_dataset") or []
        if d.get("c_isTimeseries") is not True:
            continue
        if not cd or cd[0] != "timeseries" or len(cd) < 2:
            continue
        universe += 1
        fam = cd[1]
        if fam in EXCLUDE_FAMILIES:
            excluded[fam] = excluded.get(fam, 0) + 1
            continue
        url = ""
        for dist in (d.get("distribution") or []):
            if dist.get("accessURL"):
                url = dist["accessURL"].replace("http://", "https://")
                break
        if not url:
            continue
        slug = "-".join(cd[2:]) if fam == "eits" and len(cd) >= 3 \
            else "-".join(cd[1:])
        inc.append({"slug": slug, "family": fam,
                    "title": (d.get("title") or "")[:140],
                    "url": url.rstrip("/"),
                    "modified": d.get("modified")})
    inc.sort(key=lambda x: (x["family"] != "eits", x["slug"]))
    seen = {d["slug"] for d in inc}
    new = [d["slug"] for d in inc if d["slug"] not in state["datasets"]]
    state["catalog"] = {d["slug"]: d for d in inc}
    state["n_total"] = len(inc)
    state["n_timeseries_universe"] = universe
    state["excluded_families"] = excluded
    state["queue"] = [sl for sl in state.get("queue", []) if sl in seen] \
        + [sl for sl in new if sl not in state.get("queue", [])]
    state["last_catalog_check"] = _now().isoformat(timespec="seconds")
    pj(CATALOG_KEY, {"as_of": state["last_catalog_check"],
                     "n_total": len(inc),
                     "universe_timeseries": universe,
                     "excluded_families": excluded,
                     "datasets": inc})
    return True


# ── per-dataset drain ─────────────────────────────────────────────────

def get_vars(url):
    """Returns (getlist, tp, has_year). tp is the time-predicate name
    the dataset itself declares -- 'time' or 'YEAR' -- never guessed.
    cell_value grid datasets keep the tight 8-var set; wide indicator
    datasets (BDS/QWI/SAIPE class) take up to 20 usable vars."""
    st, text = http_get(url + "/variables.json")
    if st != 200:
        return None, None, False
    try:
        avail = (json.loads(text).get("variables") or {})
    except Exception:
        return None, None, False
    tp = "time" if "time" in avail else ("YEAR" if "YEAR" in avail
                                         else None)
    has_year = "YEAR" in avail
    usable = [v for v in avail
              if v not in ("for", "in", "time", "us", "ucgid", "YEAR")
              and not (avail[v] or {}).get("predicateOnly")]
    if "cell_value" in avail:
        got = [v for v in PREFERRED_VARS if v in avail]
        if "cell_value" not in got:
            got = ["cell_value"] + [v for v in usable if v not in got][:6]
        return (got[:8] or None), tp, has_year
    return (usable[:20] or None), tp, has_year


def q(url, getlist, pred, geo, tp):
    parts = ["get=" + ",".join(getlist),
             (tp or "time") + "=" + urllib.parse.quote_plus(pred)]
    if geo:
        parts.append("for=" + urllib.parse.quote_plus(geo))
    if KEY:
        parts.append("key=" + KEY)
    return url + "?" + "&".join(parts)


def bank(key, rows):
    pj(key, rows)
    return max(0, len(rows) - 1)   # minus header


def _ds_default():
    return {"mode": None, "rows": 0, "y0": None, "y1": None,
            "calls": 0, "ok": False, "resume_year": None,
            "seen_data": False, "empty_run": 0}


def drain_one(slug, state, ctx):
    """Full-history pull with the mode ladder. Returns True when the
    dataset is DONE (banked or failed-with-reason), False when the time
    budget ran out mid-year-scan (resume next heartbeat)."""
    meta = state["catalog"].get(slug) or {}
    url = meta.get("url")
    ds = state["datasets"].setdefault(slug, _ds_default())
    if not url:
        state["failures"][slug] = "no accessURL in catalog"
        return True
    if ds.get("vars") is None:
        ds["vars"], ds["tp"], ds["has_year"] = get_vars(url)
        if not ds["vars"]:
            state["failures"][slug] = "variables.json unreadable"
            return True
    tp = ds.get("tp") or "time"

    c0 = _calls
    try:
        # rungs 1-3: whole-history in one shot (time-grammar only) ------
        if tp == "time" and (ds["mode"] is None or
                             ds["mode"] in ("full", "full_for",
                                            "full_state")):
            for vi in range(3):
                mode = MODE_BY_VI[vi][0]
                if ds["mode"] not in (None, mode):
                    continue
                st, text = http_get(
                    q(url, ds["vars"], "from 1900", VARIANTS[vi], tp),
                    timeout=120)
                if st == 200 and len(text) > BIG_TEXT:
                    break             # too big for one shot -> slice
                rows = parse_rows(text) if st == 200 else None
                if st == 200 and rows:
                    y0, y1 = year_span(rows, tp)
                    if ds.get("has_year") and y0 and y0 == y1 and \
                            not ds.get("tp_flipped"):
                        # annual set answered `time` with ONE year only
                        # (bds: time=from 1900 returns just the latest)
                        # -> its real history lives behind YEAR
                        ds.update(tp="YEAR", tp_flipped=True,
                                  mode="year", rows=0, ok=False,
                                  resume_year=None, seen_data=False,
                                  empty_run=0, y0=None, y1=None)
                        tp = "YEAR"
                        break             # fall into per-year rungs
                    n = bank(ROOT + slug + "/full.json.gz", rows)
                    ds.update(mode=mode, rows=n, ok=True)
                    if y0:
                        ds["y0"], ds["y1"] = y0, y1
                    return True
                # [] or 400 -> try next geo variant
            ds["mode"] = ds["mode"] if str(ds["mode"]).startswith("year") \
                else "year"           # escalate to slicing
        elif ds["mode"] is None:
            ds["mode"] = "year"       # YEAR-grammar: per-year only

        # rungs 4-6: per-year descending until inception ----------------
        vi = VI_BY_MODE.get(ds["mode"], 0)
        y = ds.get("resume_year") or _now().year
        while y >= YEAR_FLOOR:
            if ctx.get_remaining_time_in_millis() < KEEP_MS or \
                    _calls >= MAX_CALLS:
                ds["resume_year"] = y
                return False
            st, text = http_get(q(url, ds["vars"], str(y),
                                  VARIANTS[vi], tp))
            rows = parse_rows(text) if st == 200 else None
            if rows:
                n = bank(ROOT + slug + "/slices/%d.json.gz" % y, rows)
                ds["rows"] += n
                ds["seen_data"] = True
                ds["empty_run"] = 0
                ds["y0"] = str(y)
                ds["y1"] = ds["y1"] or str(y)
            else:
                if ds["seen_data"]:
                    ds["empty_run"] += 1
                    if ds["empty_run"] >= EMPTY_STOP:
                        break                   # inception found
                elif st == 413 or y <= _now().year - 12:
                    # variant exhausted (12-yr horizon) OR the slice is
                    # oversize even per-year -> advance the ladder now:
                    if tp == "time" and ds.get("has_year") and \
                            not ds.get("tp_flipped"):
                        ds["tp"] = tp = "YEAR"
                        ds["tp_flipped"] = True
                        y = _now().year
                        continue
                    flips = ds.get("flips", 0)
                    if flips < 2:
                        vi += 1
                        ds["flips"] = flips + 1
                        ds["mode"] = MODE_BY_VI[min(vi, 2)][1]
                        y = _now().year
                        continue
                    state["failures"][slug] = \
                        "no data any mode (last HTTP %s)" % st
                    return True
            y -= 1
        ds["ok"] = ds["seen_data"]
        ds["resume_year"] = None
        if not ds["ok"]:
            state["failures"][slug] = "year scan found no rows"
        return True
    finally:
        ds["calls"] += _calls - c0
        if ds.get("ok"):
            state["failures"].pop(slug, None)
            pj(ROOT + slug + "/manifest.json",
               {"dataset": slug, "title": meta.get("title"),
                "family": meta.get("family"),
                "mode": ds["mode"], "rows": ds["rows"],
                "years": [ds["y0"], ds["y1"]], "vars": ds["vars"],
                "tp": ds.get("tp"), "calls": ds["calls"],
                "updated_at": _now().isoformat(timespec="seconds")})


# ── refresh (post-COMPLETE heartbeats) ────────────────────────────────

def refresh(state, ctx):
    today = _now().date().isoformat()
    lc = state.get("last_catalog_check", "1970-01-01")
    if (str(_now().date()) > (lc[:10])) and \
            (_now().date() - datetime.fromisoformat(
                lc[:10]).date()).days >= CATALOG_REFRESH_D:
        discover(state)
        if state["queue"]:
            state["phase"] = "DRAIN"        # new datasets appeared
            return
    if state.get("last_refresh_date") == today:
        return
    cur = _now().year
    done = state.setdefault("_refresh_done", [])
    for slug, ds in sorted(state["datasets"].items()):
        if slug in done or not ds.get("ok"):
            continue
        if ctx.get_remaining_time_in_millis() < KEEP_MS or \
                _calls >= MAX_CALLS:
            return                          # resume next heartbeat
        url = (state["catalog"].get(slug) or {}).get("url")
        if not url:
            done.append(slug)
            continue
        tp = ds.get("tp") or "time"
        vi = VI_BY_MODE.get(ds.get("mode"), 0)
        if ds["mode"] in ("full", "full_for", "full_state"):
            st, text = http_get(q(url, ds["vars"], "from 1900",
                                  VARIANTS[vi], tp), timeout=120)
            rows = parse_rows(text) if st == 200 else None
            if rows:
                ds["rows"] = bank(ROOT + slug + "/full.json.gz", rows)
                y0, y1 = year_span(rows, tp)
                if y0:
                    ds["y0"], ds["y1"] = y0, y1
        else:
            for y in (cur, cur - 1):
                st, text = http_get(q(url, ds["vars"], str(y),
                                      VARIANTS[vi], tp))
                rows = parse_rows(text) if st == 200 else None
                if rows:
                    bank(ROOT + slug + "/slices/%d.json.gz" % y, rows)
                    ds["y1"] = str(max(int(ds["y1"] or y), y))
        ds["refreshed"] = today
        done.append(slug)
    state["last_refresh_date"] = today
    state["_refresh_done"] = []


# ── handler ───────────────────────────────────────────────────────────

def save(state):
    state["rows_total"] = sum(d.get("rows") or 0
                              for d in state["datasets"].values())
    state["n_done"] = sum(1 for d in state["datasets"].values()
                          if d.get("ok"))
    state["families"] = sorted({(v.get("family") or "?")
                                for v in state.get("catalog", {}).values()})
    state["updated_at"] = _now().isoformat(timespec="seconds")
    slim = dict(state)
    slim["catalog"] = {k: {"url": v["url"], "title": v.get("title"),
                           "family": v.get("family"),
                           "modified": v.get("modified")}
                       for k, v in state.get("catalog", {}).items()}
    pj(STATE_KEY, slim)


def lambda_handler(event, ctx):
    global _calls
    _calls = 0
    event = event or {}
    state = gj(STATE_KEY) or {
        "version": "1.1.1", "phase": "CATALOG", "queue": [],
        "datasets": {}, "catalog": {}, "failures": {},
        "n_total": 0, "n_done": 0, "rows_total": 0,
        "n_timeseries_universe": 0,
        "note": "scope: full Census timeseries universe since inception "
                "(intltrade -> import-canary; idb value-gated out)"}
    state["version"] = "1.1.1"

    if event.get("recatalog"):
        if discover(state) and state["queue"]:
            state["phase"] = "DRAIN"
        save(state)

    for slug in (event.get("redo") or []):
        # surgical re-import: reset one dataset and put it at the
        # front of the queue (used to repair wrong-grammar banks)
        if slug in state.get("catalog", {}):
            state["datasets"][slug] = _ds_default()
            state["failures"].pop(slug, None)
            if slug in state["queue"]:
                state["queue"].remove(slug)
            state["queue"].insert(0, slug)
            state["phase"] = "DRAIN"
            save(state)

    if state["phase"] == "CATALOG" or not state.get("catalog"):
        if discover(state):
            state["phase"] = "DRAIN"
        save(state)
        if state["phase"] == "CATALOG":
            return {"phase": "CATALOG", "error": "discovery failed",
                    "failures": state["failures"]}

    if state["phase"] == "DRAIN":
        while state["queue"]:
            if ctx.get_remaining_time_in_millis() < KEEP_MS or \
                    _calls >= MAX_CALLS:
                break
            slug = state["queue"][0]
            ds0 = state["datasets"].setdefault(slug, _ds_default())
            att = ds0.get("attempts", 0) + 1
            ds0["attempts"] = att
            if att > 4:
                # 4 invokes died at this head without completing it ->
                # OOM-class poison; quarantine, never starve the queue
                state["failures"][slug] = (
                    "quarantined: %d invoke attempts died without "
                    "completing (OOM-class)" % (att - 1))
                state["queue"].pop(0)
                save(state)
                continue
            prev_ry = ds0.get("resume_year")
            save(state)          # black-box: attempt survives an OOM
            try:
                done = drain_one(slug, state, ctx)
            except MemoryError:
                state["failures"][slug] = "MemoryError during pull"
                done = True
            except Exception as e:
                state["failures"][slug] = "crash: %s" % str(e)[:160]
                done = True
            if done:
                ds0.pop("attempts", None)
                state["queue"].pop(0)
            elif ds0.get("resume_year") != prev_ry:
                ds0["attempts"] = 1        # budget resume = progress
            save(state)
        if not state["queue"]:
            state["phase"] = "COMPLETE"

    if state["phase"] == "COMPLETE":
        refresh(state, ctx)

    save(state)
    return {"phase": state["phase"], "n_done": state["n_done"],
            "n_total": state["n_total"],
            "rows_total": state["rows_total"], "calls": _calls,
            "queue_left": len(state["queue"]),
            "failures": len(state["failures"])}


ENGINE_VERSION = "justhodl-census-us v1.1.1 ops4948 oom-guard"
