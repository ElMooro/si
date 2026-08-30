"""justhodl-census-us v1.1.6 -- US Census Bureau timeseries walker, FULL universe. conquest-v116 ops4972

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
GRAM_KEY = "data/warm/census-us/_state/grammar-overrides.json"

# per-state iteration targets (override geo_iter="state"): 50 states+DC
STATE_FIPS = ["01", "02", "04", "05", "06", "08", "09", "10", "11",
              "12", "13", "15", "16", "17", "18", "19", "20", "21",
              "22", "23", "24", "25", "26", "27", "28", "29", "30",
              "31", "32", "33", "34", "35", "36", "37", "38", "39",
              "40", "41", "42", "44", "45", "46", "47", "48", "49",
              "50", "51", "53", "54", "55", "56"]

_OV = None


def _ov(slug):
    """Probe-verified grammar overrides (written by ops, never guessed):
    {slug: {vars:[...], tp, full_time, year_time}}. Empty when absent."""
    global _OV
    if _OV is None:
        _OV = gj(GRAM_KEY) or {}
    return _OV.get(slug) or {}
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

def get_vars(url, slug=None):
    """Returns (getlist, tp, has_year). tp is the time-predicate name
    the dataset itself declares -- 'time' or 'YEAR' -- never guessed.
    cell_value grid datasets keep the tight 8-var set; wide indicator
    datasets (BDS/QWI/SAIPE class) take up to 20 usable vars. Probe-
    verified overrides (GRAM_KEY) replace the var list / tp when the
    default grammar is rejected by the dataset (qwi-class)."""
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
    ov = _ov(slug) if slug else {}
    if ov.get("vars"):
        got = [v for v in ov["vars"] if v in avail]
        if got:
            return got, (ov.get("tp") or tp), has_year
    usable = [v for v in avail
              if v not in ("for", "in", "time", "us", "ucgid", "YEAR")
              and not (avail[v] or {}).get("predicateOnly")]
    if "cell_value" in avail:
        got = [v for v in PREFERRED_VARS if v in avail]
        if "cell_value" not in got:
            got = ["cell_value"] + [v for v in usable if v not in got][:6]
        return (got[:8] or None), tp, has_year
    return (usable[:20] or None), tp, has_year


def q(url, getlist, pred, geo, tp, extra=None):
    # conquest-v116: tp may be None (PSEO-class -- the time axis is a
    # regular wildcarded predicate, no time= param exists); extra =
    # fixed predicate wildcards (NAICS/PBA/CIPCODE...)
    parts = ["get=" + ",".join(getlist)]
    if tp is not None:
        parts.append(tp + "=" + urllib.parse.quote_plus(pred))
    if geo:
        parts.append("for=" + urllib.parse.quote_plus(geo))
    for _k, _v in (extra or {}).items():
        parts.append(urllib.parse.quote_plus(_k) + "=" +
                     urllib.parse.quote_plus(_v))
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
        ds["vars"], ds["tp"], ds["has_year"] = get_vars(url, slug)
        if not ds["vars"]:
            state["failures"][slug] = "variables.json unreadable"
            return True
    tp = ds.get("tp") or "time"
    ov = _ov(slug)
    if "tp" in ov:
        tp = ov["tp"]                     # may be None (PSEO-class)
    # conquest-v116 rung -1: probe-pinned single geo (us:* etc) with
    # extra predicate wildcards -- one full-window shot, verbatim
    if ov.get("geo"):
        ds["mode"] = "ov_geo"
        _u = q(url, ov.get("vars") or ds["vars"],
               (ov.get("full_time") or "from 1900").format(
                   cur=_now().year),
               ov["geo"], tp, extra=ov.get("extra"))
        st_, text = http_get(_u, timeout=120)
        rows = parse_rows(text) if st_ == 200 else None
        if rows and len(rows) > 1:
            ds["rows"] = bank(ROOT + slug + "/full.json.gz", rows)
            ds["seen_data"] = ds["ok"] = True
            y0, y1 = year_span(rows, tp or "YEAR")
            ds["y0"], ds["y1"] = y0, y1
            state["failures"].pop(slug, None)
            return True
        state["failures"][slug] = (
            "ov_geo shot refused (HTTP %s) %s" % (
                st_, (text or "")[:90]))
        return True
    t_full = (ov.get("full_time") or "from 1900").format(cur=_now().year)
    y_fmt = ov.get("year_time")

    c0 = _calls
    try:
        # rung 0: probe-verified per-geo iteration (qwi-class: the API
        # rejects for=state:* -- "select a specific state") -----------
        if ov.get("geo_iter") == "state":
            ds["mode"] = "geo_state"
            gr = ds.setdefault("geo_rows", {})
            i = ds.get("resume_geo") or 0
            while i < len(STATE_FIPS):
                if ctx.get_remaining_time_in_millis() < KEEP_MS or \
                        _calls >= MAX_CALLS:
                    ds["resume_geo"] = i
                    return False
                code = STATE_FIPS[i]
                # conquest-v116 ops4972: overrides may redirect the
                # per-state rung to a finer FOR-geo iterated within
                # each state (school district / institution), plus
                # fixed extra predicates (sector/NAICS wildcards)
                _fg = ov.get("for_geo")
                _geo = ("%s:*" % _fg) if _fg else ("state:" + code)
                _u = q(url, ds["vars"],
                       (ov.get("full_time_geo") or t_full), _geo, tp)
                if _fg:
                    _u += "&in=" + urllib.parse.quote_plus(
                        "state:" + code)
                for _k, _v in (ov.get("extra") or {}).items():
                    _u += "&%s=%s" % (
                        urllib.parse.quote_plus(_k),
                        urllib.parse.quote_plus(_v))
                st, text = http_get(_u, timeout=120)
                if st == 204:
                    gr[code] = 0          # legit-empty state (v116)
                    i += 1
                    continue
                rows = parse_rows(text) if st == 200 else None
                if rows:
                    n = bank(ROOT + slug + "/geo/%s.json.gz" % code,
                             rows)
                    gr[code] = n
                    ds["seen_data"] = True
                    y0, y1 = year_span(rows, tp)
                    if y0:
                        ds["y0"] = min(ds["y0"] or y0, y0)
                        ds["y1"] = max(ds["y1"] or y1, y1)
                i += 1
            ds["rows"] = sum(gr.values())
            ds["ok"] = ds["seen_data"]
            ds["resume_geo"] = None
            if not ds["ok"]:
                state["failures"][slug] = \
                    "geo_state: no state returned rows"
            return True

        # rungs 1-3: whole-history in one shot (time-grammar only) ------
        if tp == "time" and (ds["mode"] is None or
                             ds["mode"] in ("full", "full_for",
                                            "full_state")):
            for vi in range(3):
                mode = MODE_BY_VI[vi][0]
                if ds["mode"] not in (None, mode):
                    continue
                st, text = http_get(
                    q(url, ds["vars"], t_full, VARIANTS[vi], tp),
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
            y_arg = y_fmt.format(y=y) if y_fmt else str(y)
            st, text = http_get(q(url, ds["vars"], y_arg,
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
        ovr = _ov(slug)
        vi = VI_BY_MODE.get(ds.get("mode"), 0)
        if ds.get("mode") == "geo_state":
            gr = ds.setdefault("geo_rows", {})
            for code in STATE_FIPS:
                st, text = http_get(
                    q(url, ds["vars"],
                      (ovr.get("full_time") or "from 1900").format(cur=_now().year),
                      "state:" + code, tp), timeout=120)
                if st == 204:
                    gr[code] = 0          # legit-empty state (v116)
                    i += 1
                    continue
                rows = parse_rows(text) if st == 200 else None
                if rows:
                    gr[code] = bank(
                        ROOT + slug + "/geo/%s.json.gz" % code, rows)
                    y0, y1 = year_span(rows, tp)
                    if y1:
                        ds["y1"] = max(ds["y1"] or y1, y1)
            ds["rows"] = sum(gr.values())
        elif ds["mode"] in ("full", "full_for", "full_state"):
            st, text = http_get(q(url, ds["vars"],
                                  (ovr.get("full_time") or "from 1900").format(cur=_now().year),
                                  VARIANTS[vi], tp), timeout=120)
            rows = parse_rows(text) if st == 200 else None
            if rows:
                ds["rows"] = bank(ROOT + slug + "/full.json.gz", rows)
                y0, y1 = year_span(rows, tp)
                if y0:
                    ds["y0"], ds["y1"] = y0, y1
        else:
            for y in (cur, cur - 1):
                yf = ovr.get("year_time")
                st, text = http_get(q(url, ds["vars"],
                                      yf.format(y=y) if yf else str(y),
                                      VARIANTS[vi], tp))
                if st == 204:
                    gr[code] = 0          # legit-empty state (v116)
                    i += 1
                    continue
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



# ── ops 5060: economic-scope walker (macro/finance/industry/employment) ──
ECON_STATE = "data/_state/census-econ.json"
ECON_ROOT = "data/warm/census-econ/"
ECON_SCOPE = "data/_state/census-econ-scope.json"
ECON_BUDGET = 780          # < the 850s timeout, leaves room to checkpoint
VAR_CHUNK = 45             # Census caps get= near 50
GEO_CAP = 4
ECON_PRIORITY = ["cbp", "zbp", "nonemp", "ase", "rhfs", "cfspum"]
ECON_LAST = ["cps", "sipp"]   # biggest and most survey-shaped: queued last


def _econ_rank(fam):
    """Cheap-and-useful first, the two giants last. Nothing is dropped --
    ordering only, so an interrupted crawl has already banked the data
    the physical-economy and regional desks actually read."""
    if fam in ECON_PRIORITY:
        return (0, ECON_PRIORITY.index(fam))
    if fam in ECON_LAST:
        return (3, ECON_LAST.index(fam))
    if fam.startswith("ecn") or fam.startswith("ewks"):
        return (1, 0)
    return (2, 0)


def _econ_build_queue(state):
    """Turn the reviewed scope manifest + data.json into a work queue.
    Scope lives in S3 as DATA, so it can be edited without a deploy."""
    scope = gj(ECON_SCOPE) or {}
    fams = set((scope.get("families") or {}).keys())
    if not fams:
        state["failures"]["_scope"] = "census-econ-scope.json missing"
        return state
    st, txt = http_get(DATA_JSON)
    if st != 200:
        state["failures"]["_catalog"] = "data.json HTTP %s" % st
        return state
    q = []
    for d in (json.loads(txt).get("dataset") or []):
        cd = d.get("c_dataset") or []
        if not cd or cd[0] == "timeseries" or cd[0] not in fams:
            continue
        q.append({"fam": cd[0], "ds": "/".join(str(x) for x in cd),
                  "vintage": d.get("c_vintage"),
                  "title": str(d.get("title") or "")[:120]})
    q.sort(key=lambda e: (_econ_rank(e["fam"]), e["fam"],
                          -(int(e["vintage"]) if e["vintage"] else 0)))
    done = set(state.get("done") or [])
    state["queue"] = [e for e in q
                      if (e["ds"] + "@" + str(e["vintage"])) not in done]
    state["n_total"] = len(q)
    state["phase"] = "DRAIN" if state["queue"] else "COMPLETE"
    return state


def _econ_entry(e, state, t0, ctx):
    """One dataset-vintage: variables in chunks x geography levels."""
    base = ("https://api.census.gov/data/%s/%s" % (e["vintage"], e["ds"])
            if e.get("vintage") else
            "https://api.census.gov/data/%s" % e["ds"])
    st, txt = http_get(base + "/variables.json")
    if st != 200:
        return 0, "variables HTTP %s" % st
    try:
        allv = json.loads(txt).get("variables") or {}
    except Exception as ex:
        return 0, "variables parse %s" % str(ex)[:60]
    skip = {"for", "in", "ucgid", "time"}
    names = sorted(k for k in allv if k not in skip)
    # NAICS is a PREDICATE: without an explicit wildcard the API returns
    # all-industry totals only, which would silently drop the industrial
    # detail this lane exists for.
    naics = next((k for k in names
                  if k.upper().startswith("NAICS")
                  or k.upper() in ("SECTOR", "INDGROUP")), None)
    st, txt = http_get(base + "/geography.json")
    geos = []
    if st == 200:
        try:
            for g in (json.loads(txt).get("fips") or []):
                if not g.get("requires"):
                    geos.append(g.get("name"))
        except Exception:
            pass
    geos = (geos or ["us"])[:GEO_CAP]
    rows = 0
    for gi, geo in enumerate(geos):
        for ci in range(0, len(names), VAR_CHUNK):
            if time.time() - t0 > ECON_BUDGET:
                return rows, "BUDGET"
            chunk = names[ci:ci + VAR_CHUNK]
            q = "get=" + ",".join(chunk) + "&for=" + \
                urllib.parse.quote(geo) + ":*"
            if KEY:
                q += "&key=" + KEY
            url = base + "?" + q
            stt, body = http_get(url + ("&%s=*" % naics if naics else ""))
            if stt != 200 and naics:
                # the wildcard is not valid on every dataset; the totals
                # are still worth banking, so degrade rather than fail
                stt, body = http_get(url)
            if stt != 200 or not body.strip():
                continue
            try:
                data = json.loads(body)
            except Exception:
                continue
            if not isinstance(data, list) or len(data) < 2:
                continue
            key = "%s%s/%s/%s/g%d-c%d.json.gz" % (
                ECON_ROOT, e["fam"], e["ds"].replace("/", "_"),
                e.get("vintage") or "na", gi, ci // VAR_CHUNK)
            pj(key, data)
            rows += len(data) - 1
    return rows, None


def econ_run(ctx):
    t0 = time.time()
    state = gj(ECON_STATE) or {
        "version": "econ-v1", "phase": "CATALOG", "queue": [],
        "done": [], "failures": {}, "n_total": 0, "n_done": 0,
        "rows_total": 0,
        "note": "macro/finance/industry/employment scope only; "
                "demographics excluded by directive"}
    state.setdefault("failures", {})
    state.setdefault("done", [])
    if state.get("phase") in (None, "CATALOG") or not state.get("queue"):
        state = _econ_build_queue(state)
    drained = 0
    while state.get("queue"):
        if time.time() - t0 > ECON_BUDGET:
            break
        e = state["queue"][0]
        tag = e["ds"] + "@" + str(e.get("vintage"))
        att = int((state.get("attempts") or {}).get(tag, 0)) + 1
        state.setdefault("attempts", {})[tag] = att
        try:
            n, err = _econ_entry(e, state, t0, ctx)
        except Exception as ex:
            n, err = 0, "%s: %s" % (type(ex).__name__, str(ex)[:70])
        if err == "BUDGET":
            state["rows_total"] += n
            break
        if err and att < 3:
            # retried while it can still advance; retired on the third
            # failure so one bad vintage cannot block the queue behind it
            state["failures"][tag] = "%s (attempt %d)" % (err, att)
            state["queue"].append(state["queue"].pop(0))
            state["updated_at"] = _now().isoformat()
            pj(ECON_STATE, state)
            continue
        if err:
            state["failures"][tag] = "%s -- retired" % err
        state["queue"].pop(0)
        state["done"].append(tag)
        state["n_done"] = len(state["done"])
        state["rows_total"] += n
        drained += 1
        state["updated_at"] = _now().isoformat()
        pj(ECON_STATE, state)
    if not state.get("queue"):
        state["phase"] = "COMPLETE"
    state["queue_left"] = len(state.get("queue") or [])
    state["updated_at"] = _now().isoformat()
    pj(ECON_STATE, state)
    return {"mode": "econ", "phase": state["phase"],
            "n_done": state["n_done"], "n_total": state["n_total"],
            "queue_left": state["queue_left"],
            "rows_total": state["rows_total"],
            "drained_this_run": drained, "calls": _calls,
            "failures": len(state["failures"])}


def lambda_handler(event, ctx):
    global _calls, _OV
    _calls = 0
    _OV = None                       # re-read overrides each invoke
    event = event or {}
    if event.get("mode") == "econ":
        # separate state, separate prefix -- the timeseries lane is
        # untouched by this path
        return econ_run(ctx)
    state = gj(STATE_KEY) or {
        "version": "1.1.4", "phase": "CATALOG", "queue": [],
        "datasets": {}, "catalog": {}, "failures": {},
        "n_total": 0, "n_done": 0, "rows_total": 0,
        "n_timeseries_universe": 0,
        "note": "scope: full Census timeseries universe since inception "
                "(intltrade -> import-canary; idb value-gated out)"}
    state["version"] = "1.1.4"

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


ENGINE_VERSION = "justhodl-census-us v1.1.4 ops4951 bounded-range"
