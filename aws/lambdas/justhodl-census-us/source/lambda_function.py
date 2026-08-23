"""justhodl-census-us v1.0.0 -- US Census Bureau economic timeseries walker.

Khalid (2026-08-23): "add US Census Bureau to data.html and import all
the data from there they have a lot of factory orders, manufacturing
etc ... make sure you are importing all the historic data since
inception".

Scope, stated honestly: the Census cell API spans ~1,400 datasets
(ACS, decennial, CBP, economic census -- geographic cross-sections in
the multi-TB range that no timeseries platform mirrors cell-by-cell).
The Census content that belongs on THIS platform is the Economic
Indicators Time Series family (timeseries/eits/*): M3 factory orders,
advance durable goods, retail sales, housing starts + permits, new
home sales, construction spending, business + wholesale + retail
inventories, QFR, quarterly services, business formation, trade
balance headline, homeownership. v1.0 drains EVERY eits dataset in the
live data.json catalog, full history since each dataset's inception,
every category / data type / seasonal-adjustment cell the endpoint
serves. The full timeseries universe (intltrade country-level etc.) is
discovered and counted in state as deferred tier-2 -- a knob, not a
rewrite.

Mechanics (fleet conventions -- fred-catalog / sdmx-walker lineage):
  - phases CATALOG -> DRAIN -> COMPLETE; state checkpointed to S3
  - adaptive per-dataset pull ladder, winner memoized:
      full (?time=from+1900) -> full+for=us:* -> per-year descending
      (inception found after 4 consecutive empty years) -> per-year+for
  - reserved concurrency 1 (single flight), 15-min Scheduler heartbeat
  - after COMPLETE the same heartbeat refreshes: daily re-pull of the
    trailing window per dataset (captures revisions + new months) and
    a weekly data.json re-discovery that queues NEW datasets
  - every failure lands in state.failures with the reason -- never
    silently dropped (the 13F lesson, ops 4936-4940)

S3 layout under data/warm/census-us/:
  _state/state.json            walker state (sentinel + catalog read it)
  catalog.json.gz              filtered dataset catalog (target source)
  <slug>/full.json.gz          whole-history payload (full modes)
  <slug>/slices/<YYYY>.json.gz per-year payloads (year modes)
  <slug>/manifest.json         rows, mode, year span, calls, refreshed
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

PREFERRED_VARS = ["cell_value", "data_type_code", "category_code",
                  "seasonally_adj", "time_slot_id", "error_data",
                  "geo_level_code", "program_code"]

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
                "User-Agent": "justhodl-census-us/1.0 (raafouis@gmail.com)"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.status, r.read().decode("utf-8", "replace")
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


# ── discovery ─────────────────────────────────────────────────────────

def discover(state):
    """Pull data.json, filter the EITS family, bank the catalog, build
    the drain queue. Counts the full timeseries universe for the
    deferred-tier note."""
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
    eits, universe = [], 0
    for d in dsets:
        cd = d.get("c_dataset") or []
        if d.get("c_isTimeseries") is True:
            universe += 1
        if len(cd) >= 3 and cd[0] == "timeseries" and cd[1] == "eits":
            url = ""
            for dist in (d.get("distribution") or []):
                if dist.get("accessURL"):
                    url = dist["accessURL"].replace("http://", "https://")
                    break
            if not url:
                continue
            eits.append({"slug": "-".join(cd[2:]),
                         "title": (d.get("title") or "")[:140],
                         "url": url.rstrip("/"),
                         "modified": d.get("modified")})
    eits.sort(key=lambda x: x["slug"])
    seen = {d["slug"] for d in eits}
    new = [sl for sl in sorted(seen) if sl not in state["datasets"]]
    state["catalog"] = {d["slug"]: d for d in eits}
    state["n_total"] = len(eits)
    state["n_timeseries_universe"] = universe
    state["queue"] = [sl for sl in state.get("queue", []) if sl in seen] \
        + [sl for sl in new if sl not in state.get("queue", [])]
    state["last_catalog_check"] = _now().isoformat(timespec="seconds")
    pj(CATALOG_KEY, {"as_of": state["last_catalog_check"],
                     "n_total": len(eits),
                     "universe_timeseries": universe,
                     "datasets": eits})
    return True


# ── per-dataset drain ─────────────────────────────────────────────────

def get_vars(url):
    st, text = http_get(url + "/variables.json")
    if st != 200:
        return None
    try:
        avail = (json.loads(text).get("variables") or {})
    except Exception:
        return None
    usable = [v for v in avail
              if v not in ("for", "in", "time", "us", "ucgid")
              and not (avail[v] or {}).get("predicateOnly")]
    got = [v for v in PREFERRED_VARS if v in avail]
    if "cell_value" not in got:
        got = (["cell_value"] if "cell_value" in avail else []) + \
            [v for v in usable if v not in got][:6]
    return got[:8] or None


def q(url, getlist, timepred, with_for):
    parts = ["get=" + ",".join(getlist),
             "time=" + urllib.parse.quote_plus(timepred)]
    if with_for:
        parts.append("for=" + urllib.parse.quote_plus("us:*"))
    if KEY:
        parts.append("key=" + KEY)
    return url + "?" + "&".join(parts)


def bank(key, rows):
    pj(key, rows)
    return max(0, len(rows) - 1)   # minus header


def drain_one(slug, state, ctx):
    """Full-history pull with the mode ladder. Returns True when the
    dataset is DONE (banked or failed-with-reason), False when the time
    budget ran out mid-year-scan (resume next heartbeat)."""
    meta = state["catalog"].get(slug) or {}
    url = meta.get("url")
    ds = state["datasets"].setdefault(
        slug, {"mode": None, "rows": 0, "y0": None, "y1": None,
               "calls": 0, "ok": False, "resume_year": None,
               "seen_data": False, "empty_run": 0})
    if not url:
        state["failures"][slug] = "no accessURL in catalog"
        return True
    if ds.get("vars") is None:
        ds["vars"] = get_vars(url)
        if not ds["vars"]:
            state["failures"][slug] = "variables.json unreadable"
            return True

    c0 = _calls
    try:
        # rung 1+2: whole-history in one shot -----------------------------
        if ds["mode"] in (None, "full", "full_for"):
            for wf, mode in ((False, "full"), (True, "full_for")):
                if ds["mode"] not in (None, mode):
                    continue
                st, text = http_get(q(url, ds["vars"], "from 1900", wf),
                                    timeout=120)
                rows = parse_rows(text) if st == 200 else None
                if st == 200 and rows:
                    n = bank(ROOT + slug + "/full.json.gz", rows)
                    ds.update(mode=mode, rows=n, ok=True)
                    ys = sorted({str(r[-1])[:4] for r in rows[1:]
                                 if r and r[-1] and
                                 str(r[-1])[:4].isdigit()})
                    if ys:
                        ds["y0"], ds["y1"] = ys[0], ys[-1]
                    return True
                if st == 200 and rows == []:
                    continue          # syntax accepted, nothing matched
            ds["mode"] = "year"       # escalate to slicing

        # rung 3+4: per-year descending until inception -------------------
        y = ds.get("resume_year") or _now().year
        while y >= YEAR_FLOOR:
            if ctx.get_remaining_time_in_millis() < KEEP_MS or \
                    _calls >= MAX_CALLS:
                ds["resume_year"] = y
                return False
            wf = ds["mode"] == "year_for"
            st, text = http_get(q(url, ds["vars"], str(y), wf))
            rows = parse_rows(text) if st == 200 else None
            if rows:
                n = bank(ROOT + slug + "/slices/%d.json.gz" % y, rows)
                ds["rows"] += n
                ds["seen_data"] = True
                ds["empty_run"] = 0
                ds["y0"] = str(y)
                ds["y1"] = ds["y1"] or str(y)
            else:
                if not ds["seen_data"] and not ds.get("flipped") and \
                        y >= _now().year - 3 and ds["mode"] == "year":
                    ds["mode"] = "year_for"     # flip variant once
                    ds["flipped"] = True
                    continue
                if ds["seen_data"]:
                    ds["empty_run"] += 1
                    if ds["empty_run"] >= EMPTY_STOP:
                        break                   # inception found
                elif y <= _now().year - 12:
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
                "mode": ds["mode"], "rows": ds["rows"],
                "years": [ds["y0"], ds["y1"]], "vars": ds["vars"],
                "calls": ds["calls"],
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
        wf = ds["mode"] in ("full_for", "year_for")
        if ds["mode"] in ("full", "full_for"):
            st, text = http_get(q(url, ds["vars"], "from 1900", wf),
                                timeout=120)
            rows = parse_rows(text) if st == 200 else None
            if rows:
                ds["rows"] = bank(ROOT + slug + "/full.json.gz", rows)
                ys = [str(r[-1])[:4] for r in rows[1:]
                      if r and r[-1] and str(r[-1])[:4].isdigit()]
                if ys:
                    ds["y1"] = max(ys)
        else:
            for y in (cur, cur - 1):
                st, text = http_get(q(url, ds["vars"], str(y), wf))
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
    state["updated_at"] = _now().isoformat(timespec="seconds")
    slim = dict(state)
    slim["catalog"] = {k: {"url": v["url"], "title": v.get("title"),
                           "modified": v.get("modified")}
                       for k, v in state.get("catalog", {}).items()}
    pj(STATE_KEY, slim)


def lambda_handler(event, ctx):
    global _calls
    _calls = 0
    state = gj(STATE_KEY) or {
        "version": "1.0.0", "phase": "CATALOG", "queue": [],
        "datasets": {}, "catalog": {}, "failures": {},
        "n_total": 0, "n_done": 0, "rows_total": 0,
        "n_timeseries_universe": 0,
        "note": "scope: full EITS family since inception; wider "
                "timeseries universe deferred tier-2"}

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
            if drain_one(slug, state, ctx):
                state["queue"].pop(0)
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


ENGINE_VERSION = "justhodl-census-us v1.0.0 ops4944"
