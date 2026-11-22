"""Three sources the repo board reads itself, each with a parser of its own (2026-11-22).

Khalid, 2026-11-21 22:44 ET, on the Repo desk's open items: "do all these one by one".

  1. A daily repo rate before August 2014.  The New York Fed published one on 2018-03-09: the volume-weighted mean
     rate of primary dealers' overnight Treasury general-collateral repo borrowing, from the Desk's morning survey,
     daily from 1998-02-20 to 2018-02-28 (4,998 observations).  The file is final: it is banked once and read from
     the bank; a missing bank is rebuilt from the source.
  2. Tri-party haircuts.  The 81 monthly series were parsed once, by an op, in August 2026 and never again, so they
     ended on 2026-07-09.  The New York Fed publishes two workbooks: May 2010 - October 2025 under the old reporting,
     and December 2024 onwards under the reporting it introduced in November 2025.  haircut_series() reads both and
     refresh_haircuts() keeps the bank level with them, asking only once the next monthly snapshot can exist.
  3. DTCC's daily fails file moved from www.dtcc.com/data/ to cms-prod.dtcc.com/data/ (a real browser showed the
     request, ops 6899); the old address answers 404 since 2026-09-09.  The new one sits behind a cache that served
     a week-old copy until the request carried a fresh `v` parameter, and it refuses Python's default client name.

Parsers are pure (bytes in, series out).  bank_survey_rate() and refresh_haircuts() take their reads, fetches and
writes as arguments: the engine wires them in, the tests and the dry-run op run them on stubs.
"""
from datetime import date, timedelta
import hashlib
import io
import re
import xml.etree.ElementTree as ET
import zipfile

_M = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_R = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}"

SURVEY_URL = "https://www.newyorkfed.org/medialibrary/media/markets/HistoricalOvernightTreasGCRepoPriDealerSurvRate.xlsx"
SURVEY_PAGE = "https://www.newyorkfed.org/markets/opolicy/operating_policy_180309"
SURVEY_KEY = "data/warm/nyfed-research/gc-survey-rate.json"
SURVEY_ID = "GC_SURVEY"
SURVEY_TITLE = ("Overnight Treasury GC repo: primary dealer survey rate (%; New York Fed, daily 1998-02-20 to 2018-02-28, "
                "series ended) - the repo rate before SOFR")

TRIPARTY_NOW = "https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/tri-party-repo/tri-party-repo_data"
TRIPARTY_OLD = "https://www.newyorkfed.org/medialibrary/Research/Interactives/Data/tri-party-repo/tri-party-repo-preNov25_data"
TRIPARTY_PAGE = "https://www.newyorkfed.org/research/tri-party-repo"
HAIRCUT_PREFIX = "data/warm/nyfed-research/haircuts-series/"
HAIRCUT_INDEX = HAIRCUT_PREFIX + "_index.json"
HAIRCUT_SHEET = "volume_haircut_concentration"
# the workbook's column -> the bank's series suffix (the suffixes are the ones the August 2026 parse chose)
HAIRCUT_METRICS = (("collateral_value", "collateral_value"), ("share_of_total", "collateral_share"),
                   ("top_3_concentration", "top3_concentration"), ("median", "median_haircut"))
# A snapshot is the seventh business day of a month.  The next one cannot be published before the bank's newest is
# this old; from then on the source is asked on every run until it arrives.
HAIRCUT_ASK_AFTER_DAYS = 36

DTCC_FAILS_URLS = ("https://cms-prod.dtcc.com/data/failsdata.csv", "https://www.dtcc.com/data/failsdata.csv")
DTCC_PAGE = "https://www.dtcc.com/market-index-data/charts/daily-total-us-treasury-trade-fails"
DTCC_CLIENT = {"User-Agent": "Mozilla/5.0"}


# ------------------------------------------------------------------ the engine's reads, fetches and writes
def bank_io(s3, bucket, client="Mozilla/5.0 JustHodl research"):
    """(read, fetch, put) for the routines below.  read() answers None only for a key that does not exist: any other
    failure is raised, so a bad minute at S3 can never be mistaken for an empty bank."""
    import gzip
    import json
    import urllib.request

    def read(key):
        try:
            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except Exception as e:  # noqa: BLE001
            code = str((getattr(e, "response", None) or {}).get("Error", {}).get("Code", ""))
            if code in ("NoSuchKey", "404") or "NoSuchKey" in str(e):
                return None
            raise
        return json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)

    def fetch(url):
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": client}), timeout=60) as resp:
            return resp.read()

    def put(key, doc):
        s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(doc, separators=(",", ":")).encode(), ContentType="application/json")

    return read, fetch, put


def bank_keys(s3, bucket):
    """list(prefix) -> every key under a prefix."""
    def keys(prefix):
        out, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            out.extend(o["Key"] for o in page.get("Contents") or [])
            token = page.get("NextContinuationToken")
            if not token:
                return out
    return keys


# ------------------------------------------------------------------ the FRED group's inventory
# 2026-11-22: FRED refused every request for about half a minute during the 06:10 UTC run.  The engine asked its
# discovery once, got nothing, and the board lost the whole FRED group - 1,028 series, the policy rates among them -
# and the six spreads built on it (ops 6904, 6907, 6908; FRED answered normally ten minutes later).  A discovery that comes back short is now asked
# again, and when FRED still refuses the group is served from its bank: every series ever discovered is banked.
FRED_API = "https://api.stlouisfed.org/fred/"
FRED_INVENTORY_KEY = "data/repo-state/fred-repo-inventory.json"
FRED_CLIENT = {"User-Agent": "JustHodl research"}


def fred_json(url, tries=3, pauses=(3, 8), sleep=None, opener=None):
    """One FRED answer as JSON; a refusal that can pass (429, 5xx, a dropped connection) is asked again."""
    import json
    import time
    import urllib.error
    import urllib.request
    opener, sleep = opener or urllib.request.urlopen, sleep or time.sleep
    for attempt in range(tries):
        try:
            with opener(urllib.request.Request(url, headers=FRED_CLIENT), timeout=20) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504) or attempt == tries - 1:
                raise
        except (OSError, ValueError):
            if attempt == tries - 1:
                raise
        sleep(pauses[min(attempt, len(pauses) - 1)])


def fred_repo_inventory(key, get_json):
    """{series id: title}: the same three questions the engine's discovery asks (two pages of the 'repurchase
    agreements' tag, and the SOFR family)."""
    from urllib.parse import urlencode
    out = {}
    for offset in (0, 500):
        rows = get_json(FRED_API + "tags/series?" + urlencode({"tag_names": "repurchase agreements", "api_key": key, "file_type": "json",
                                                               "limit": 500, "offset": offset})).get("seriess") or []
        for row in rows:
            out[row["id"]] = str(row.get("title") or "")[:150]
        if len(rows) < 500:
            break
    for row in get_json(FRED_API + "series/search?" + urlencode({"search_text": "SOFR", "api_key": key, "file_type": "json", "limit": 60})).get("seriess") or []:
        if row["id"].startswith("SOFR"):
            out.setdefault(row["id"], str(row.get("title") or "")[:150])
    return out


def inventory_short(found, last_good):
    """A discovery with nothing in it, or with under four fifths of the last good one, is not the whole group."""
    return not found or (bool(last_good) and len(found) < 0.8 * len(last_good))


def fred_group_inventory(found, key, read, put, keys, bank_prefix, now, get_json=None, sleep=None):
    """({series id: title}, how it was obtained) for this run.

    found = what the engine's own single-shot discovery returned (possibly nothing, possibly half).  A whole
    inventory is recorded as the last good one, written only when it differs; a short one is asked again; and if
    FRED still refuses, the last good inventory - or, before one exists, every banked document - is used, so the
    series stay on the board at their banked values.
    """
    import time
    last = read(FRED_INVENTORY_KEY) or {}
    last_ids = last.get("ids") if isinstance(last.get("ids"), dict) else {}
    found, how = dict(found), "fred"
    if inventory_short(found, last_ids) and key:
        try:
            (sleep or time.sleep)(3)
            found, how = fred_repo_inventory(key, get_json or fred_json), "fred, asked again"
        except Exception as e:  # noqa: BLE001
            how = "fred refused (%s)" % type(e).__name__
    if how.startswith("fred refused") or not found or (how == "fred" and inventory_short(found, last_ids)):
        if last_ids:
            banked, source = dict(last_ids), "the last good inventory of %s" % str(last.get("at") or "")[:10]
        else:
            banked = {k.rsplit("/", 1)[-1][:-5]: "" for k in keys(bank_prefix + "/") if k.endswith(".json") and not k.rsplit("/", 1)[-1].startswith("_")}
            source = "the bank's own listing"
        banked.update({k: v for k, v in found.items() if v})
        return banked, "%s: served from %s" % (how if how != "fred" else "fred answered nothing", source)
    if found != last_ids:
        put(FRED_INVENTORY_KEY, {"at": now, "ids": found})
    return found, how


# ------------------------------------------------------------------ workbooks, standard library only
def _column(ref):
    n = 0
    for ch in ref:
        if ch.isalpha():
            n = n * 26 + (ord(ch.upper()) - 64)
    return n - 1


def workbook(raw):
    """{sheet name: rows} of an .xlsx file; a row is a list of str | float | None by column."""
    try:
        z = zipfile.ZipFile(io.BytesIO(raw))
        names = set(z.namelist())
        shared = []
        if "xl/sharedStrings.xml" in names:
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall(_M + "si"):
                shared.append("".join(t.text or "" for t in si.iter(_M + "t")))
        target = {rel.get("Id"): rel.get("Target") for rel in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
        out = {}
        for sh in ET.fromstring(z.read("xl/workbook.xml")).find(_M + "sheets"):
            path = target[sh.get(_R + "id")]
            path = path.lstrip("/") if path.startswith("/") else "xl/" + path
            rows = []
            for row in ET.fromstring(z.read(path)).iter(_M + "row"):
                cells = {}
                for c in row.findall(_M + "c"):
                    v, kind = c.find(_M + "v"), c.get("t")
                    if kind == "s":
                        val = shared[int(v.text)] if v is not None else None
                    elif kind == "inlineStr":
                        val = "".join(t.text or "" for t in c.iter(_M + "t"))
                    elif kind in ("str", "e", "b"):
                        val = v.text if v is not None else None
                    else:
                        try:
                            val = float(v.text) if v is not None and v.text not in (None, "") else None
                        except ValueError:
                            val = v.text
                    cells[_column(c.get("r") or "A")] = val
                rows.append([cells.get(i) for i in range(max(cells) + 1 if cells else 0)])
            out[sh.get("name")] = rows
        return out
    except (zipfile.BadZipFile, KeyError, ET.ParseError, AttributeError, IndexError, TypeError) as e:
        raise ValueError("not a readable workbook: %s" % type(e).__name__) from e


def excel_day(n):
    """An Excel day number (1900 date system) as an ISO date."""
    return (date(1899, 12, 30) + timedelta(days=int(n))).isoformat()


def _day(s):
    return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))


def _number(v):
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(str(v).replace(",", "").strip())
    except ValueError:
        return None


# ------------------------------------------------------------------ 1. the dealer survey rate, 1998-2018
def survey_rate(raw):
    """[(ISO day, rate in %)] oldest first, from the New York Fed's workbook.  Refuses anything that is not that series."""
    book = workbook(raw)
    named = any(isinstance(c, str) and "survey rate" in c.lower() for rows in book.values() for r in rows[:12] for c in r)
    best = []
    for rows in book.values():
        pairs = [(excel_day(r[0]), r[1]) for r in rows
                 if len(r) >= 2 and isinstance(r[0], float) and 30000 < r[0] < 60000 and isinstance(r[1], float)]
        if len(pairs) > len(best):
            best = pairs
    days = [d for d, _ in best]
    if (not named or len(best) < 4000 or days != sorted(set(days)) or not "1998-01-01" <= days[0] <= "1998-12-31"
            or any(not -1.0 <= v <= 30.0 for _, v in best)):
        raise ValueError("not the primary dealer survey rate workbook")
    return best


def bank_survey_rate(read, fetch, put, now):
    """Bank the survey rate once.  read(key) -> document or None; fetch(url) -> bytes; put(key, document)."""
    doc = read(SURVEY_KEY)
    if isinstance(doc, dict) and len(doc.get("observations") or []) >= 4000:
        return {"status": "banked", "n": len(doc["observations"])}
    raw = fetch(SURVEY_URL)
    pairs = survey_rate(raw)
    put(SURVEY_KEY, {"id": SURVEY_ID, "title": SURVEY_TITLE, "banked_at": now, "source": SURVEY_URL, "final": True,
                     "source_sha256": hashlib.sha256(raw).hexdigest(),
                     "observations": [{"date": d, "value": v} for d, v in pairs]})
    return {"status": "written", "n": len(pairs), "first": pairs[0][0], "last": pairs[-1][0]}


def survey_scope():
    """The board's scope entry for the banked survey rate."""
    return {"mnemonic": SURVEY_ID, "name": SURVEY_TITLE, "family": "GCSURVEY", "s3_key": SURVEY_KEY, "tier": 1, "api_url": SURVEY_PAGE}


# ------------------------------------------------------------------ 2. tri-party haircuts
def slug(name):
    return re.sub(r"[^a-z0-9]+", "-", str(name).lower()).strip("-")


def _snapshot_day(v):
    s = str(int(v)) if isinstance(v, float) and v == int(v) else str(v or "").strip()
    if re.fullmatch(r"(19|20)\d{6}", s):
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8])).isoformat()
        except ValueError:
            return None
    return None


def triparty_rows(raw):
    """[{date, group, <the four metric columns>}] from a tri-party workbook's haircut sheet."""
    rows = workbook(raw).get(HAIRCUT_SHEET)
    if rows is None:
        raise ValueError("tri-party workbook has no %s sheet" % HAIRCUT_SHEET)
    head = next((i for i, r in enumerate(rows) if r and r[0] == "date"), None)
    if head is None or not {"group_name"} | {c for c, _ in HAIRCUT_METRICS} <= set(rows[head]):
        raise ValueError("tri-party haircut sheet has lost a column")
    names, out = rows[head], []
    for r in rows[head + 1:]:
        rec = {names[i]: r[i] for i in range(min(len(names), len(r))) if names[i]}
        day, group = _snapshot_day(rec.get("date")), rec.get("group_name")
        if not day or not isinstance(group, str) or not group.strip():
            continue
        row = {"date": day, "group": group.strip()}
        for col, _ in HAIRCUT_METRICS:
            row[col] = _number(rec.get(col))
        out.append(row)
    return out


def haircut_series(old_rows, new_rows):
    """{series id: {"title", "observations": {day: value}}} out of the two workbooks' rows.

    Where both workbooks hold a snapshot (December 2024 - October 2025) the new reporting is used: it is the
    publisher's current statement of those months and the one every later month is comparable with.
    """
    out = {}
    for rows in (old_rows, new_rows):                 # the new workbook is applied second, so it wins a shared day
        for r in rows:
            for col, suffix in HAIRCUT_METRICS:
                if r.get(col) is None:
                    continue
                sid = "%s--%s" % (slug(r["group"]), suffix)
                ser = out.setdefault(sid, {"title": "Tri-party haircuts: %s — %s" % (r["group"], suffix.replace("_", " ")), "observations": {}})
                ser["observations"][r["date"]] = r[col]
    return out


def refresh_haircuts(read, fetch, put, now, today, force=False, dry_run=False):
    """Bring the haircut bank level with the New York Fed's workbooks; writes only the series that changed.

    read(key) -> document or None; fetch(url) -> bytes; put(key, document); today = a date.
    The bank is the permanent history: a banked observation the source no longer lists is kept, and so is a banked
    series the source has dropped (four asset classes ended in 2011 and four in 2015).
    """
    index = read(HAIRCUT_INDEX) or {}
    entries = {e["id"]: e for e in (index.get("series") or []) if isinstance(e, dict) and e.get("id")}
    newest = max([e.get("last") or "" for e in entries.values()] or [""])
    if entries and not force and (today - _day(newest)).days < HAIRCUT_ASK_AFTER_DAYS:
        return {"status": "not due", "newest": newest, "series": len(entries), "asked": False}
    new_rows = triparty_rows(fetch(TRIPARTY_NOW))
    old_rows = triparty_rows(fetch(TRIPARTY_OLD)) if (force or not entries) else []
    source_newest = max([r["date"] for r in new_rows] or [""])
    if len(new_rows) < 100 or len({r["group"] for r in new_rows}) < 15 or (newest and source_newest < newest):
        raise ValueError("tri-party workbook looks cut short (%d rows, newest %s, bank %s)" % (len(new_rows), source_newest or "-", newest or "-"))
    fresh = haircut_series(old_rows, new_rows)
    stats = {"status": "level", "asked": True, "source_newest": source_newest, "bank_newest_before": newest or None, "written": 0, "unchanged": 0,
             "new_series": [], "observations_added": 0, "observations_revised": 0, "revised_sample": [], "dry_run": bool(dry_run),
             "old_workbook_read": bool(old_rows)}
    known = set(entries)
    for sid in sorted(fresh):
        ser = fresh[sid]
        doc = read(HAIRCUT_PREFIX + sid + ".json") if sid in entries else None
        banked = {o["date"]: o["value"] for o in ((doc or {}).get("observations") or [])}
        added = [d for d in ser["observations"] if d not in banked]
        revised = [d for d, v in ser["observations"].items() if d in banked and banked[d] != v]
        if not added and not revised:
            stats["unchanged"] += 1
            continue
        merged = dict(banked)
        merged.update(ser["observations"])
        title = (doc or {}).get("title") or ser["title"]
        if not dry_run:
            put(HAIRCUT_PREFIX + sid + ".json", {"id": sid, "title": title, "observations": [{"date": d, "value": merged[d]} for d in sorted(merged)]})
        entries[sid] = {"id": sid, "title": title, "n": len(merged), "first": min(merged), "last": max(merged)}
        stats["written"] += 1
        stats["observations_added"] += len(added)
        stats["observations_revised"] += len(revised)
        if sid not in known:
            stats["new_series"].append(sid)
        for d in sorted(revised)[:2]:
            if len(stats["revised_sample"]) < 12:
                stats["revised_sample"].append([sid, d, banked[d], ser["observations"][d]])
    stats["series"] = len(entries)
    stats["bank_newest_after"] = max([e.get("last") or "" for e in entries.values()] or [""]) or None
    if stats["written"]:
        stats["status"] = "would write" if dry_run else "written"
        if not dry_run:
            put(HAIRCUT_INDEX, {"built_at": index.get("built_at") or now, "refreshed_at": now, "source": [TRIPARTY_OLD, TRIPARTY_NOW],
                                "series": [entries[k] for k in sorted(entries)]})
    return stats


# ------------------------------------------------------------------ 3. DTCC daily fails
def dtcc_url(base, stamp_ms):
    """DTCC's own page asks with a fresh `v`; without it the cache in front of the file answered a week-old copy."""
    return "%s?v=%d" % (base, stamp_ms)


def dtcc_fails(text):
    """({day: Treasury fails, USD}, {day: agency fails, USD}) from DTCC's daily file.

    The file ends with `TRAILER,<rows>`; a file whose trailer does not match its rows was cut short and is refused.
    """
    treasury, agency, trailer, head = {}, {}, None, ""
    for line in str(text).splitlines():
        cells = [c.strip() for c in line.split(",")]
        if cells[0].upper() == "TRAILER":
            trailer = int(cells[1]) if len(cells) > 1 and cells[1].isdigit() else None
            continue
        if cells[0].lower() == "date":
            head = line.lower()
            continue
        m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", cells[0])
        if not m or len(cells) < 3:
            continue
        try:
            day = date(int(m.group(3)), int(m.group(1)), int(m.group(2))).isoformat()
            treasury[day], agency[day] = float(cells[1]), float(cells[2])
        except ValueError:
            continue
    if "treasury" not in head or "agency" not in head or len(treasury) < 20 or (trailer is not None and trailer != len(treasury)):
        raise ValueError("not DTCC's daily fails file (%d rows, trailer %s)" % (len(treasury), trailer))
    return treasury, agency
