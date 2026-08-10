"""justhodl-grid-queue v1.0 — POWER BUILDOUT CANARY (T-9 to T-6).

Canary list item #2. A fab, data center, or electrolyzer is visible in an
interconnection queue and in planned-capacity filings roughly 18-24 months
before it appears in anyone's revenue line. Retail does not watch this.

AUDIT (ops 3733/3734, extend-don't-duplicate): capex-pulse measures REPORTED
capex dollars (backward-looking, T-0); structural-pre-signals counts capex
MENTIONS in filings. Neither engine touches physical grid interconnection or
forward-dated capacity. Clean gap.

SOURCES — PROVEN LIVE, ops 3733/3734, keyless or existing key:
  CAISO PublicQueueReport.xlsx  sheets 'Grid GenerationQueue' /
        'Completed Generation Projects' / 'Withdrawn Generation Projects'.
        Parsed with stdlib zipfile + XML (no pandas/openpyxl in the zip).
  EIA v2 operating-generator-capacity  facets stateid/sector/technology/
        status; data cols include planned-uprate-year-month,
        planned-derate-year-month, planned-retirement-year-month, county,
        latitude, longitude. sector carries industrial-chp /
        industrial-non-chp — industrial self-generation is the fab/DC tell.
  EIA v2 retail-sales sectorid=IND  industrial electricity SALES by state:
        load shows up before revenue does.

DELIBERATE OMISSIONS (declared, never faked):
  ERCOT  reportTypeId for the GIS queue not resolved in 3734 (the IDs probed
         were the co-located battery report and DAM prices). Dropped from v1
         rather than shipped wrong.
  PJM    Data Miner 2 returns 401 without a subscription key.
  MISO   public API endpoint 404.
  LBNL   'Queued Up' page 403s from Lambda IPs (Cloudflare).
  PERMITS  EPA ECHO serves COMPLIANCE data, not construction permits. Using
         it as a permit proxy would be a fabrication, so it is excluded.
  These are reported in `gaps` on every run so the page can be honest.

WHAT IT COMPUTES
  queue_totals     active MW by state / technology / status, from CAISO
  queue_velocity   new entrants vs withdrawals (queue churn is the honest
                   read: a queue that only grows is an artifact, not demand)
  large_projects   individual projects >=100MW with county + fuel
  planned_capacity forward-dated uprates/additions by state and sector,
                   with the INDUSTRIAL cut isolated
  industrial_load  EIA industrial electricity sales by state, YoY + 3m mom
  hotspots         states where queue MW, planned industrial capacity, and
                   industrial load are ALL rising = physical buildout
                   confirmed on three independent legs
"""
import io
import json
import os
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3

from impact_mapper import (build as impact_build,
                           structural_row, beta_impact)

VERSION = "2.1.0"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/grid-queue.json"
HIST_KEY = "data/grid-queue-history.json"
S3 = boto3.client("s3", region_name="us-east-1")

UA = {"User-Agent": "JustHodl research admin@justhodl.ai"}
CTX = ssl.create_default_context()
EIA_KEY = os.environ.get("EIA_API_KEY", "")
EIA = "https://api.eia.gov/v2"
CAISO_QUEUE = "https://www.caiso.com/PublishedDocuments/PublicQueueReport.xlsx"

LARGE_MW = 100.0        # project size that matters for a read-through
HOT_STATES = 12


def _get(url, timeout=60, raw=False, retries=2):
    last = None
    for i in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
                b = r.read()
                return b if raw else b.decode("utf-8", "replace")
        except urllib.error.HTTPError as e:
            last = "HTTP %s" % e.code
            if e.code in (400, 401, 403, 404):
                break
        except Exception as e:
            last = "%s %s" % (type(e).__name__, str(e)[:80])
    print("[grid-queue] fetch fail:", last, url[:120])
    return None


# ── XLSX parsing, stdlib only ────────────────────────────────────────────
def _shared_strings(z):
    try:
        xml = z.read("xl/sharedStrings.xml").decode("utf-8", "replace")
    except KeyError:
        return []
    out = []
    for si in re.findall(r"<si>(.*?)</si>", xml, re.S):
        parts = re.findall(r"<t[^>]*>(.*?)</t>", si, re.S)
        out.append("".join(parts))
    return out


def _unescape(s):
    return (s.replace("&amp;", "&").replace("&lt;", "<")
             .replace("&gt;", ">").replace("&quot;", '"')
             .replace("&#39;", "'").replace("&apos;", "'"))


def _col_idx(ref):
    m = re.match(r"([A-Z]+)", ref or "")
    if not m:
        return 0
    n = 0
    for ch in m.group(1):
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _sheet_rows(z, sheet_path, shared, max_rows=20000):
    """Yield lists of cell values for one worksheet."""
    try:
        xml = z.read(sheet_path).decode("utf-8", "replace")
    except KeyError:
        return []
    rows = []
    for rm in re.finditer(r"<row[^>]*>(.*?)</row>", xml, re.S):
        cells = {}
        for cm in re.finditer(
                r'<c r="([A-Z]+\d+)"([^>]*)>(.*?)</c>', rm.group(1), re.S):
            ref, attrs, inner = cm.group(1), cm.group(2), cm.group(3)
            idx = _col_idx(ref)
            t = re.search(r't="([^"]+)"', attrs)
            typ = t.group(1) if t else "n"
            vm = re.search(r"<v>(.*?)</v>", inner, re.S)
            if typ == "inlineStr":
                im = re.findall(r"<t[^>]*>(.*?)</t>", inner, re.S)
                cells[idx] = _unescape("".join(im))
                continue
            if not vm:
                continue
            raw = vm.group(1)
            if typ == "s":
                try:
                    cells[idx] = _unescape(shared[int(raw)])
                except Exception:
                    cells[idx] = ""
            else:
                cells[idx] = _unescape(raw)
        if cells:
            width = max(cells) + 1
            rows.append([cells.get(i, "") for i in range(width)])
        if len(rows) >= max_rows:
            break
    return rows


def _f(v):
    try:
        s = str(v).replace(",", "").replace("$", "").strip()
        if not s:
            return None
        return float(s)
    except (TypeError, ValueError):
        return None


def _find_header(rows, needles, scan=30):
    """Locate the header row by matching expected column names."""
    for i, r in enumerate(rows[:scan]):
        low = [str(c).strip().lower() for c in r]
        joined = " | ".join(low)
        if sum(1 for n in needles if n in joined) >= 2:
            return i, low
    return None, None


def _pick(header, *cands):
    for ci, h in enumerate(header):
        for c in cands:
            if c in h:
                return ci
    return None


def parse_caiso(gaps):
    """CAISO public interconnection queue -> active / completed / withdrawn."""
    blob = _get(CAISO_QUEUE, timeout=90, raw=True)
    if not blob or blob[:2] != b"PK":
        gaps.append("CAISO queue workbook unavailable")
        return {}
    try:
        z = zipfile.ZipFile(io.BytesIO(blob))
    except Exception as e:
        gaps.append("CAISO xlsx unreadable: %s" % str(e)[:60])
        return {}
    shared = _shared_strings(z)

    # ops 3737: map sheet NAME -> path via workbook.xml.rels. Filename order
    # is NOT guaranteed to match workbook sheet order; relying on it is how
    # a parser reads the wrong tab and reports confident nonsense.
    sheet_map = {}
    try:
        wb = z.read("xl/workbook.xml").decode("utf-8", "replace")
        pairs = re.findall(r'<sheet[^>]*name="([^"]+)"[^>]*r:id="([^"]+)"', wb)
        rels = {}
        rx = z.read("xl/_rels/workbook.xml.rels").decode("utf-8", "replace")
        for m in re.finditer(r'Id="([^"]+)"[^>]*Target="([^"]+)"', rx):
            rels[m.group(1)] = m.group(2)
        for nm, rid in pairs:
            tgt = rels.get(rid, "")
            if tgt:
                sheet_map[nm] = tgt if tgt.startswith("xl/") else "xl/" + tgt.lstrip("/")
    except Exception as e:
        gaps.append("CAISO sheet mapping fell back to order: %s" % str(e)[:50])
    if not sheet_map:
        try:
            names = re.findall(r'<sheet[^>]*name="([^"]+)"',
                               z.read("xl/workbook.xml").decode("utf-8", "replace"))
        except Exception:
            names = []
        paths = sorted([n for n in z.namelist()
                        if n.startswith("xl/worksheets/sheet")],
                       key=lambda p: int(re.search(r"(\d+)", p).group(1)))
        for i, nm in enumerate(names):
            if i < len(paths):
                sheet_map[nm] = paths[i]

    def load(display_key, needles):
        path = None
        for nm, p in sheet_map.items():
            if display_key.lower() in nm.lower():
                path = p
                break
        if not path:
            return []
        rows = _sheet_rows(z, path, shared)
        hi, hdr = _find_header(rows, needles)
        if hi is None:
            return []
        c_st = _pick(hdr, "state")
        c_cty = _pick(hdr, "county")
        c_fuel = _pick(hdr, "fuel-1", "fuel", "technology", "type-1")
        c_name = _pick(hdr, "project name", "generation project", "name")
        c_stat = _pick(hdr, "application status",
                       "interconnection request status", "status")
        c_ia = _pick(hdr, "interconnection agreement", "agreement status",
                     "ia status", "gia")
        c_date = _pick(hdr, "proposed on-line date", "on-line date",
                       "commercial operation", "queue date")
        # ops 3737 FINDING: 'net mws to grid' is populated on only 2 of 277
        # CAISO rows — the real capacity sits in mw-1 / mw-2 / mw-3, one per
        # fuel leg. Sum those; fall back to net-mws only if none parse.
        mw_cols = [ci for ci, h in enumerate(hdr)
                   if re.match(r"^mw-\d", h.strip())]
        c_net = _pick(hdr, "net mws to grid", "net mw")
        out = []
        for r in rows[hi + 1:]:
            if not any(str(x).strip() for x in r):
                continue
            mw = 0.0
            for ci in mw_cols:
                if ci < len(r):
                    mw += _f(r[ci]) or 0.0
            if mw <= 0 and c_net is not None and c_net < len(r):
                mw = _f(r[c_net]) or 0.0
            if mw <= 0:
                continue
            # fuel legs: join non-empty fuel columns. ops 3738 caught CAISO
            # putting NUMBERS in fuel-2/fuel-3 (they mirror mw-2/mw-3), which
            # produced labels like "Solar + 1150". Reject anything numeric.
            fuels = []
            for ci, h in enumerate(hdr):
                if h.strip().startswith("fuel-") and ci < len(r):
                    v = str(r[ci]).strip()
                    if not v or v.lower() in ("", "n/a", "none", "-"):
                        continue
                    if _f(v) is not None:      # numeric => not a fuel name
                        continue
                    if v not in fuels:
                        fuels.append(v)
            if not fuels and c_fuel is not None and c_fuel < len(r):
                v = str(r[c_fuel]).strip()
                if v and _f(v) is None:
                    fuels.append(v)
            out.append({
                "project": (r[c_name] if c_name is not None and c_name < len(r) else "")[:70],
                "mw": round(mw, 1),
                "state": (r[c_st] if c_st is not None and c_st < len(r) else "").strip()[:20] or "CA",
                "county": (r[c_cty] if c_cty is not None and c_cty < len(r) else "").strip()[:40],
                "fuel": (" + ".join(fuels[:3]) if fuels else "Unspecified")[:44],
                "status": (r[c_stat] if c_stat is not None and c_stat < len(r) else "").strip()[:40],
                "ia": (r[c_ia] if c_ia is not None and c_ia < len(r) else "").strip()[:40],
                "online": (r[c_date] if c_date is not None and c_date < len(r) else "").strip()[:24],
            })
        return out

    needles = ["mw", "county", "fuel", "state", "project"]
    active = load("Grid GenerationQueue", needles)
    done = load("Completed Generation", needles)
    gone = load("Withdrawn Generation", needles)
    if not active:
        gaps.append("CAISO active queue parsed 0 rows")
    return {"active": active, "completed": done, "withdrawn": gone}


def eia_json(path, params):
    p = dict(params)
    p["api_key"] = EIA_KEY
    url = "%s/%s?%s" % (EIA, path, urllib.parse.urlencode(p, doseq=True))
    b = _get(url, timeout=60)
    if not b:
        return None
    try:
        return json.loads(b)
    except Exception:
        return None


def eia_planned(gaps):
    """Forward-dated capacity: uprates + planned online dates, by state/sector."""
    d = eia_json("electricity/operating-generator-capacity/data/", {
        "frequency": "monthly",
        "data[0]": "net-summer-capacity-mw",
        "data[1]": "planned-uprate-summer-cap-mw",
        "sort[0][column]": "period", "sort[0][direction]": "desc",
        "length": 5000,
    })
    rows = ((d or {}).get("response") or {}).get("data") or []
    if not rows:
        gaps.append("EIA planned capacity returned 0 rows")
        return {}
    latest = rows[0].get("period")
    by_state, by_sector, industrial = {}, {}, []
    upcoming = []
    for r in rows:
        if r.get("period") != latest:
            continue
        st = r.get("stateid") or "?"
        sec = r.get("sector") or "?"
        up = _f(r.get("planned-uprate-summer-cap-mw")) or 0.0
        cap = _f(r.get("net-summer-capacity-mw")) or 0.0
        by_state[st] = by_state.get(st, 0.0) + up
        by_sector[sec] = by_sector.get(sec, 0.0) + up
        if "industrial" in sec and cap > 0:
            industrial.append({
                "state": st, "entity": (r.get("entityName") or "")[:50],
                "plant": (r.get("plantName") or "")[:50],
                "county": r.get("county"), "tech": r.get("technology"),
                "capacity_mw": round(cap, 1), "sector": sec,
            })
        uy = r.get("planned-uprate-year-month")
        if uy and up > 0:
            upcoming.append({"state": st, "entity": (r.get("entityName") or "")[:50],
                             "plant": (r.get("plantName") or "")[:46],
                             "when": uy, "uprate_mw": round(up, 1),
                             "tech": r.get("technology")})
    industrial.sort(key=lambda x: -x["capacity_mw"])
    upcoming.sort(key=lambda x: -x["uprate_mw"])
    return {
        "period": latest,
        "uprate_by_state": dict(sorted(by_state.items(), key=lambda kv: -kv[1])[:25]),
        "uprate_by_sector": dict(sorted(by_sector.items(), key=lambda kv: -kv[1])),
        "industrial_plants": industrial[:60],
        "n_industrial": len(industrial),
        "upcoming_uprates": upcoming[:40],
    }


def eia_industrial_load(gaps):
    """Industrial electricity SALES by state — load precedes revenue."""
    d = eia_json("electricity/retail-sales/data/", {
        "frequency": "monthly", "data[0]": "sales",
        "facets[sectorid][]": "IND",
        "sort[0][column]": "period", "sort[0][direction]": "desc",
        "length": 5000,
    })
    rows = ((d or {}).get("response") or {}).get("data") or []
    if not rows:
        gaps.append("EIA industrial retail sales returned 0 rows")
        return {}
    series = {}
    for r in rows:
        st, per = r.get("stateid"), r.get("period")
        v = _f(r.get("sales"))
        if st and per and v is not None:
            series.setdefault(st, {})[per] = v
    latest = max((p for s in series.values() for p in s), default=None)
    if not latest:
        return {}
    y, m = int(latest[:4]), int(latest[5:7])
    prev_y = "%04d-%02d" % (y - 1, m)

    def back(n):
        yy, mm = y, m
        for _ in range(n):
            mm -= 1
            if mm == 0:
                mm, yy = 12, yy - 1
        return "%04d-%02d" % (yy, mm)

    out = []
    for st, s in series.items():
        cur = s.get(latest)
        py = s.get(prev_y)
        if cur is None:
            continue
        yoy = round((cur / py - 1) * 100, 2) if py else None
        c3 = [s.get(back(i)) for i in range(3)]
        p3 = [s.get(back(i)) for i in range(3, 6)]
        m3 = (round((sum(c3) / sum(p3) - 1) * 100, 2)
              if all(x for x in c3) and all(x for x in p3) else None)
        out.append({"state": st, "sales_gwh": round(cur, 1),
                    "yoy_pct": yoy, "mom_3m_pct": m3})
    out.sort(key=lambda x: -(x["yoy_pct"] if x["yoy_pct"] is not None else -999))
    return {"period": latest, "states": out}


# ═══════════════════════════════════════════════════════════════════════════
# ops-4559 BUG-12 — multi-ISO layer + executed-IA MW + LBNL survival priors
#
# LBNL "Queued Up": of all requests entering US queues, ~13% ever reach COD,
# ~75% withdraw, median request→operation now >5 years. Headline queue MW is
# therefore mostly vapor; the tradeable figure is MW with an EXECUTED
# interconnection agreement. Every adapter below is probe-first: a failed
# fetch lands in gaps[] with the reason, never a hardcoded assumption.
# Large-load (datacenter) interconnection is NOT in generation queues — that
# blind spot is declared as unmeasured, not absent.
# ═══════════════════════════════════════════════════════════════════════════

LBNL_PRIORS = {
    "source": "LBNL 'Queued Up' (emp.lbl.gov/queues)",
    "vintage": "2024 edition — 2000-2018 request cohorts (capacity basis)",
    "completion_rate_all_requests": 0.13,
    "withdrawal_rate": 0.75,
    "median_years_request_to_cod": 5.0,
    "completion_rate_by_fuel": {"gas": 0.31, "wind": 0.20, "solar": 0.14,
                                 "battery": 0.11, "storage": 0.11},
    "fuel_note": ("published LBNL cohort completion rates by fuel; applied "
                  "per-project where the queue row carries a recognizable "
                  "fuel, else the 13% all-request rate. cited_static_priors "
                  "— direct LBNL download 403s from Lambda IPs; refresh "
                  "manually on new editions"),
    "note": ("headline queue MW × completion prior approximates realistic "
             "eventual COD MW; executed-IA MW is the survivor pool and is "
             "reported unhaircut as the primary tradeable figure. Direct "
             "LBNL download 403s from Lambda IPs.")}

_IA_RX = re.compile(r"(executed|signed|effective)", re.I)
_IA_CTX = re.compile(r"\b(ia|gia|interconnection\s+agreement|lgia|sgia)\b", re.I)


def _classify_ia(row):
    """True if this row's agreement text says the IA is executed/signed."""
    ia = str(row.get("ia") or "")
    if ia and _IA_RX.search(ia):
        return True
    st = str(row.get("status") or "")
    return bool(_IA_CTX.search(st) and _IA_RX.search(st))


def _mk_iso(iso, rows, src, ia_detect, gaps, note=None):
    headline = round(sum(r.get("mw") or 0 for r in rows), 1)
    ia_mw = None
    if ia_detect != "none":
        ia_mw = round(sum((r.get("mw") or 0) for r in rows if _classify_ia(r)), 1)
    if not rows:
        gaps.append("%s adapter parsed 0 rows" % iso)
    by_fuel = {}
    for r in rows:
        k = (r.get("fuel") or "?")
        by_fuel[k] = by_fuel.get(k, 0.0) + (r.get("mw") or 0)
    return {"iso": iso, "n_projects": len(rows), "headline_mw": headline,
            "mw_with_executed_ia": ia_mw, "ia_detection": ia_detect,
            "src": src, "note": note,
            "by_fuel_mw": dict(sorted(by_fuel.items(), key=lambda kv: -kv[1])[:12]),
            "large_projects": sorted([r for r in rows if (r.get("mw") or 0) >= LARGE_MW],
                                     key=lambda r: -r["mw"])[:25]}


def _generic_xlsx_rows(blob, needles, mw_cands, gaps, tag):
    """Any-ISO xlsx → rows using the stdlib parser already in this file."""
    try:
        z = zipfile.ZipFile(io.BytesIO(blob))
    except Exception as e:
        gaps.append("%s workbook unreadable: %s" % (tag, str(e)[:50])); return []
    shared = _shared_strings(z)
    sheets = [n for n in z.namelist() if n.startswith("xl/worksheets/sheet")]
    best = []
    for sp in sorted(sheets)[:8]:
        rows = _sheet_rows(z, sp, shared, max_rows=30000)
        hi, hdr = _find_header(rows, needles)
        if hi is None:
            continue
        c_mw = _pick(hdr, *mw_cands)
        if c_mw is None:
            continue
        c_name = _pick(hdr, "project name", "generation project", "project", "name")
        c_st = _pick(hdr, "state")
        c_cty = _pick(hdr, "county")
        c_fuel = _pick(hdr, "fuel", "technology", "type", "resource")
        c_stat = _pick(hdr, "status", "phase", "stage", "availability")
        c_ia = _pick(hdr, "interconnection agreement", "ia signed", "ia executed",
                     "gia", "agreement", "ia date", "ia tender")
        out = []
        for r in rows[hi + 1:]:
            mw = _f(r[c_mw]) if c_mw < len(r) else None
            if not mw or mw <= 0:
                continue
            def g(ci):
                return str(r[ci]).strip() if (ci is not None and ci < len(r)) else ""
            out.append({"project": g(c_name)[:70], "mw": round(mw, 1),
                        "state": g(c_st)[:20], "county": g(c_cty)[:40],
                        "fuel": (g(c_fuel) or "Unspecified")[:44],
                        "status": g(c_stat)[:60], "ia": g(c_ia)[:60]})
        if len(out) > len(best):
            best = out
            best_ia = c_ia is not None
    if not best:
        gaps.append("%s: no parseable sheet (needles=%s)" % (tag, needles))
        return []
    best_ia = any(r.get("ia") for r in best)
    for r in best:
        r["_ia_col"] = best_ia
    return best


def fetch_nyiso(gaps):
    url = "https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx"
    blob = _get(url, timeout=90, raw=True)
    if not blob:
        gaps.append("NYISO queue xlsx unavailable (probe failed)"); return None
    rows = _generic_xlsx_rows(blob, ["project", "mw", "county"],
                              ("sp (mw)", "sp(mw)", "capacity", "mw"), gaps, "NYISO")
    if not rows:
        return None
    detect = "column" if rows and rows[0].get("_ia_col") else "status_regex"
    return _mk_iso("NYISO", rows, url, detect, gaps)


def fetch_miso(gaps):
    url = "https://www.misoenergy.org/api/giqueue/getprojects"
    b = _get(url, timeout=60)
    if not b:
        gaps.append("MISO giqueue API unavailable"); return None
    try:
        j = json.loads(b)
    except Exception:
        gaps.append("MISO giqueue non-JSON"); return None
    items = j if isinstance(j, list) else (j.get("rows") or j.get("data") or [])
    rows = []
    for it in items:
        if not isinstance(it, dict):
            continue
        low = {str(k).lower(): v for k, v in it.items()}
        def pick(*cands):
            for c in cands:
                for k, v in low.items():
                    if c in k and v not in (None, ""):
                        return v
            return ""
        status = str(pick("applicationstatus", "status"))
        if status and "active" not in status.lower():
            continue
        mw = _f(pick("summernetmw", "netmw", "megawatt", "mw"))
        if not mw or mw <= 0:
            continue
        rows.append({"project": str(pick("projectnumber", "projectname", "j"))[:70],
                     "mw": round(mw, 1), "state": str(pick("state"))[:20],
                     "county": str(pick("county"))[:40],
                     "fuel": (str(pick("fueltype", "fuel")) or "Unspecified")[:44],
                     "status": status[:60],
                     "ia": str(pick("giastatus", "gia", "agreementstatus", "giaexecut"))[:60]})
    if not rows:
        gaps.append("MISO giqueue parsed 0 active rows"); return None
    detect = "column" if any(r["ia"] for r in rows) else "status_regex"
    return _mk_iso("MISO", rows, url, detect, gaps)


def fetch_spp(gaps):
    for url in ("https://opsportal.spp.org/Studies/GenerateActiveCSV",
                "https://portal.spp.org/file-browser-api/download/generation-interconnection-active-queue?path=%2FGI_ActiveRequests.csv"):
        b = _get(url, timeout=60)
        if b and "," in b:
            try:
                import csv as _csv
                rdr = list(_csv.reader(io.StringIO(b)))
            except Exception:
                continue
            hi, hdr = _find_header(rdr, ["mw", "state", "status"])
            if hi is None:
                continue
            c_mw = _pick(hdr, "capacity", "mw"); c_st = _pick(hdr, "state")
            c_cty = _pick(hdr, "county"); c_fuel = _pick(hdr, "fuel", "generation type")
            c_stat = _pick(hdr, "status", "stage"); c_ia = _pick(hdr, "gia", "agreement")
            c_name = _pick(hdr, "request", "project", "name")
            rows = []
            for r in rdr[hi + 1:]:
                mw = _f(r[c_mw]) if (c_mw is not None and c_mw < len(r)) else None
                if not mw or mw <= 0:
                    continue
                def g(ci):
                    return str(r[ci]).strip() if (ci is not None and ci < len(r)) else ""
                rows.append({"project": g(c_name)[:70], "mw": round(mw, 1),
                             "state": g(c_st)[:20], "county": g(c_cty)[:40],
                             "fuel": (g(c_fuel) or "Unspecified")[:44],
                             "status": g(c_stat)[:60], "ia": g(c_ia)[:60]})
            if rows:
                detect = "column" if any(r["ia"] for r in rows) else "status_regex"
                return _mk_iso("SPP", rows, url, detect, gaps)
    gaps.append("SPP active-queue CSV unavailable on both candidate endpoints")
    return None


def fetch_isone(gaps):
    """v2.1: the two historical IRTT endpoints started failing live (4574).
    Adds a browser UA (IRTT sits behind an Incapsula-style edge that 403s
    plain clients), a third endpoint variant, and — critically — records
    the exact response signature per endpoint so the next debugging session
    starts from evidence, not guesses."""
    sigs = []
    for url in ("https://irtt.iso-ne.com/reports/external?download=csv",
                "https://irtt.iso-ne.com/reports/external.csv",
                "https://irtt.iso-ne.com/external/reports?download=csv"):
        # wo4585: the 4582 gap showed bare "HTTP 302" on all endpoints —
        # urllib's default handler raised instead of following. Walk the
        # chain manually (<=3 hops): a redirect that lands on CSV self-
        # heals; an edge challenge gets NAMED in the gap for next time.
        hdrs = {"User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                               "AppleWebKit/537.36 (KHTML, like Gecko) "
                               "Chrome/126.0 Safari/537.36"),
                "Accept": "text/csv,application/csv,*/*"}

        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *a, **k):
                return None

        opener = urllib.request.build_opener(_NoRedirect)
        cur, chain, b = url, [], None
        try:
            for _hop in range(4):
                try:
                    with opener.open(urllib.request.Request(cur, headers=hdrs),
                                     timeout=60) as r0:
                        b = r0.read().decode("utf-8", "replace")
                        break
                except urllib.error.HTTPError as e0:
                    if e0.code in (301, 302, 303, 307, 308):
                        loc = e0.headers.get("Location") or ""
                        chain.append("%s→%s %s" % (e0.code,
                                                   loc[:90] or "<no-Location>",
                                                   ""))
                        if not loc:
                            break
                        cur = urllib.parse.urljoin(cur, loc)
                        continue
                    chain.append("HTTP %s" % e0.code)
                    break
        except Exception as e:
            chain.append(type(e).__name__)
        if b is None:
            sigs.append("%s → %s" % (url.split("/")[-1][:28],
                                     " | ".join(chain) or "no-response"))
            continue
        if b and "," in b and "<html" not in b[:200].lower():
            try:
                import csv as _csv
                rdr = list(_csv.reader(io.StringIO(b)))
            except Exception:
                continue
            hi, hdr = _find_header(rdr, ["mw", "project", "status"])
            if hi is None:
                continue
            c_mw = _pick(hdr, "net mw", "capacity", "mw")
            c_stat = _pick(hdr, "status", "stage"); c_ia = _pick(hdr, "ia", "agreement")
            c_name = _pick(hdr, "project", "alternative name", "name")
            c_fuel = _pick(hdr, "fuel", "type"); c_st = _pick(hdr, "state")
            rows = []
            for r in rdr[hi + 1:]:
                mw = _f(r[c_mw]) if (c_mw is not None and c_mw < len(r)) else None
                if not mw or mw <= 0:
                    continue
                def g(ci):
                    return str(r[ci]).strip() if (ci is not None and ci < len(r)) else ""
                rows.append({"project": g(c_name)[:70], "mw": round(mw, 1),
                             "state": g(c_st)[:20] or "NE", "county": "",
                             "fuel": (g(c_fuel) or "Unspecified")[:44],
                             "status": g(c_stat)[:60], "ia": g(c_ia)[:60]})
            if rows:
                detect = "column" if any(r["ia"] for r in rows) else "status_regex"
                return _mk_iso("ISO-NE", rows, url, detect, gaps)
        else:
            sigs.append("%s → 200 but %s" % (
                url.split("/")[-1][:28],
                ("html_body" if b and "<html" in b[:200].lower() else
                 "empty" if not b else "non-csv")))
    gaps.append("ISO-NE IRTT queue unavailable — %s" % "; ".join(sigs))
    return None


def fetch_ercot(gaps):
    lst = _get("https://www.ercot.com/misapp/servlets/IceDocListJsonWS?reportTypeId=15933",
               timeout=60)
    doc_id = None
    if lst:
        try:
            docs = (json.loads(lst).get("ListDocsByRptTypeRes") or {}).get("DocumentList") or []
            for d in docs:
                doc = d.get("Document") or {}
                if "GIS" in (doc.get("ConstructedName") or "") or True:
                    doc_id = doc.get("DocID"); break
        except Exception:
            pass
    if not doc_id:
        gaps.append("ERCOT GIS report list unavailable (reportTypeId 15933)"); return None
    blob = _get("https://www.ercot.com/misdownload/servlets/mirDownload?doclookupId=%s" % doc_id,
                timeout=120, raw=True)
    if not blob:
        gaps.append("ERCOT GIS report download failed (doc %s)" % doc_id); return None
    if blob[:2] == b"PK" and b"[Content_Types].xml" not in blob[:4000]:
        try:  # zip containing the xlsx
            zz = zipfile.ZipFile(io.BytesIO(blob))
            inner = [n for n in zz.namelist() if n.lower().endswith(".xlsx")]
            if inner:
                blob = zz.read(inner[0])
        except Exception:
            pass
    rows = _generic_xlsx_rows(blob, ["project", "capacity", "county"],
                              ("capacity (mw)", "capacity", "mw"), gaps, "ERCOT")
    if not rows:
        return None
    for r in rows:
        r["state"] = r.get("state") or "TX"
    detect = "column" if any(r.get("ia") for r in rows) else "status_regex"
    return _mk_iso("ERCOT", rows, "ercot.com GIS report (reportTypeId 15933)", detect, gaps)


def fetch_pjm(gaps):
    url = "https://services.pjm.com/PJMPlanningApi/api/Queue/ExportToXls"
    blob = _get(url, timeout=90, raw=True)
    if blob and blob[:2] == b"PK":
        rows = _generic_xlsx_rows(blob, ["project", "mw", "status"],
                                  ("mfo", "capacity", "mw"), gaps, "PJM")
        if rows:
            detect = "column" if any(r.get("ia") for r in rows) else "status_regex"
            return _mk_iso("PJM", rows, url, detect, gaps)
    gaps.append("PJM queue export blocked from Lambda (Planning API). "
                "Data Miner needs a subscription key — set PJM_API_KEY to enable.")
    return None


def fetch_ercot_large_load(gaps):
    """wo4580: ERCOT publishes a Large Load Interconnection Status report on
    MIS — the ONE public window into the datacenter blind spot. Probe-first:
    candidate reportTypeIds are tried and every miss is recorded; a wrong id
    lands in gaps[], never a fabricated number."""
    for rid in (16004, 15964):
        lst = _get("https://www.ercot.com/misapp/servlets/IceDocListJsonWS"
                   "?reportTypeId=%d" % rid, timeout=60)
        if not lst:
            continue
        try:
            docs = (json.loads(lst).get("ListDocsByRptTypeRes") or {})                 .get("DocumentList") or []
        except Exception:
            continue
        for d in docs[:3]:
            doc = d.get("Document") or {}
            nm = (doc.get("ConstructedName") or "")
            if "load" not in nm.lower():
                continue
            blob = _get("https://www.ercot.com/misdownload/servlets/mirDownload"
                        "?doclookupId=%s" % doc.get("DocID"), timeout=120,
                        raw=True)
            if not blob:
                continue
            rows = _generic_xlsx_rows(blob, ["load", "mw"],
                                      ("capacity (mw)", "mw", "load"),
                                      gaps, "ERCOT-LL")
            if rows:
                tot = round(sum(r.get("mw") or 0 for r in rows), 1)
                return {"status": "OK", "report": nm[:80], "n_rows": len(rows),
                        "total_mw": tot, "source_report_type_id": rid,
                        "note": ("large-load (datacenter et al) queue MW — "
                                 "partial daylight on the declared blind spot")}
    gaps.append("ERCOT large-load report not found on probed reportTypeIds "
                "(16004, 15964) — blind spot remains UNMEASURED, as declared")
    return {"status": "UNAVAILABLE"}


def iso_queues_all(gaps):
    """Run every non-CAISO adapter in parallel; return {iso: block}."""
    out = {}
    with ThreadPoolExecutor(max_workers=6) as ex:
        futs = {ex.submit(fn, gaps): nm for nm, fn in
                (("NYISO", fetch_nyiso), ("MISO", fetch_miso), ("SPP", fetch_spp),
                 ("ISO-NE", fetch_isone), ("ERCOT", fetch_ercot), ("PJM", fetch_pjm))}
        for f in futs:
            try:
                blk = f.result(timeout=150)
            except Exception as e:
                gaps.append("%s adapter crashed: %s" % (futs[f], str(e)[:60]))
                blk = None
            if blk:
                out[blk["iso"]] = blk
    return out


def lambda_handler(event, context):
    started = datetime.now(timezone.utc)
    gaps = []

    with ThreadPoolExecutor(max_workers=3) as ex:
        f_caiso = ex.submit(parse_caiso, gaps)
        f_plan = ex.submit(eia_planned, gaps)
        f_load = ex.submit(eia_industrial_load, gaps)
        caiso = f_caiso.result()
        planned = f_plan.result()
        load = f_load.result()

    # ops-4559 BUG-12: the other six ISOs (own pool; probe-first, gap on fail)
    iso = iso_queues_all(gaps)

    active = caiso.get("active") or []
    withdrawn = caiso.get("withdrawn") or []
    completed = caiso.get("completed") or []

    def agg(rows, key):
        d = {}
        for r in rows:
            k = (r.get(key) or "?").strip() or "?"
            d[k] = d.get(k, 0.0) + (r.get("mw") or 0)
        return dict(sorted(d.items(), key=lambda kv: -kv[1])[:20])

    queue = {
        "active_projects": len(active),
        "active_mw": round(sum(r["mw"] for r in active), 1),
        "withdrawn_projects": len(withdrawn),
        "withdrawn_mw": round(sum(r["mw"] for r in withdrawn), 1),
        "completed_projects": len(completed),
        "completed_mw": round(sum(r["mw"] for r in completed), 1),
        "by_fuel_mw": agg(active, "fuel"),
        "by_county_mw": agg(active, "county"),
        "large_projects": sorted(
            [r for r in active if r["mw"] >= LARGE_MW],
            key=lambda r: -r["mw"])[:50],
    }
    # churn: withdrawals relative to the active book is the honest read
    if queue["active_mw"] > 0:
        queue["withdrawal_ratio"] = round(
            queue["withdrawn_mw"] / (queue["active_mw"] + queue["withdrawn_mw"]) * 100, 1)
        queue["completion_ratio"] = round(
            queue["completed_mw"] / (queue["active_mw"] + queue["completed_mw"]) * 100, 1)

    # ── HOTSPOTS: three independent legs agreeing ────────────────────────
    load_by_state = {s["state"]: s for s in (load.get("states") or [])}
    upr = planned.get("uprate_by_state") or {}
    hotspots = []
    for st, s in load_by_state.items():
        yoy = s.get("yoy_pct")
        if yoy is None:
            continue
        up_mw = upr.get(st, 0.0)
        ind_n = sum(1 for p in (planned.get("industrial_plants") or [])
                    if p.get("state") == st)
        legs = 0
        if yoy > 2:
            legs += 1
        if up_mw > 0:
            legs += 1
        if ind_n >= 3:
            legs += 1
        if legs >= 2:
            hotspots.append({
                "state": st, "industrial_load_yoy_pct": yoy,
                "load_3m_pct": s.get("mom_3m_pct"),
                "planned_uprate_mw": round(up_mw, 1),
                "industrial_plants": ind_n, "legs": legs,
                "read": ("CONFIRMED_BUILDOUT" if legs == 3
                         else "EMERGING_BUILDOUT"),
            })
    hotspots.sort(key=lambda h: (-h["legs"], -(h["industrial_load_yoy_pct"] or 0)))
    hotspots = hotspots[:HOT_STATES]

    # ── ops-4559 BUG-12: national rollup with executed-IA as the PRIMARY ──
    caiso_ia_mw = round(sum(r["mw"] for r in active if _classify_ia(r)), 1)
    caiso_blk = {"iso": "CAISO", "n_projects": len(active),
                 "headline_mw": queue["active_mw"],
                 "mw_with_executed_ia": caiso_ia_mw,
                 "ia_detection": ("column" if any(r.get("ia") for r in active)
                                  else "status_regex"),
                 "src": CAISO_QUEUE}
    iso_all = {"CAISO": caiso_blk, **iso}
    live = sorted(iso_all.keys())
    missing = sorted(set(("CAISO", "PJM", "ERCOT", "MISO", "SPP", "ISO-NE", "NYISO")) - set(live))
    headline_nat = round(sum(b["headline_mw"] for b in iso_all.values()), 1)
    ia_known = [b for b in iso_all.values() if b.get("mw_with_executed_ia") is not None]
    ia_nat = round(sum(b["mw_with_executed_ia"] for b in ia_known), 1)
    national = {
        "primary_metric": "mw_with_executed_ia",
        "mw_with_executed_ia": ia_nat,
        "ia_measured_isos": [b["iso"] for b in ia_known],
        "headline_queue_mw": headline_nat,
        "headline_risk_adjusted_mw": round(headline_nat * LBNL_PRIORS["completion_rate_all_requests"], 1),
        "isos_live": live, "isos_missing": missing,
        "n_isos_live": len(live),
        "assumption": ("headline_risk_adjusted applies LBNL's ~13%% all-request "
                       "completion rate; mw_with_executed_ia is reported unhaircut "
                       "because the executed-IA cohort is the survivor pool"),
        "blind_spot": ("large-LOAD (datacenter) interconnection is handled outside "
                       "generation queues — absence of a queue signal for a "
                       "datacenter region means UNMEASURED, not absent"),
    }

    # wo4580: large-load probe (the datacenter blind spot, partially lit)
    large_load = fetch_ercot_large_load(gaps)

    # wo4580: snapshot archive → queue VELOCITY (MW entering executed-IA).
    # Stock was the v2.0 metric; the tradeable read is the flow.
    today_s = datetime.now(timezone.utc).date().isoformat()
    snap = {"date": today_s,
            "per_iso": {k: {"headline_mw": v.get("headline_mw"),
                            "ia_mw": v.get("mw_with_executed_ia")}
                        for k, v in iso_all.items()},
            "national_ia_mw": ia_nat, "national_headline_mw": headline_nat}
    # rev 4582: the block below said s3 (lowercase) — this module's client is
    # S3. The NameError was swallowed by the except and the DEFAULT dict
    # (n_snapshots: 1) masqueraded as a written archive. Default is now 0 so
    # a failed put can never look like history again.
    velocity = {"status": "INSUFFICIENT_HISTORY", "n_snapshots": 0}
    try:
        S3.put_object(Bucket=BUCKET,
                      Key="data/archive/grid-queue/%s.json" % today_s,
                      Body=json.dumps(snap).encode(),
                      ContentType="application/json")
        resp = S3.list_objects_v2(Bucket=BUCKET,
                                  Prefix="data/archive/grid-queue/", MaxKeys=400)
        keys = sorted(k2["Key"] for k2 in resp.get("Contents", []))
        velocity["n_snapshots"] = len(keys)
        if len(keys) >= 2:
            first = json.loads(S3.get_object(Bucket=BUCKET,
                                             Key=keys[0])["Body"].read())
            span_d = max((datetime.fromisoformat(today_s)
                          - datetime.fromisoformat(first["date"])).days, 1)
            d_ia = ia_nat - (first.get("national_ia_mw") or 0)
            d_hl = headline_nat - (first.get("national_headline_mw") or 0)
            per_iso_v = {}
            for k2, v2 in snap["per_iso"].items():
                f2 = (first.get("per_iso") or {}).get(k2) or {}
                if v2.get("ia_mw") is not None and f2.get("ia_mw") is not None:
                    per_iso_v[k2] = round((v2["ia_mw"] - f2["ia_mw"])
                                          / span_d * 30.44, 1)
            velocity = {"status": "LIVE", "n_snapshots": len(keys),
                        "span_days": span_d,
                        "national_ia_mw_per_month": round(d_ia / span_d * 30.44, 1),
                        "national_headline_mw_per_month": round(d_hl / span_d * 30.44, 1),
                        "per_iso_ia_mw_per_month": per_iso_v,
                        "method": ("Δ vs oldest archived snapshot, scaled to "
                                   "MW/month; the archive accrues daily from "
                                   "today — nothing backfilled")}
    except Exception as _e:
        gaps.append("velocity archive failed: %s" % str(_e)[:80])

    # wo4580 impact_map: structural per-MW buildout exposure (direction
    # honest, no invented pp) + beta-estimated sectors once history earns n.
    _shock = velocity.get("national_ia_mw_per_month") \
        if velocity.get("status") == "LIVE" else 0
    est_rows, est_missing = beta_impact(
        ["Utilities", "Industrials", "Energy"], "grid_executed_mw",
        _shock or 0, kind="industry")
    struct = [
        structural_row("Electrical Equipment (transformers/switchgear)",
                       "industry", "per-MW interconnection buildout demand "
                       "(ETN/PWR/HUBB class)", +1),
        structural_row("Heavy Electrical Equipment (gas turbines)", "industry",
                       "gas-fuel queue share drives turbine orders (GEV "
                       "class)", +1),
        structural_row("Engineering & Construction (EPC)", "industry",
                       "executed-IA MW converts to EPC backlog", +1),
        structural_row("Regulated Electric Utilities", "industry",
                       "queue growth feeds rate-base CAGR (quasi-measured "
                       "regulated math — tighter read once betas accrue)", +1),
    ]
    impact = impact_build(
        "grid-queue", "grid_executed_mw",
        struct + [r for r in est_rows if r["pp"] > 0],
        [r for r in est_rows if r["pp"] <= 0],
        "Structural rows: per-MW buildout beneficiaries (direction only — no "
        "pp asserted without measurement). Estimated rows appear once the "
        "nightly factor history accrues n_obs>=8 for grid_executed_mw.",
        insufficient_rows=est_missing[:6],
        basis_note="velocity %s (%d snapshots)"
                   % (velocity.get("status"), velocity.get("n_snapshots", 0)))

    out = {
        "version": VERSION,
        "generated_at": started.isoformat(),
        "national": national,
        "large_load_queue": large_load,
        "queue_velocity": velocity,
        "impact_map": impact,
        "lbnl_priors": LBNL_PRIORS,
        "iso_queues": iso_all,
        "queue": queue,
        "planned_capacity": planned,
        "industrial_load": load,
        "hotspots": hotspots,
        "gaps": gaps + [
            "EPA ECHO serves compliance data, not construction permits — "
            "excluded rather than used as a false permit proxy",
        ],
        "coverage": {
            "iso_queues_live": live,
            "iso_queues_missing": missing,
            "caiso_active_rows": len(active),
            "eia_industrial_plants": planned.get("n_industrial", 0),
            "eia_load_states": len(load.get("states") or []),
        },
        "method": ("v2 (ops-4559): interconnection queues from all seven US ISOs "
                   "where a public endpoint answers (per-ISO adapters, probe-first, "
                   "gaps[] on failure). PRIMARY figure is mw_with_executed_ia — "
                   "LBNL shows only ~13% of headline queue requests ever reach COD, "
                   "so headline MW is demoted to context with the 13% haircut "
                   "stated. Forward capacity from EIA-860M; industrial load from "
                   "EIA-861M (sectorid=IND). Hotspots need 2+ independent legs."),
        "attribution": "CAISO public queue report; U.S. EIA API v2 (Forms 860M/861M)",
    }

    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, separators=(",", ":")),
                  ContentType="application/json")

    # self-building history for queue MW trend
    try:
        hist = json.loads(S3.get_object(Bucket=BUCKET, Key=HIST_KEY)["Body"].read())
    except Exception:
        hist = {"snapshots": []}
    hist["snapshots"].append({
        "at": started.strftime("%Y-%m-%d"),
        "active_mw": queue["active_mw"],
        "active_projects": queue["active_projects"],
        "withdrawn_mw": queue["withdrawn_mw"],
    })
    hist["snapshots"] = hist["snapshots"][-400:]
    S3.put_object(Bucket=BUCKET, Key=HIST_KEY,
                  Body=json.dumps(hist, separators=(",", ":")),
                  ContentType="application/json")

    print("[grid-queue] active=%d/%.0fMW industrial_plants=%d hotspots=%d gaps=%d"
          % (queue["active_projects"], queue["active_mw"],
             planned.get("n_industrial", 0), len(hotspots), len(gaps)))
    return {"statusCode": 200, "body": json.dumps({
        "active_projects": queue["active_projects"],
        "active_mw": queue["active_mw"],
        "hotspots": len(hotspots)})}
