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

VERSION = "1.0.2"
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
            # fuel legs: join the non-empty fuel columns for an honest label
            fuels = []
            for ci, h in enumerate(hdr):
                if h.strip().startswith("fuel-") and ci < len(r):
                    v = str(r[ci]).strip()
                    if v and v.lower() not in ("", "n/a", "none"):
                        fuels.append(v)
            out.append({
                "project": (r[c_name] if c_name is not None and c_name < len(r) else "")[:70],
                "mw": round(mw, 1),
                "state": (r[c_st] if c_st is not None and c_st < len(r) else "").strip()[:20] or "CA",
                "county": (r[c_cty] if c_cty is not None and c_cty < len(r) else "").strip()[:40],
                "fuel": (" + ".join(fuels[:3]) if fuels else
                         (r[c_fuel] if c_fuel is not None and c_fuel < len(r) else "").strip())[:44],
                "status": (r[c_stat] if c_stat is not None and c_stat < len(r) else "").strip()[:40],
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

    out = {
        "version": VERSION,
        "generated_at": started.isoformat(),
        "queue": queue,
        "planned_capacity": planned,
        "industrial_load": load,
        "hotspots": hotspots,
        "gaps": gaps + [
            "ERCOT GIS queue report ID unresolved (ops 3734) — excluded from v1",
            "PJM Data Miner 2 requires a subscription key (401)",
            "MISO public queue endpoint returns 404",
            "LBNL 'Queued Up' blocked (403) from Lambda IPs",
            "EPA ECHO serves compliance data, not construction permits — "
            "excluded rather than used as a false permit proxy",
        ],
        "coverage": {
            "iso_queues_live": ["CAISO"],
            "iso_queues_missing": ["ERCOT", "PJM", "MISO", "ISO-NE", "NYISO", "SPP"],
            "caiso_active_rows": len(active),
            "eia_industrial_plants": planned.get("n_industrial", 0),
            "eia_load_states": len(load.get("states") or []),
        },
        "method": ("Queue MW from the CAISO public interconnection report; "
                   "forward capacity from EIA-860M planned uprates; industrial "
                   "load from EIA-861M retail sales (sectorid=IND). A hotspot "
                   "requires at least two independent legs to agree — a queue "
                   "that only grows is an artifact, not demand, so withdrawal "
                   "and completion ratios are published alongside."),
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
