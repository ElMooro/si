"""risk_sources — shared plumbing for the free systemic / sovereign risk source engines.

One engine per public source (named exactly after the source, page named exactly
after it). Every engine does the same four things, so they live here once:

  1. fetch the upstream file(s) with a conditional-friendly GET (retries, UA);
  2. parse it (CSV, JSON, or .xlsx read with the standard library only — no
     openpyxl in the Lambda runtime);
  3. normalise everything into ONE packet shape the shared page renderer
     (/jh-risk-source.js) understands:
        {slug, name, version, generated_at, as_of, doctrine, source:{...},
         kpis:[{label,value,sub,tone}], series:{id:{label,unit,freq,latest,
         prev,chg,chg_pct,pct_rank,min,max,n,first,last,tail:[[d,v]...],
         warm_key}}, tables:{name:{columns,rows,note}}, notes:[...],
         source_files:[{name,url,bytes,status}]}
  4. publish: hot packet data/<slug>.json, raw mirror data/warm/<slug>/src/,
     one gzip JSON per series data/warm/<slug>/series/<id>.json.gz and a
     data/warm/<slug>/state.json catalog so justhodl-provider-catalog counts
     the source on data.html exactly like OFR / NY Fed / BIS.

Doctrine: descriptive only. No call, no target, no sizing. Each packet carries
decision.call None / sizing_eligible False so the public contract gate passes.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import math
import os
import re
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone

VERSION = "1.1.0"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
UA = {"User-Agent": "JustHodl Research (justhodl.ai; raafouis@gmail.com) risk-sources/" + VERSION,
      "Accept": "*/*"}
TAIL_POINTS = 420          # points kept inline in the hot packet per series
MAX_HOT_SERIES = 400       # beyond this the hot packet keeps a short tail per series (warm has everything)
TAIL_SMALL = 160
FORBIDDEN_WORDS = ("buy", "sell", "target", "forecast", "predict")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ──────────────────────────────────────────────────────────────── HTTP ──
def http_get(url, timeout=60, headers=None, retries=2, sleep=1.5):
    """GET bytes with a polite UA and simple retry. Raises on final failure."""
    h = dict(UA)
    if headers:
        h.update(headers)
    last = None
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers=h)
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:  # noqa: PERF203
            last = e
            if isinstance(e, urllib.error.HTTPError) and e.code in (400, 401, 403, 404):
                break
            time.sleep(sleep * (attempt + 1))
    raise RuntimeError(f"GET {url} failed: {type(last).__name__}: {str(last)[:120]}")


def http_json(url, **kw):
    return json.loads(http_get(url, **kw).decode("utf-8", "replace"))


def http_text(url, **kw):
    return http_get(url, **kw).decode("utf-8", "replace")


# ───────────────────────────────────────────────────────────── parsing ──
def parse_num(s):
    """'1,234.5' / '12.3%' / '' / None -> float or None."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return None if (isinstance(s, float) and math.isnan(s)) else float(s)
    t = str(s).strip().replace(",", "").replace("%", "").replace("\u2212", "-")
    if t in ("", "-", "--", "n/a", "NA", "na", ".", "…", "x", "X", ":"):
        return None
    try:
        return float(t)
    except ValueError:
        return None


def csv_rows(text, delimiter=","):
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return list(csv.reader(io.StringIO(text), delimiter=delimiter))


def excel_date(serial):
    """Excel 1900-system serial -> ISO date string."""
    try:
        n = float(serial)
    except (TypeError, ValueError):
        return None
    if n < 60 or n > 80000:
        return None
    return (date(1899, 12, 30) + timedelta(days=int(n))).isoformat()


def month_end(y, m):
    nxt = date(y + (m // 12), (m % 12) + 1, 1)
    return (nxt - timedelta(days=1)).isoformat()


def quarter_end(y, q):
    return month_end(y, q * 3)


def period_to_date(p):
    """'2025', '2025-Q3', '2025Q3', '2025-09', '202509', '2025-S2', '2025M09' -> ISO date (period end)."""
    s = str(p).strip()
    m = re.fullmatch(r"(\d{4})$", s)
    if m:
        return f"{m.group(1)}-12-31"
    m = re.fullmatch(r"(\d{4})-?Q([1-4])", s)
    if m:
        return quarter_end(int(m.group(1)), int(m.group(2)))
    m = re.fullmatch(r"(\d{4})-?S([12])", s)
    if m:
        return month_end(int(m.group(1)), 6 if m.group(2) == "1" else 12)
    m = re.fullmatch(r"(\d{4})-?M?(\d{2})$", s)
    if m and 1 <= int(m.group(2)) <= 12:
        return month_end(int(m.group(1)), int(m.group(2)))
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return s
    m = re.fullmatch(r"(\d{1,2})/(\d{4})", s)          # FAO '1/1990'
    if m:
        return month_end(int(m.group(2)), int(m.group(1)))
    m = re.fullmatch(r"(\d{1,2})/(\d{1,2})/(\d{4})", s)  # US m/d/yyyy
    if m:
        return f"{m.group(3)}-{int(m.group(1)):02d}-{int(m.group(2)):02d}"
    return None


class Xlsx:
    """Minimal .xlsx reader (shared strings, inline strings, numbers). Standard library only."""

    def __init__(self, blob):
        self.z = zipfile.ZipFile(io.BytesIO(blob))
        wb = self.z.read("xl/workbook.xml").decode("utf-8", "replace")
        self.sheets = re.findall(r'<sheet [^>]*?name="([^"]+)"[^>]*?r:id="([^"]+)"', wb)
        if not self.sheets:  # attribute order can differ
            self.sheets = [(n, i) for i, n in re.findall(r'<sheet [^>]*?r:id="([^"]+)"[^>]*?name="([^"]+)"', wb)]
        rels = self.z.read("xl/_rels/workbook.xml.rels").decode("utf-8", "replace")
        self.rels = {}
        for m in re.finditer(r"<Relationship ([^>]+?)/?>", rels):
            a = dict(re.findall(r'(\w+)="([^"]*)"', m.group(1)))
            if "Id" in a and "Target" in a:
                self.rels[a["Id"]] = a["Target"]
        self.strings = []
        if "xl/sharedStrings.xml" in self.z.namelist():
            ss = self.z.read("xl/sharedStrings.xml").decode("utf-8", "replace")
            self.strings = [_unescape(re.sub(r"<[^>]+>", "", s))
                            for s in re.findall(r"<si>(.*?)</si>", ss, re.S)]

    def sheet_names(self):
        return [n for n, _ in self.sheets]

    def rows(self, sheet, max_rows=None):
        if isinstance(sheet, int):
            name, rid = self.sheets[sheet]
        else:
            d = dict(self.sheets)
            if sheet not in d:
                raise KeyError(f"sheet {sheet!r} not in {list(d)}")
            rid = d[sheet]
        target = self.rels[rid]
        if target.startswith("/"):          # absolute package path ("/xl/worksheets/sheet1.xml")
            target = target.lstrip("/")
        elif not target.startswith("xl/"):
            target = "xl/" + target
        xml = self.z.read(target).decode("utf-8", "replace")
        out = []
        for row in re.findall(r"<row [^>]*>(.*?)</row>", xml, re.S):
            cells = {}
            for ref, attrs, inner in re.findall(r'<c r="([A-Z]+)\d+"([^>]*?)(?:/>|>(.*?)</c>)', row, re.S):
                v = None
                if 't="s"' in attrs:
                    m = re.search(r"<v>(.*?)</v>", inner or "")
                    if m:
                        try:
                            v = self.strings[int(m.group(1))]
                        except (ValueError, IndexError):
                            v = None
                elif 't="inlineStr"' in attrs:
                    v = _unescape(re.sub(r"<[^>]+>", "", inner or ""))
                else:
                    m = re.search(r"<v>(.*?)</v>", inner or "")
                    if m:
                        raw = m.group(1)
                        try:
                            v = float(raw)
                        except ValueError:
                            v = raw
                cells[_col_index(ref)] = v
            if cells:
                width = max(cells) + 1
                out.append([cells.get(i) for i in range(width)])
            else:
                out.append([])
            if max_rows and len(out) >= max_rows:
                break
        return out


def _col_index(letters):
    n = 0
    for ch in letters:
        n = n * 26 + (ord(ch) - 64)
    return n - 1


def _unescape(s):
    return (s.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
             .replace("&quot;", '"').replace("&apos;", "'").replace("&#39;", "'"))


# ──────────────────────────────────────────────────────────── statistics ──
def ordinal(n):
    """18.1 -> '18th', 2 -> '2nd', 11 -> '11th' (for percentile captions)."""
    if n is None:
        return "n/a"
    i = int(round(float(n)))
    suf = "th" if 10 <= i % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(i % 10, "th")
    return f"{i}{suf}"


def pct_rank(values, x):
    vals = [v for v in values if v is not None]
    if x is None or len(vals) < 3:
        return None
    below = sum(1 for v in vals if v < x)
    equal = sum(1 for v in vals if v == x)
    return round(100.0 * (below + 0.5 * equal) / len(vals), 1)


def series_stats(points):
    """points: sorted [[iso_date, value], ...] -> stats dict."""
    pts = [[d, v] for d, v in points if d and v is not None and not (isinstance(v, float) and math.isnan(v))]
    pts.sort(key=lambda p: p[0])
    if not pts:
        return None, []
    vals = [v for _, v in pts]
    latest = pts[-1]
    prev = pts[-2] if len(pts) > 1 else [None, None]
    chg = (latest[1] - prev[1]) if prev[1] is not None else None
    chg_pct = (100.0 * chg / abs(prev[1])) if (chg is not None and prev[1]) else None
    st = {
        "latest": latest[1], "date": latest[0], "prev": prev[1], "prev_date": prev[0],
        "chg": None if chg is None else round(chg, 6),
        "chg_pct": None if chg_pct is None else round(chg_pct, 2),
        "pct_rank": pct_rank(vals, latest[1]),
        "min": min(vals), "max": max(vals),
        "min_date": pts[vals.index(min(vals))][0], "max_date": pts[vals.index(max(vals))][0],
        "n": len(pts), "first": pts[0][0], "last": latest[0],
    }
    return st, pts


# ─────────────────────────────────────────────────────────────── packet ──
class Packet:
    def __init__(self, slug, name, title, description, source, cadence="daily", page=None):
        self.slug = slug
        self.name = name
        self.title = title
        self.description = description
        self.source = source            # {"provider","url","docs","license","notes"}
        self.cadence = cadence
        self.page = page or f"/{slug}.html"
        self.series = {}
        self.series_order = []
        self.tables = {}
        self.kpis = []
        self.notes = []
        self.source_files = []
        self.extra = {}
        self.hot_tail = None            # per-packet override of how many trailing points the hot packet carries
        self._raw = []                  # (name, bytes, content_type)

    def add_series(self, sid, label, points, unit="", freq="", group="", **meta):
        st, pts = series_stats(points)
        if not st:
            return None
        sid = re.sub(r"[^A-Za-z0-9_.:-]+", "_", sid)
        rec = {"id": sid, "label": label, "unit": unit, "freq": freq, "group": group}
        rec.update(meta)
        rec.update(st)
        rec["_points"] = pts
        self.series[sid] = rec
        if sid not in self.series_order:
            self.series_order.append(sid)
        return rec

    def add_table(self, name, columns, rows, note="", title=None):
        self.tables[name] = {"title": title or name, "columns": columns, "rows": rows, "note": note}

    def kpi(self, label, value, sub="", tone="info"):
        self.kpis.append({"label": label, "value": value, "sub": sub, "tone": tone})

    def note(self, text):
        self.notes.append(text)

    def raw(self, name, blob, content_type="application/octet-stream", url=None, status="live"):
        self._raw.append((name, blob, content_type))
        self.source_files.append({"name": name, "url": url, "bytes": len(blob), "status": status})

    def file_status(self, name, url, status, bytes_=0):
        self.source_files.append({"name": name, "url": url, "bytes": bytes_, "status": status})

    def as_of(self):
        dates = [s["last"] for s in self.series.values() if s.get("last")]
        return max(dates) if dates else None

    def build(self):
        gen = now_iso()
        hot_series = {}
        for sid in self.series_order:
            s = dict(self.series[sid])
            pts = s.pop("_points")
            s["warm_key"] = f"data/warm/{self.slug}/series/{sid}.json.gz"
            keep = self.hot_tail or (TAIL_POINTS if len(self.series_order) <= MAX_HOT_SERIES else TAIL_SMALL)
            s["tail"] = pts[-keep:]
            s["tail_is_full"] = len(pts) <= keep
            hot_series[sid] = s
        packet = {
            "slug": self.slug, "name": self.name, "title": self.title, "description": self.description,
            "version": VERSION, "generated_at": gen, "as_of": self.as_of(), "cadence": self.cadence,
            "page": self.page,
            "doctrine": "descriptive",
            "decision": {"call": None, "sizing_eligible": False,
                         "note": "Descriptive measurements from a public source. No call, no sizing."},
            "source": self.source,
            "kpis": self.kpis,
            "series_order": self.series_order,
            "series": hot_series,
            "n_series": len(self.series_order),
            "n_points": sum(s["n"] for s in self.series.values()),
            "tables": self.tables,
            "notes": self.notes,
            "source_files": self.source_files,
            "warm_prefix": f"data/warm/{self.slug}/",
        }
        packet.update(self.extra)
        return packet


def check_doctrine(packet):
    """Public text must not contain call words (page + packet gate)."""
    blob = json.dumps({k: v for k, v in packet.items() if k in ("title", "description", "notes", "kpis", "tables")}).lower()
    hits = [w for w in FORBIDDEN_WORDS if re.search(r"\b" + w + r"\b", blob)]
    return hits


# ──────────────────────────────────────────────────────────────── publish ──
def publish(packet_obj, s3=None, bucket=BUCKET, mirror_raw=True):
    """Write hot packet + warm mirror + per-series gz + state catalog. Returns summary."""
    import boto3  # local import keeps tests dependency-free
    s3 = s3 or boto3.client("s3", region_name="us-east-1")
    packet = packet_obj.build()
    hits = check_doctrine(packet)
    if hits:
        raise ValueError(f"doctrine words in public text: {hits}")
    slug = packet_obj.slug
    put = lambda key, body, ct, **kw: s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=ct, **kw)  # noqa: E731
    n_series = 0
    for sid in packet_obj.series_order:
        s = packet_obj.series[sid]
        body = json.dumps({"id": sid, "label": s["label"], "unit": s["unit"], "freq": s["freq"],
                           "slug": slug, "generated_at": packet["generated_at"],
                           "points": s["_points"]}, separators=(",", ":")).encode()
        put(f"data/warm/{slug}/series/{sid}.json.gz", gzip.compress(body), "application/json",
            ContentEncoding="gzip", CacheControl="max-age=300")
        n_series += 1
    if mirror_raw:
        for name, blob, ct in packet_obj._raw:
            put(f"data/warm/{slug}/src/{name}", blob, ct, CacheControl="max-age=300")
    state = {"slug": slug, "as_of": packet["as_of"], "generated_at": packet["generated_at"],
             "catalog": list(packet_obj.series_order), "n_series": n_series,
             "status": "COMPLETE-maintaining", "version": VERSION,
             "source_files": packet["source_files"], "hot": f"data/{slug}.json", "page": packet["page"]}
    put(f"data/warm/{slug}/state.json", json.dumps(state).encode(), "application/json", CacheControl="no-cache")
    put(f"data/warm/{slug}/_last-check.json", json.dumps({"checked_at": packet["generated_at"]}).encode(),
        "application/json", CacheControl="no-cache")
    hot = json.dumps(packet, separators=(",", ":"), default=str).encode()
    put(f"data/{slug}.json", hot, "application/json", CacheControl="no-cache")
    return {"ok": True, "slug": slug, "hot_bytes": len(hot), "n_series": n_series,
            "n_points": packet["n_points"], "as_of": packet["as_of"],
            "raw_files": len(packet_obj._raw), "generated_at": packet["generated_at"]}


def run(slug, build_fn, event=None):
    """Standard handler body: build -> publish -> summary. `event.dry_run` skips S3."""
    event = event or {}
    t0 = time.time()
    pk = build_fn(event)
    if event.get("dry_run"):
        packet = pk.build()
        res = {"ok": True, "dry_run": True, "slug": slug, "n_series": packet["n_series"],
               "n_points": packet["n_points"], "as_of": packet["as_of"],
               "doctrine_hits": check_doctrine(packet)}
    else:
        res = publish(pk)
    res["elapsed_s"] = round(time.time() - t0, 1)
    print(json.dumps(res, default=str))
    return {"statusCode": 200, "body": json.dumps(res, default=str)}


def read_s3_json(key, bucket=BUCKET, default=None):
    import boto3
    s3 = boto3.client("s3", region_name="us-east-1")
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if key.endswith(".gz"):
            body = gzip.decompress(body)
        return json.loads(body)
    except Exception:  # noqa: BLE001
        return default
